import argparse
import contextlib
import pathlib
import os
import socket
import subprocess
import tempfile
from multiprocessing import Process, Queue, cpu_count
import time
from .utils import seed_everything, readable_time, readable_num, count_parameters
from .utils import get_all_possible_moves, create_elo_dict
from .utils import decompress_zst, read_or_create_chunks, setup_project_directories
from .logger import get_logger
from .main import MAIA2Model, preprocess_thread, train_chunks, read_monthly_data_filenames, process_chunks

import mlflow
import mlflow.pytorch
import torch
import torch.nn as nn
import pdb


log = get_logger("training")

client = mlflow.MlflowClient()


def _git_sha() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"], stderr=subprocess.DEVNULL
        ).decode().strip()
    except Exception:
        return "unknown"


def _mlflow_param_dict(cfg) -> dict:
    out = {}
    for key, value in vars(cfg).items():
        if isinstance(value, (str, int, float, bool)) or value is None:
            out[key] = value
        else:
            out[key] = str(value)
    return out


def run(cfg):
    paths = setup_project_directories()
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    print('Configurations:', flush=True)
    for arg in vars(cfg):
        print(f'\t{arg}: {getattr(cfg, arg)}', flush=True)
    seed_everything(cfg.seed)
    num_processes = cfg.num_cpu_left

    checkpoint_info = f'{cfg.lr}_{cfg.batch_size}_{cfg.wd}_{cfg.start_year}-{cfg.start_month:02d}_{cfg.end_year}-{cfg.end_month:02d}'

    tracking_enabled = bool(getattr(cfg, "tracking_enabled", False))
    if tracking_enabled:
        mlflow.set_tracking_uri(getattr(cfg, "tracking_uri", "./mlruns"))
        mlflow.set_experiment(getattr(cfg, "experiment_name", "maia2"))
        run_ctx = mlflow.start_run(run_name=checkpoint_info)
    else:
        run_ctx = contextlib.nullcontext()

    with run_ctx:
        if tracking_enabled:
            mlflow.log_params(_mlflow_param_dict(cfg))
            mlflow.set_tag("git.commit", _git_sha())
            mlflow.set_tag("host", socket.gethostname())
            if getattr(cfg, "from_checkpoint", False):
                mlflow.set_tag("resumed_from_epoch", str(cfg.checkpoint_epoch))
                mlflow.set_tag("resumed_from", f"{cfg.checkpoint_year}-{cfg.checkpoint_month:02d}")
                if getattr(cfg, "checkpoint_run_id", ""):
                    mlflow.set_tag("parent_run_id", cfg.checkpoint_run_id)
            
            config_artifact = paths["ml_models"] / "config.yaml"
            if config_artifact.exists():
                mlflow.log_artifact(str(config_artifact))

        all_moves = get_all_possible_moves()
        all_moves_dict = {move: i for i, move in enumerate(all_moves)}
        elo_dict = create_elo_dict()

        model = MAIA2Model(len(all_moves), elo_dict, cfg)

        print(model, flush=True)
        model = model.to(device)
        model = nn.DataParallel(model)
        criterion_maia = nn.CrossEntropyLoss()
        criterion_side_info = nn.BCEWithLogitsLoss()
        criterion_value = nn.MSELoss()

        optimizer = torch.optim.AdamW(model.parameters(), lr=cfg.lr, weight_decay=cfg.wd)
        N_params = count_parameters(model)
        log.info(f'Trainable Parameters: {N_params}')
        if tracking_enabled:
            mlflow.log_param("n_params", N_params)

        accumulated_samples = 0
        accumulated_games = 0

        if cfg.from_checkpoint:
            run_id = getattr(cfg, "checkpoint_run_id", "")
            if not run_id:
                raise ValueError(
                    "from_checkpoint=true but checkpoint_run_id is empty. "
                    "Set checkpoint_run_id in config.yaml to the MLflow run that produced the checkpoint."
                )
            log.info(f"Loading checkpoint from MLflow run {run_id}, epoch {cfg.checkpoint_epoch} of {cfg.checkpoint_year}-{cfg.checkpoint_month:02d}")
            
            old_run_data = client.get_run(run_id).data
            ignored_params = {"checkpoint_epoch", "checkpoint_year", "checkpoint_month", "checkpoint_run_id", "n_params"}
            for param_key, param_val in old_run_data.params.items():
                if param_key in ignored_params:
                    continue

                current_val = getattr(cfg, param_key, "<missing>")
                if str(current_val) != param_val:
                    log.warning(
                        f"Checkpoint was trained with {param_key}={param_val} but current config has {param_key}={current_val}. "
                        f"Using value from checkpoint config for consistency: {param_key}={param_val}"
                    )
            
            for metric_key in old_run_data.metrics.keys():
                metric_history = client.get_metric_history(run_id, metric_key)
                for m in metric_history:
                    mlflow.log_metric(metric_key, m.value, step=m.step, timestamp=m.timestamp)

            del old_run_data

            mlflow.set_tracking_uri(getattr(cfg, "tracking_uri", "./mlruns"))
            artifact_path = (
                f"checkpoints/epoch_{cfg.checkpoint_epoch}/"
                f"epoch_{cfg.checkpoint_epoch}_blitz_games_{cfg.checkpoint_year}_{cfg.checkpoint_month:02d}.pgn.pt"
            )
            local_pt = mlflow.artifacts.download_artifacts(run_id=run_id, artifact_path=artifact_path)
            checkpoint = torch.load(local_pt)
            model.load_state_dict(checkpoint['model_state_dict'])
            optimizer.load_state_dict(checkpoint['optimizer_state_dict'])
            accumulated_samples = checkpoint['accumulated_samples']
            accumulated_games = checkpoint['accumulated_games']

        start_epoch = cfg.checkpoint_epoch - 1 if cfg.from_checkpoint else 1
        for epoch in range(start_epoch, cfg.max_epochs):

            log.info(f'Epoch {epoch + 1}')
            pgn_filenames = read_monthly_data_filenames(cfg)

            num_file = 0
            for filename in pgn_filenames:
                if cfg.from_checkpoint and (filename < f"{cfg.checkpoint_year}_{cfg.checkpoint_month:02d}.pgn" and (start_epoch == epoch)):
                    log.info(f'Skipping {filename} because it is less than or equal to checkpoint date {cfg.checkpoint_year}_{cfg.checkpoint_month:02d}')
                    num_file += 1
                    continue

                log.info(f'Processing {filename}')

                start_time = time.time()

                # Define the raw and processed data file path
                raw_data_path = str(paths["raw_data"] / (filename + ".zst"))
                processed_data_path = str(paths["processed_data"] / filename)

                try:
                    decompress_zst(raw_data_path, processed_data_path)
                except FileNotFoundError as e:
                    log.error(f"File {raw_data_path} not found. Skipping.")
                    continue
                
                log.info(f'Decompressing {raw_data_path} took {readable_time(time.time() - start_time)}')

                pgn_chunks = read_or_create_chunks(processed_data_path, cfg)

                queue = Queue(maxsize=cfg.queue_length)

                pgn_chunks_sublists = []
                for i in range(0, len(pgn_chunks), num_processes):
                    pgn_chunks_sublists.append(pgn_chunks[i:i + num_processes])


                pgn_chunks_sublist = pgn_chunks_sublists[0]
                # For debugging only
                # process_chunks(cfg, processed_data_path, pgn_chunks_sublist, elo_dict)
                worker = Process(target=preprocess_thread, args=(queue, cfg, processed_data_path, pgn_chunks_sublist, elo_dict))
                worker.start()

                num_chunk = 0
                offset = 0
                while True:
                    if not queue.empty():
                        if offset + 1 < len(pgn_chunks_sublists):
                            pgn_chunks_sublist = pgn_chunks_sublists[offset + 1]
                            worker = Process(target=preprocess_thread, args=(queue, cfg, processed_data_path, pgn_chunks_sublist, elo_dict))
                            worker.start()
                            offset += 1
                        data, game_count, chunk_count = queue.get()
                        loss, loss_maia, loss_side_info, loss_value = train_chunks(cfg, data, model, optimizer, all_moves_dict, criterion_maia, criterion_side_info, criterion_value)
                        num_chunk += chunk_count
                        accumulated_samples += len(data)
                        accumulated_games += game_count
                        log.info(
                            f'\n[{num_chunk}/{len(pgn_chunks)}]\n'
                            f'[# Positions]: {readable_num(accumulated_samples)}\n'
                            f'[# Games]: {readable_num(accumulated_games)}\n'
                            f'[# Loss]: {loss}\n'
                            f'[# Loss MAIA]: {loss_maia}\n'
                            f'[# Loss Side Info]: {loss_side_info}\n'
                            f'[# Loss Value]: {loss_value}\n'
                        )
                        if tracking_enabled:
                            mlflow.log_metrics(
                                {
                                    "loss": float(loss),
                                    "loss_maia": float(loss_maia),
                                    "loss_side_info": float(loss_side_info),
                                    "loss_value": float(loss_value),
                                },
                                step=accumulated_samples,
                            )
                        if num_chunk == len(pgn_chunks):
                            break

                num_file += 1
                log.info(f'### ({num_file} / {len(pgn_filenames)}) Took {readable_time(time.time() - start_time)} to train {processed_data_path} with {len(pgn_chunks)} chunks.')
                os.remove(processed_data_path)

                if tracking_enabled:
                    with tempfile.TemporaryDirectory() as tmpdir:
                        tmp_path = pathlib.Path(tmpdir) / f'epoch_{epoch + 1}_{pathlib.Path(processed_data_path).name}.pt'
                        torch.save({'model_state_dict': model.state_dict(),
                                    'optimizer_state_dict': optimizer.state_dict(),
                                    'accumulated_samples': accumulated_samples,
                                    'accumulated_games': accumulated_games}, tmp_path)
                        mlflow.log_artifact(str(tmp_path), artifact_path=f"checkpoints/epoch_{epoch + 1}")
                else:
                    log.warning("tracking_enabled=false; per-epoch checkpoint not persisted")

        final_model_path = paths["ml_models"] / f"{getattr(cfg, 'registered_model_name', 'maia2_final')}.pt"
        torch.save({
            'model_state_dict': model.module.state_dict(),
            'cfg': _mlflow_param_dict(cfg),
            'accumulated_samples': accumulated_samples,
            'accumulated_games': accumulated_games,
        }, final_model_path)
        log.info(f"Final model saved to {final_model_path}")

        if tracking_enabled:
            mlflow.pytorch.log_model(
                pytorch_model=model.module,
                name="model",
                registered_model_name=getattr(cfg, "registered_model_name", None),
            )
