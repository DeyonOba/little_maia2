import argparse
import pathlib
import os
from multiprocessing import Process, Queue, cpu_count
import time
from .utils import seed_everything, readable_time, readable_num, count_parameters
from .utils import get_all_possible_moves, create_elo_dict
from .utils import decompress_zst, read_or_create_chunks, setup_project_directories
from .main import MAIA2Model, preprocess_thread, train_chunks, read_monthly_data_filenames, process_chunks

import torch
import torch.nn as nn
import pdb


def run(cfg):
    paths = setup_project_directories()
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    
    print('Configurations:', flush=True)
    for arg in vars(cfg):
        print(f'\t{arg}: {getattr(cfg, arg)}', flush=True)
    seed_everything(cfg.seed)
    # num_processes = cpu_count() - cfg.num_cpu_left
    num_processes = cpu_count() // 2

    save_root = f'./saves/{cfg.lr}_{cfg.batch_size}_{cfg.wd}/'
    if not os.path.exists(save_root):
        os.makedirs(save_root)

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
    print(f'Trainable Parameters: {N_params}', flush=True)

    accumulated_samples = 0
    accumulated_games = 0

    if cfg.from_checkpoint:
        formatted_month = f"{cfg.checkpoint_month:02d}"
        checkpoint = torch.load(save_root + f'epoch_{cfg.checkpoint_epoch}_{cfg.checkpoint_year}-{formatted_month}.pgn.pt')
        model.load_state_dict(checkpoint['model_state_dict'])
        optimizer.load_state_dict(checkpoint['optimizer_state_dict'])
        accumulated_samples = checkpoint['accumulated_samples']
        accumulated_games = checkpoint['accumulated_games']

    for epoch in range(cfg.max_epochs):
        
        print(f'Epoch {epoch + 1}', flush=True)
        pgn_filenames = read_monthly_data_filenames(cfg)
        print(f"Training files:")
        
        for idx, filename in enumerate(pgn_filenames):
            print(f"{idx}. {filename}")
        
        num_file = 0
        for filename in pgn_filenames:
            print(f'Processing {filename}', flush=True)
            
            start_time = time.time()

            # Define the raw and processed data file path
            raw_data_path = str(paths["raw_data"] / (filename + ".zst"))
            processed_data_path = str(paths["processed_data"] / filename)

            decompress_zst(raw_data_path, processed_data_path)
            print(f'Decompressing {raw_data_path} took {readable_time(time.time() - start_time)}', flush=True)

            pgn_chunks = read_or_create_chunks(processed_data_path, cfg)
  
            print(f'Training {str} with {len(pgn_chunks)} chunks.', flush=True)
            
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
                    print(f'[{num_chunk}/{len(pgn_chunks)}]', flush=True)
                    print(f'[# Positions]: {readable_num(accumulated_samples)}', flush=True)
                    print(f'[# Games]: {readable_num(accumulated_games)}', flush=True)
                    print(f'[# Loss]: {loss} | [# Loss MAIA]: {loss_maia} | [# Loss Side Info]: {loss_side_info} | [# Loss Value]: {loss_value}', flush=True)
                    if num_chunk == len(pgn_chunks):
                        break

            num_file += 1
            print(f'### ({num_file} / {len(pgn_filenames)}) Took {readable_time(time.time() - start_time)} to train {processed_data_path} with {len(pgn_chunks)} chunks.', flush=True)
            os.remove(processed_data_path)
            
            torch.save({'model_state_dict': model.state_dict(),
                        'optimizer_state_dict': optimizer.state_dict(),
                        'accumulated_samples': accumulated_samples,
                        'accumulated_games': accumulated_games}, f'{save_root}epoch_{epoch + 1}_{pathlib.Path(processed_data_path).name}.pt')