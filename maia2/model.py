# import gdown
import os
from .main import MAIA2Model
from .utils import get_all_possible_moves, create_elo_dict, parse_cfg
import torch
from torch import nn
import warnings
warnings.filterwarnings("ignore")
import pdb
from typing import Literal

# def from_pretrained(type, device, save_root = "./maia2_models"):
    
#     if os.path.exists(save_root) == False:
#         os.makedirs(save_root)
    
#     if type == "blitz":
#         url = "https://drive.google.com/uc?id=1X-Z4J3PX3MQFJoa8gRt3aL8CIH0PWoyt"
#         output_path = os.path.join(save_root, "blitz_model.pt")
    
#     elif type == "rapid":
#         url = "https://drive.google.com/uc?id=1gbC1-c7c0EQOPPAVpGWubezeEW8grVwc"
#         output_path = os.path.join(save_root, "rapid_model.pt")
    
#     else:
#         raise ValueError("Invalid model type. Choose between 'blitz' and 'rapid'.")

#     if os.path.exists(output_path):
#         print(f"Model for {type} games already downloaded.")
#     else:
#         print(f"Downloading model for {type} games.")
#         gdown.download(url, output_path, quiet=False)

#     cfg_url = "https://drive.google.com/uc?id=1GQTskYMVMubNwZH2Bi6AmevI15CS6gk0"
#     cfg_path = os.path.join(save_root, "config.yaml")
#     if not os.path.exists(cfg_path):
#         gdown.download(cfg_url, cfg_path, quiet=False)

#     cfg = parse_args(cfg_path)

#     all_moves = get_all_possible_moves()
#     elo_dict = create_elo_dict()

#     model = MAIA2Model(len(all_moves), elo_dict, cfg)
#     model = nn.DataParallel(model)
    
#     checkpoint = torch.load(output_path, map_location='cpu')
#     model.load_state_dict(checkpoint['model_state_dict'])
#     model = model.module
    
#     if device == "gpu":
#         model = model.cuda()
    
#     print(f"Model for {type} games loaded to {device}.")
    
#     return model

# This is just an experimental test
# cp saves/*/checkpoint{filename}.pgn.pt -> ./maia2_models/blitz_model.pt
# Example: cp saves/0.0001_8192_1e-05/epoch_3_blitz_games_2013_12.pgn.pt  maia2_models/blitz_model.pt
def from_pretrained(time_control_format: Literal["blitz", "rapid"], device: Literal["cpu", "gpu"], save_root="./maia2_models"):
    if os.path.exists(save_root) == False:
        raise FileNotFoundError(f"Directory {save_root} does not exist within the project root directory")
    
    if time_control_format == "blitz":
        output_path = os.path.join(save_root, "blitz_model.pt")

    elif time_control_format == "rapid":
        raise NotImplementedError("The model for rapid game time control has not been implemented yet")
    else:
        raise ValueError("Invalid chess game time control format passed. Choose between 'blitz' or 'rapid'")
    
    if os.path.exists(output_path) == False:
        raise FileNotFoundError(f"Model file path {output_path} does not exist")
    
    cfg_path = os.path.join(save_root, "config.yaml")

    if not os.path.exists(cfg_path):
        raise FileNotFoundError(f"Model configuration file not found within {cfg_path}")
    
    cfg = parse_cfg(cfg_path)

    all_moves = get_all_possible_moves()
    elo_dict = create_elo_dict()

    model = MAIA2Model(len(all_moves), elo_dict, cfg)
    model = nn.DataParallel(model)
    
    checkpoint = torch.load(output_path, map_location='cpu')
    model.load_state_dict(checkpoint['model_state_dict'])
    model = model.module
    
    if device == "gpu":
        model = model.cuda()
    
    print(f"Model for {time_control_format} games loaded to {device}.")
    
    return model
