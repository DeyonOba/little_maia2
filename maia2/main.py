import torch
import torch.nn as nn
from torch.nn import functional as F


class BasicBlock(nn.Module):
    def __init__(self, in_planes, planes, stride=1):
        super(BasicBlock, self).__init__()
        
        mid_planes = planes
        
        self.conv1 = nn.Conv2d(in_planes, mid_planes, kernel_size=3, stride=stride, padding=1, bias=False)
        self.bn1 = nn.BatchNorm2d(mid_planes)
        self.conv2 = nn.Conv2d(mid_planes, planes, kernel_size=3, stride=1)
        self.bn2 = nn.BatchNorm2d(planes)
        self.dropout = nn.Dropout(0.5)
    
    def forward(self, x):
        out = self.conv1(x)
        out = self.bn1(out)
        out = F.relu(out)
        out = self.dropout(out)
        
        out = self.conv2(out)
        out = self.bn2(out)
        out += x
        out = F.relu(out)
        
        return out

class ChessResnet(nn.Module):
    def __init__(self, block, cfg):
        super(ChessResnet, self).__init__()
        
        self.conv1 = nn.Conv2d(cfg.input_channels, cfg.dim_cnn, kernel_size=3, stride=1, padding=1, bias=False)
        self.bn1 = nn.BatchNorm2d(cfg.dim_cnn)
        self.layers = self._make_layer(block, cfg.dim_cnn, cfg.num_blocks_cnn)
        self.conv_last = nn.Conv2d(cfg.dim_cnn, cfg.vit_length, kernel_size=3, stride=1, padding=1, bias=False)
        self.bn_last = nn.BatchNorm2d(cfg.vit_length)
        
    def _make_layer(self, block, planes, num_blocks, stride=1):
        layers = []
        
        for _ in range(num_blocks):
            layers.append(block(planes, planes, stride))
        return nn.Sequential(*layers)
    
    def forward(self, x):
        out = F.relu(self.bn1(self.conv1(x)))
        out = self.layers(out)
        out = self.conv_last(out)
        out = self.bn_last(out)
        
        return out