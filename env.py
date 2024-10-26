from numpy import inf,float32,full,ndarray
import torch

class BinancePerpetualContract():
    def __init__(self, data: ndarray) -> None:

        #手续费
        self.Maker_conmission = 0.02/100
        self.Taker_conmission = 0.05/100
        
        pass
    def step(self, actions: torch.Tensor):
        pass
    def reset(self):
        pass
