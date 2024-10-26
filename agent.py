import numpy as np
import torch
import torch.cuda



class Agrnt():
    def __init__(self) -> None:
        pass

    def update_eps(self) -> None:
        pass

    def select_action_train(self, data) -> torch.Tensor:
        pass

    def update(self) -> torch.Tensor:
        pass