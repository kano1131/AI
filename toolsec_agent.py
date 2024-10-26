import torch
import numpy as np
from typing import Optional,Tuple,List


class PrioritizedReplayBuffer:  # 优先级经验回放
    def __init__(self, capacity, device):
        self.capacity = capacity
        self.buffer = [None] * capacity
        self.priorities = torch.zeros(capacity, device=device)
        self.position = 0
        self.size = 0
        self.device = device

    def push(self, state, action, reward, next_state, done):
        max_priority = self.priorities.max() if self.size > 0 else 1.0

        self.buffer[self.position] = (state, action, reward, next_state, done)
        self.priorities[self.position] = max_priority

        self.position = (self.position + 1) % self.capacity
        self.size = min(self.size + 1, self.capacity)

    def sample(self, batch_size, priority_scale=1.0):
        sampling_probabilities = self.priorities[:self.size] ** priority_scale
        sampling_probabilities /= sampling_probabilities.sum().expand_as(sampling_probabilities)

        indices = torch.multinomial(sampling_probabilities, batch_size, replacement=True)

        state_batch = torch.cat([self.buffer[idx][0] for idx in indices])
        action_batch = torch.cat([self.buffer[idx][1] for idx in indices])
        reward_batch = torch.cat([self.buffer[idx][2] for idx in indices])
        next_state_batch = torch.cat([self.buffer[idx][3] for idx in indices])
        done_batch = torch.cat([self.buffer[idx][4] for idx in indices])

        return state_batch, action_batch, reward_batch, next_state_batch, done_batch
    
    def __len__(self):
        return self.size
