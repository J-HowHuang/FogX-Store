from .base_transformation import CollectorfoxTransformation
import tensorflow_datasets as tfds
from collections import defaultdict
import numpy as np
import pyarrow as pa
import tensorflow as tf
from typing import Literal, Optional, List
from PIL import Image
import io
from pathlib import Path
import tensorflow as tf
import torch
import json

CHANNEL = ['quest_control', 'stretch_status', 'meta_2_head_cam']

class DatasetIterator:
    def __init__(self, src_path: str, episodes_per_task: int = 5):
        self.src_path = src_path
        self.episodes_per_task = episodes_per_task
    
    def _process_step_data(self, raw_dataset):
        dataset_group_by_channel = defaultdict(list)

        for data in raw_dataset:
            dataset_group_by_channel[data['channel']].append(data)

        # Get trajetory length
        traj_len = min([len(val) for _, val in dataset_group_by_channel.items()])

        # Drop redundant steps
        for key, _ in dataset_group_by_channel.items():
            dataset_group_by_channel[key] = dataset_group_by_channel[key][:traj_len]
        
        steps = defaultdict(list)

        # build up the step data
        for channel in CHANNEL:
            for step in dataset_group_by_channel[channel]:
                for key, data in step['data'].items():
                    new_key = f"{channel}.{key}"
                    if new_key == 'meta_2_head_cam.image':
                       new_key = 'observation'
                       # convert string to bytes
                       data = bytes.fromhex(data)
                    steps[new_key].append(data)
        return steps, traj_len

    def _process_episode_data(self, raw_dataset):
        """TODO - Implement this function"""
        raise NotImplementedError

    def __iter__(self):
        # src_path structure:
        # raw_data_no_image
        # ├── test1
        # │   ├── episode1.jsonl -> each line of this file is a step
        # │   ├── episode2.jsonl
        # │   ├── episode3.jsonl
        # │   └── episode4.jsonl
        # ├── test2
        # │   ├── episode1.jsonl 
        # │   ├── episode2.jsonl
        # │   ├── episode3.jsonl
        # │   └── episode4.jsonl
        
        test_id_curr_task_id = defaultdict(int)
        task_id_tracker = defaultdict(int)
        
        for path in Path(self.src_path).rglob('*.jsonl'):
            test_id = path.parent.name
            metadata = defaultdict(list)
            
            # update task_id every five episodes
            task_id_tracker[test_id] += 1
            if task_id_tracker[test_id] % self.episodes_per_task == 0:
                task_id_tracker[test_id] = 1
                test_id_curr_task_id[test_id] += 1
                
            with open(path, 'r') as f:
                raw_dataset = [json.loads(line) for line in f]
                steps, traj_len = self._process_step_data(raw_dataset)
                metadata['test_id'].append(test_id)
                metadata['task_id'].append(f"task_{test_id_curr_task_id[test_id]}")
                metadata['traj_len'].append(traj_len)
                yield metadata, steps

class ArPipeline(CollectorfoxTransformation):
    def __init__(self, step_data_sample_col: Optional[List[str]]=None, step_data_sample_method: Optional[Literal['first', 'last', 'mid']]=None, episodes_per_task: int = 5):
        super().__init__()
        self.sample_col = step_data_sample_col
        self.sample_method = step_data_sample_method
        self.episodes_per_task = episodes_per_task
        self.dataset_name = None
    
    def get_iterator(self, src_path: str, dataset_name: str = None):
        self.dataset_name = dataset_name
        
        # load all the jsonl files in src_path
        self.data_iter = iter(DatasetIterator(src_path, self.episodes_per_task))
        return self.__iter__()
    
    def __next__(self):
        episode, step_data = next(self.data_iter)
        
        sample_step_idx = -1
        if self.sample_method is None:
            sample_step_idx = -1   
        elif self.sample_method == 'first':
            sample_step_idx = 0
        elif self.sample_method == 'last':
            sample_step_idx = episode['traj_len']
        elif self.sample_method == 'mid':
            sample_step_idx = episode['traj_len'] // 2
            
        
        for col in self.sample_col:
            print(f"Sampling {col} at step {sample_step_idx}")
            print(len(step_data))
            episode[col] = [step_data[col][sample_step_idx]]
            
        return episode, pa.table(step_data)
    
            