from .base_transformation import CollectorfoxTransformation
from lerobot.common.datasets.push_dataset_to_hub.openx_rlds_format import load_from_raw
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

GCS_TOP_K=5

img_in_steps = set()

def unpack_tensor(tensor):
    if tensor.dtype == tf.string:
        return tensor.numpy().decode('utf-8')
    if not isinstance(tensor, np.ndarray):
        tensor = tensor.numpy()  # Assuming it's a TensorFlow or PyTorch tensor
    return tensor

def image_to_bytes(image: Image.Image, format="PNG"):
    # Convert image to bytes
    buffer = io.BytesIO()
    image.save(buffer, format=format)
    return buffer.getvalue()

def group_lerobot_steps_with_episodes(dataset):
    # map episode index to the dataset
    dataset_by_episode = []
    grouped_data = {}
    episode_indexes = set(dataset['episode_index'].numpy())
    for target_index in episode_indexes:
        mask = dataset['episode_index'] == target_index
        for key in dataset.keys():
            print(key, type(dataset[key][0]))
            if isinstance(dataset[key], torch.Tensor) or isinstance(dataset[key], tf.Tensor):
                grouped_data[key] = dataset[key][mask].numpy()
                if len(grouped_data[key].shape) == 2:
                    grouped_data[key] = [row for row in grouped_data[key]]
            else:
                if all([type(v) == Image.Image for v in dataset[key]]):
                    img_in_steps.add(key)
                    grouped_data[key] = [image_to_bytes(v) for v, m in zip(dataset[key], mask) if m]
                else:
                    grouped_data[key] = [v for v, m in zip(dataset[key], mask) if m]
                            
        dataset_by_episode.append(grouped_data)
    return dataset_by_episode
    

class LeRobotPipeline(CollectorfoxTransformation):
    def __init__(self, step_data_sample_col: Optional[List[str]]=None, step_data_sample_method: Optional[Literal['first', 'last', 'mid']]=None):
        super().__init__()
        self.sample_col = step_data_sample_col
        self.sample_method = step_data_sample_method
    
    def get_iterator(self, src_path: str, dataset_name: str = None):
        assert(src_path.startswith('gs://')) # only support GCS for now
        print("Loading dataset from {}".format(src_path))
        
        if dataset_name is None: # dangerous
            dataset_name = src_path.split('/')[-2]
        
        videos_dir = Path("./video")
        self.lerobot_steps_dict = load_from_raw(
            raw_dir=src_path,
            videos_dir=videos_dir,
            fps=3, 
            video=False,
            openx_dataset_name=dataset_name,
            split='train',
            topk=GCS_TOP_K
        )
        self.grouped_steps_iter = iter(group_lerobot_steps_with_episodes(self.lerobot_steps_dict))
        return self.__iter__()
    
    def __next__(self):
        steps = next(self.grouped_steps_iter)
        
        episode_table = {}
        
        sample_step_idx = -1
        if self.sample_method is None:
            sample_step_idx = -1   
        elif self.sample_method == 'first':
            sample_step_idx = 0
        elif self.sample_method == 'last':
            sample_step_idx = len(steps)
        elif self.sample_method == 'mid':
            sample_step_idx = len(steps) // 2

        observation_col_set = set(steps.keys()).intersection(img_in_steps)
        # take the first image column as observation
        observation_col = observation_col_set.pop() if len(observation_col_set) > 0 else ""
        if observation_col == "":
            episode_table['observation'] = [""]
        else:
            value = steps[observation_col][sample_step_idx]
            episode_table['observation'] = [value]
       
        
        
        if 'language_instruction' not in steps:
            episode_table['language_instruction'] = [""]
        else:
            value = steps['language_instruction'][sample_step_idx]
            episode_table['language_instruction'] = [value]
        
        return episode_table, pa.table(steps)
    
            