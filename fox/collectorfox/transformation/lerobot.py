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
default_step_cols = [
    'observation.images.image_wrist',
    'observation.images.wrist45_image',
    'observation.images.wrist225_image',
    'observation.images.image1',
    'next.reward',
    'observation.images.image_2',
    'next.done',
    'observation.images.highres_image',
    'observation.images.hand_image',
    'observation.images.image_additional_view',
    'action',
    'observation.images.eye_in_hand_rgb',
    'observation.state',
    'frame_index',
    'observation.images.top_image',
    'observation.images.front_rgb',
    'observation.images.rgb_gripper',
    'observation.images.rgb_static',
    'observation.images.wrist_image',
    'observation.images.rgb',
    'index',
    'observation.images.agentview_rgb',
    'observation.images.image_1',
    'observation.images.image2',
    'timestamp',
    'episode_index',
    'language_instruction',
    'observation.images.image']

default_schema = pa.schema([
    ('observation.images.image_wrist', pa.binary()),
    ('observation.images.wrist45_image', pa.binary()),
    ('observation.images.wrist225_image', pa.binary()),
    ('observation.images.image1', pa.binary()),
    ('next.reward', pa.float32()),  # Keeping as float
    ('observation.images.image_2', pa.binary()),
    ('next.done', pa.bool_()),  # Keeping as bool
    ('observation.images.highres_image', pa.binary()),
    ('observation.images.hand_image', pa.binary()),
    ('observation.images.image_additional_view', pa.binary()),
    ('action', pa.list_(pa.float32())),  # Assuming list of floats
    ('observation.images.eye_in_hand_rgb', pa.binary()),
    ('observation.state', pa.list_(pa.float32())),  # Assuming list of floats
    ('frame_index', pa.int64()),
    ('observation.images.top_image', pa.binary()),
    ('observation.images.front_rgb', pa.binary()),
    ('observation.images.rgb_gripper', pa.binary()),
    ('observation.images.rgb_static', pa.binary()),
    ('observation.images.wrist_image', pa.binary()),
    ('observation.images.rgb', pa.binary()),
    ('index', pa.int64()),
    ('observation.images.agentview_rgb', pa.binary()),
    ('observation.images.image_1', pa.binary()),
    ('observation.images.image2', pa.binary()),
    ('timestamp', pa.float64()),  # Assuming timestamp as float
    ('episode_index', pa.int64()),
    ('language_instruction', pa.string()),
    ('observation.images.image', pa.binary()),
    ('dataset_name', pa.string())
])
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
    bytes = buffer.getvalue()
    del buffer
    return bytes

def group_lerobot_steps_with_episodes(dataset):
    # map episode index to the dataset
    dataset_by_episode = []
    grouped_data = {}
    episode_indexes = set(dataset['episode_index'].numpy())
    for target_index in episode_indexes:
        mask = dataset['episode_index'] == target_index
        for key in dataset.keys():
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
        self.dataset_name = None
    
    def get_iterator(self, src_path: str, dataset_name: str = None):
        assert(src_path.startswith('gs://')) # only support GCS for now
        print("Loading dataset from {}".format(src_path))
        self.dataset_name = src_path.split('/')[-2]
        videos_dir = Path("./video")
        self.lerobot_steps_dict = load_from_raw(
            raw_dir=src_path,
            videos_dir=videos_dir,
            fps=3, 
            video=False,
            openx_dataset_name=self.dataset_name,
            split='train',
            topk=5
        )
        self.grouped_steps_iter = iter(group_lerobot_steps_with_episodes(self.lerobot_steps_dict))
        return self.__iter__()
    
    def __next__(self):
        steps = next(self.grouped_steps_iter)
        
        steps_len = len(steps['action'])
        
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
        print("Observation column set: ", observation_col_set)
        print("Step keys: ", steps.keys())
        print(f"Observation column: {observation_col}")
        if observation_col == "":
            episode_table['observation'] = [""]
        else:
            value = steps[observation_col][sample_step_idx]
            episode_table['observation'] = [value]
            print(type(value))
       
        
        
        if 'language_instruction' not in steps:
            episode_table['language_instruction'] = [""]
        else:
            value = steps['language_instruction'][sample_step_idx]
            episode_table['language_instruction'] = [value]
            
        # if any default column is missing, fill with None
        extended_steps = {}
        for col in default_step_cols:
            if col not in steps:
                extended_steps[col] = [None for _ in range(steps_len)]
            else:
                extended_steps[col] = steps[col]
        extended_steps['dataset_name'] = [self.dataset_name] * steps_len
        
        print('extended_steps: ', extended_steps.keys())
        
        return episode_table, pa.table(extended_steps, schema=default_schema)
    
            