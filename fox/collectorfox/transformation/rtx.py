from .base_transformation import CollectorfoxTransformation

import tensorflow_datasets as tfds
from collections import defaultdict
import numpy as np
import pyarrow as pa
import tensorflow as tf
from typing import Literal, Optional, List
from PIL import Image
import io

GCS_TOP_K=5

def unpack_tensor(tensor):
    if tensor.dtype == tf.string:
        return tensor.numpy().decode('utf-8')
    if not isinstance(tensor, np.ndarray):
        tensor = tensor.numpy()  # Assuming it's a TensorFlow or PyTorch tensor
    return tensor

def get_step_data_content(step_data, column):
    value = step_data.get(column, None)
    if value is None:
        return None
    if column == "observation":
        image_tensor = value.get("image")
        image_np = unpack_tensor(image_tensor)
        image = Image.fromarray(image_np)
        buf = io.BytesIO()
        image.save(buf, format='PNG')
        return buf.getvalue()
    
    return unpack_tensor(value)
    

class RTXPipeline(CollectorfoxTransformation):
    def __init__(self, step_data_sample_col: Optional[List[str]]=None, step_data_sample_method: Optional[Literal['first', 'last', 'mid']]=None):
        super().__init__()
        self.sample_col = step_data_sample_col
        self.sample_method = step_data_sample_method
    
    def get_iterator(self, src_path):
        assert(src_path.startswith('gs://'))
        print("Loading dataset from {}".format(src_path))
        b = tfds.builder_from_directory(builder_dir=src_path)
        self.ds = b.as_dataset(split='train[:{}]'.format(GCS_TOP_K))
        self.episode_iter = iter(self.ds)
        return self.__iter__()
    
    def __next__(self):
        episode = next(self.episode_iter)
        step_data = defaultdict(list)

        episode_table = {}
        
        sample_step_idx = -1
        if self.sample_method is None:
            sample_step_idx = -1   
        elif self.sample_method == 'first':
            sample_step_idx = 0
        elif self.sample_method == 'last':
            sample_step_idx = len(episode['steps'])
        elif self.sample_method == 'mid':
            sample_step_idx = len(episode['steps']) // 2
            
        
        for key, value in episode['episode_metadata'].items():
            value = unpack_tensor(value)
            episode_table[key] = [value]
        for i, step in enumerate(episode['steps']):
            if i == sample_step_idx:
                for col in self.sample_col:
                    value = get_step_data_content(step, col)
                    episode_table[col] = [value]
            for key in step.keys():
                value = get_step_data_content(step, key)
                step_data[key].append(value)
        return episode_table, pa.table(step_data)
    
            