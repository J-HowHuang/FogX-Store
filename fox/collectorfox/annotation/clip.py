from .base_annotator import CollectorfoxAnnotator
from PIL import Image
import open_clip
import io
import numpy as np
import torch
import os
import pyarrow as pa

class ClipAnnotator(CollectorfoxAnnotator):
    def __init__(self):
        model, _, preprocess = open_clip.create_model_and_transforms('ViT-B-32', pretrained='laion2b_s34b_b79k', cache_dir=os.getenv("CACHE_DIR"))
        model.eval()  # model in train mode by default, impacts some models with BatchNorm or stochastic depth active
        self.model = model
        self.preprocess = preprocess
    
    def annotate(self, input_data):
        images = [self.preprocess(Image.open(io.BytesIO(image_bytes)).convert("RGB")) for image_bytes in input_data]
        res = self.model.encode_image(torch.tensor(np.stack(images))).detach().numpy()
        print(res.shape)
        return res.tolist()
    
    def get_annotator_name(cls):
        return "openai/clip-vit-base-patch32"
    
    def get_output_type(cls):
        return pa.list_(pa.float32(), 512)