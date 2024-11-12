from .base_annotator import CollectorfoxAnnotator
from PIL import Image
import open_clip
import io
import numpy as np
import torch

class ClipAnnotator(CollectorfoxAnnotator):
    def __init__(self):
        model, _, preprocess = open_clip.create_model_and_transforms('ViT-B-32', pretrained='laion2b_s34b_b79k')
        model.eval()  # model in train mode by default, impacts some models with BatchNorm or stochastic depth active
        self.model = model
        self.preprocess = preprocess
    
    def annotate(self, input_data):
        images = [self.preprocess(Image.open(io.BytesIO(image_bytes)).convert("RGB")) for image_bytes in input_data]
        res = self.model.encode_image(torch.tensor(np.stack(images))).detach().numpy()
        print(res.shape)
        return res.tolist()
    
    def get_input_type(cls):
        return str
    
    def get_output_type(cls):
        return