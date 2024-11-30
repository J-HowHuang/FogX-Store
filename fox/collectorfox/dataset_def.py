from dataset import FoxDatasetDefinition
from transformation import RTXPipeline
from annotation import TextAnnotator, ClipAnnotator
import pyarrow as pa

class CMUStretch(FoxDatasetDefinition):
    def __init__(self):
        super().__init__("cmu_stretch", pa.schema([
            pa.field("file_path", pa.string()),
            pa.field("language_instruction", pa.string()),
            pa.field("observation", pa.binary())
        ]))
        # add transformations
        self.add_transformation("rtx", RTXPipeline(step_data_sample_col=["language_instruction", "observation"], step_data_sample_method="first"))
        self.add_annotators("language_instruction", "language_embedding", TextAnnotator("sentence-transformers/all-MiniLM-L6-v2"))
        self.add_annotators("observation", "clip_embedding", ClipAnnotator())