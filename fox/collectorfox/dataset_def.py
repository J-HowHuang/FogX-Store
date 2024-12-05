from dataset import FoxDatasetDefinition
from transformation import RTXPipeline, LeRobotPipeline, ArPipeline
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
        
class LeRobotUniversal(FoxDatasetDefinition):
    def __init__(self):
        super().__init__("lerobot_universal", pa.schema([
                pa.field("language_instruction", pa.string()),
                pa.field("observation", pa.binary())
        ]))
        
        self.add_transformation("rtx", LeRobotPipeline(step_data_sample_col=["language_instruction", "observation"], step_data_sample_method="first"))
        self.add_annotators("language_instruction", "language_embedding", TextAnnotator("sentence-transformers/all-MiniLM-L6-v2"))
        self.add_annotators("observation", "clip_embedding", ClipAnnotator())
        # add transformations
        
class ARDataset(FoxDatasetDefinition):
    def __init__(self):
        super().__init__("ar_dataset", pa.schema([
            pa.field("test_id", pa.string()),
            pa.field("task_id", pa.string()),
            pa.field("traj_len", pa.int64()),
            pa.field("observation", pa.binary())
        ]))
        
        self.add_transformation("ar", ArPipeline(episodes_per_task=5, step_data_sample_col=["observation"], step_data_sample_method="first"))
        self.add_annotators("observation", "clip_embedding", ClipAnnotator())
        


