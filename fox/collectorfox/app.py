import api
from collector import collectorfox
from transformation import LeRobotPipeline
from annotation import TextAnnotator, ClipAnnotator

if __name__ == '__main__':
    collectorfox.register_transformation("cmu_stretch", "rtx", LeRobotPipeline(step_data_sample_col=["language_instruction", "observation"], step_data_sample_method="first"))
    collectorfox.register_transformation("universal", "rtx", LeRobotPipeline(step_data_sample_col=["language_instruction", "observation"], step_data_sample_method="first"))
    collectorfox.register_transformation("ucsd", "rtx", LeRobotPipeline(step_data_sample_col=["language_instruction", "observation"], step_data_sample_method="first"))
    collectorfox.register_annotators("cmu_stretch", "language_instruction", "language_embedding", TextAnnotator("sentence-transformers/all-MiniLM-L6-v2"))
    collectorfox.register_annotators("universal", "language_instruction", "language_embedding", TextAnnotator("sentence-transformers/all-MiniLM-L6-v2"))
    collectorfox.register_annotators("universal", "observation", "clip_embedding", ClipAnnotator())
    collectorfox.register_annotators("cmu_stretch", "observation", "clip_embedding", ClipAnnotator())
    collectorfox.register_annotators("ucsd", "language_instruction", "language_embedding", TextAnnotator("sentence-transformers/all-MiniLM-L6-v2"))
    collectorfox.register_annotators("ucsd", "observation", "clip_embedding", ClipAnnotator())
    collectorfox.run()