import api
from collector import collectorfox
from transformation_impl import RTXPipeline
from annotation_impl import TextAnnotator

if __name__ == '__main__':
    collectorfox.register_transformation("cmu_stretch", "rtx", RTXPipeline(step_data_sample_col=["language_instruction", "observation"], step_data_sample_method="first"))
    collectorfox.register_transformation("ucsd", "rtx", RTXPipeline())
    collectorfox.register_annotators("cmu_stretch", "language_instruction", "language_embedding", TextAnnotator("sentence-transformers/all-MiniLM-L6-v2"))
    collectorfox.register_annotators("ucsd", "language_instruction", "language_embedding", TextAnnotator("sentence-transformers/all-MiniLM-L6-v2"))
    collectorfox.run()