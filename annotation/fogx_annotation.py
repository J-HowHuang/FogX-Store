# -*- coding: utf-8 -*-
"""fogx_annotation.ipynb

Automatically generated by Colab.

Original file is located at
    https://colab.research.google.com/drive/1j27zLr1PHwUjR5wnjmKfRh8CV0294JAK
"""

# !pip install -r requirements.txt
# !pip install --upgrade transformers

# load dataset
import tensorflow_datasets as tfds
# load models
from transformers import AutoModelForCausalLM
from transformers import AutoProcessor
from transformers import Qwen2VLForConditionalGeneration
from transformers import AutoTokenizer
# requests
import requests
# image and torch
from PIL import Image
import torch
import numpy as np
import pyarrow as pa
# show image
import matplotlib.pyplot as plt


# Get data from the dataset builder
# Store each image in each episode
# Each image is a tensor of shape [224, 224, 3]

### Load data from RLDS
### Sample data, 10 rlds data
b = tfds.builder_from_directory(builder_dir="gs://gresearch/robotics/ucsd_pick_and_place_dataset_converted_externally_to_rlds/0.1.0")
ds = b.as_dataset(split='train[:10]').shuffle(10)

# Initialize a list to store images extracted from the episodes
# Store each image in each episode
images = []

# Loop through each episode in the dataset
for episode in ds:
    # Flag to indicate if the first image in the current episode has been retrieved
    get = False

    # Iterate through each step in the episode
    for step_id, step in enumerate(episode['steps']):
        for key, data in step.items():
            if key == 'observation':
                images.append(data['image'])
                get = True

        # Exit the loop once the first observation image is found in this episode
        if get:
            break


### Helper function: input image tensor, show image using matplotlib
def show_image(image_tensor):
    '''
    input:
        image_tensor: a tensor of shape [224, 224, 3]
    show the image
    '''
    image_array = image_tensor.numpy()
    plt.figure(figsize=(8, 8))
    plt.imshow(image_array)
    # plt.axis('off')
    plt.show()

    ### OR: Convert to PIL image to show
    # image = Image.fromarray(image_tensor.numpy())
    # display(image)

### Helper function: Check the number of images per episode
def count_images_per_episode(ds):
    # Count the number of images in each episode (count only the steps that include observations)
    for idx, episode in enumerate(ds):
        num_images = sum(1 for step in episode['steps'] if 'observation' in step and 'image' in step['observation'])
        print(f"Episode {idx + 1} has {num_images} images.")


### Helper function, generate image annotation using Qwen (hugging face) model
def generate_image_annotations_Qwen(images, handle_type="image", prompt_type="text", prompt="Describe this image."):
    """
    Generates annotations for the input list of images and returns the pyarrow table containing the annotations.

    input:
        images (list of torch.Tensor): each image's shape is (224, 224, 3)

    return:
        pyarrow.Table: column names: episode_number、first_image、image_annotation
    """
    # Load the model in half-precision on the available device(s)
    device = "cuda" if torch.cuda.is_available() else "cpu"
    model = Qwen2VLForConditionalGeneration.from_pretrained(
        "Qwen/Qwen2-VL-2B-Instruct", torch_dtype="auto", device_map=device
    )
    processor = AutoProcessor.from_pretrained("Qwen/Qwen2-VL-2B-Instruct")

    # Initialize lists for table columns
    episode_numbers = []
    first_images = []
    annotations = []

    for idx, image in enumerate(images):
        # Convert image tensor to PIL format
        pil_image = Image.fromarray(images[idx].numpy().astype('uint8'))

        # Create conversation template
        conversation = [
            {
                "role": "user",
                "content": [
                    {
                        "type": handle_type,
                    },
                    {"type": prompt_type, "text": prompt},
                ],
            }
        ]

        # Preprocess the inputs
        text_prompt = processor.apply_chat_template(conversation, add_generation_prompt=True)
        # Excepted output: '<|im_start|>system\nYou are a helpful assistant.<|im_end|>\n<|im_start|>user\n<|vision_start|><|image_pad|><|vision_end|>Describe this image.<|im_end|>\n<|im_start|>assistant\n'

        inputs = processor(
            text=[text_prompt], images=[image], padding=True, return_tensors="pt"
        )
        inputs = inputs.to("cuda")

        # Inference: Generation of the output
        output_ids = model.generate(**inputs, max_new_tokens=128)
        generated_ids = [
            output_ids[len(input_ids) :]
            for input_ids, output_ids in zip(inputs.input_ids, output_ids)
        ]
        output_text = processor.batch_decode(
            generated_ids, skip_special_tokens=True, clean_up_tokenization_spaces=True
        )

        # Collect data for the table
        episode_numbers.append(idx)
        # store image numpy (originally image is tensor)
        first_images.append(image.numpy().flatten())
        annotations.append(output_text)

    # Create PyArrow table
    table = pa.table({
        "episode_number": episode_numbers,
        "first_image": first_images,
        "image_annotation": annotations,
    })

    return table

# Test:
# table = generate_image_annotations_Qwen(images[:3])


### Helper function, generate image annotation using Phi (hugging face) model
def generate_image_annotations_Phi(images, handle_type="image", prompt_type="text", text_prompt="Describe this image."):
    """
    Generates annotations for the input list of images and returns the pyarrow table containing the annotations.

    input:
        images (list of torch.Tensor): each image's shape is (224, 224, 3)

    return:
        pyarrow.Table: column names: episode_number、first_image、image_annotation
    """
    model_id = "microsoft/Phi-3.5-vision-instruct"
    device = "cuda" if torch.cuda.is_available() else "cpu"
    # Note: set _attn_implementation='eager' if you don't have flash_attn installed
    model = AutoModelForCausalLM.from_pretrained(
    model_id,
    device_map=device,
    trust_remote_code=True,
    torch_dtype="auto",
    _attn_implementation='eager'
    )

    # for best performance, use num_crops=4 for multi-frame, num_crops=16 for single-frame.
    processor = AutoProcessor.from_pretrained(model_id,
    trust_remote_code=True,
    num_crops=4
    )

    # Initialize lists for table columns
    episode_numbers = []
    first_images = []
    annotations = []

    for idx, image in enumerate(images):
        # Convert image tensor to PIL format
        images_temp = [Image.fromarray(images[idx].numpy().astype('uint8'))]
        placeholder = f"<|image_{1}|>\n"

        messages = [
            {"role": "user",
                "content": placeholder + text_prompt},
        ]
        prompt = processor.tokenizer.apply_chat_template(
        messages,
        tokenize=False,
        add_generation_prompt=True
        )

        inputs = processor(prompt, images_temp, return_tensors="pt").to(device)

        generation_args = {
            "max_new_tokens": 1000,
            "temperature": 0.0,
            "do_sample": False,
        }

        generate_ids = model.generate(**inputs,
        eos_token_id=processor.tokenizer.eos_token_id,
        **generation_args
        )

        # remove input tokens
        generate_ids = generate_ids[:, inputs['input_ids'].shape[1]:]
        response = processor.batch_decode(generate_ids,
        skip_special_tokens=True,
        clean_up_tokenization_spaces=False)[0]

        # print('-----------', idx, '------------')
        # print(response)

        # add to result
        episode_numbers.append(idx)
        # store image numpy (originally image is tensor)
        first_images.append(image.numpy().flatten())
        annotations.append(response)

        # Create PyArrow table
        table = pa.table({
            "episode_number": episode_numbers,
            "first_image": first_images,
            "image_annotation": annotations,
        })

    # Create PyArrow table
    table = pa.table({
        "episode_number": episode_numbers,
        "first_image": first_images,
        "image_annotation": annotations,
    })

    return table

# Test
# table = generate_image_annotations_Phi(images[:3])
# print(len(table))


### Annotator function
def annotator(input_model_info, images):
    '''
    input:
        input_model_info: a tuple of model and handle type, like (model_id, type='image')
            eg: assume current models = ("microsoft/Phi-3.5-vision-instruct", 'image')
        images: list of images (each image is a tensor of shape [224, 224, 3])

    return:
        pyarrow table
        table schema:
            column name:    episode_number |  first_image  |  image_annotation
            type:           int               tensor           string
    '''
    (model_id, handle_type) = input_model_info

    # different model use different model to generate annotation
    if model_id == "microsoft/Phi-3.5-vision-instruct":
        pyarrow_table = generate_image_annotations_Phi(images, handle_type=handle_type)
    if model_id == "Qwen/Qwen2-VL-2B-Instruct":
        pyarrow_table = generate_image_annotations_Qwen(images, handle_type=handle_type)

    return pyarrow_table

### Test
# input_model_info = ("Qwen/Qwen2-VL-2B-Instruct", "image")
# table = annotator(input_model_info, images)
# print(len(table))
# print(table)
