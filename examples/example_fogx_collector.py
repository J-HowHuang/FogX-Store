import fog_x

# 🦊 Dataset Creation 
# from distributed dataset storage 
dataset1 = fog_x.Dataset(
    name="demo_ds_1",
    path="./test_dataset", # can be AWS S3, Google Bucket! 
)  

# dataset2 = fog_x.Dataset(
#     name="demo_ds_2",
#     path="~/test_dataset", # can be AWS S3, Google Bucket! 
# )  

# 🦊 Data collection: 
# create a new trajectory
# episode = dataset1.new_episode()
# # collect step data for the episode
# episode.add(feature = "collector", value = "user 1")
# # Automatically time-aligns and saves the trajectory
# episode.close()

# episode = dataset2.new_episode()
# # collect step data for the episode
# episode.add(feature = "collector", value = "user 2")
# # Automatically time-aligns and saves the trajectory
# episode.close()

dataset1.load_rtx_episodes(
    name="berkeley_autolab_ur5",
    split="train[3:5]",
    additional_metadata={"collector": "User 2", "custom_tag": "Partition_2"},
)


# 🦊 Data Loading:
# load from existing RT-X/Open-X datasets
# dataset.load_rtx_episodes(
#     name="berkeley_autolab_ur5",
#     additional_metadata={"collector": "User 2"}
# )

# 🦊 Data Management and Analytics: 
# Compute and memory efficient filter, map, aggregate, groupby
episode_info = dataset1.get_episode_info()
# desired_episodes = episode_info.filter(episode_info["collector"] == "User 2")
print(episode_info)