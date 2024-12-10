import requests
import sys
import pyarrow as pa
import base64

if __name__ == "__main__":
    # Define the API URL
    url_1 = f"{sys.argv[1]}/write"
    url_2 = f"{sys.argv[2]}/write"
    
    DATASETS = [
        # 'utokyo_saytap_converted_externally_to_rlds',
        # 'utokyo_xarm_pick_and_place_converted_externally_to_rlds',
        # 'utokyo_xarm_bimanual_converted_externally_to_rlds',
        # 'robo_net',
        # 'berkeley_mvp_converted_externally_to_rlds',
        # 'berkeley_rpt_converted_externally_to_rlds',
        # 'kaist_nonprehensile_converted_externally_to_rlds',
        # 'stanford_mask_vit_converted_externally_to_rlds',
        # 'tokyo_u_lsmo_converted_externally_to_rlds',
        # 'dlr_sara_pour_converted_externally_to_rlds',
        # 'dlr_sara_grid_clamp_converted_externally_to_rlds',
        # 'dlr_edan_shared_control_converted_externally_to_rlds',
        # 'asu_table_top_converted_externally_to_rlds',
        # 'stanford_robocook_converted_externally_to_rlds',
        # 'eth_agent_affordances',
        'imperialcollege_sawyer_wrist_cam',
        # 'iamlab_cmu_pickup_insert_converted_externally_to_rlds',
        # 'uiuc_d3field',
        # 'utaustin_mutex',
        # 'berkeley_fanuc_manipulation',
        # 'cmu_play_fusion',
        # 'cmu_stretch',
        # 'berkeley_gnm_recon',
        # 'berkeley_gnm_cory_hall',
        'berkeley_gnm_sac_son'
    ]
    
    for dataset in DATASETS:
    
        # Define the payload
        data = {
            "src_type": "rtx", # load lancedb from this uri
            "src_uri": f"gs://gresearch/robotics/{dataset}/0.1.0",
            "dataset" : "lerobot_universal",
        }

        # Send the POST request
        response = requests.post(url_1, json=data)
        print(response.status_code)
        print(response.json())

        response = requests.post(url_2, json=data)
        print(response.status_code)
        print(response.json())