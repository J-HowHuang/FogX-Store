from flask import Flask, request, jsonify, render_template, redirect, url_for
import pandas as pd
import numpy as np
import base64
from io import BytesIO

# ### Need import from SkulkClient!!!
# from skulk.client import SkulkClient
# from skulk.server import SkulkServer
# from skulk.core.query import SkulkQuery
# from skulkclient.client import SkulkClient
# from skulkclient.predatorfox.cmd_pb2 import SkulkQuery, VectorQuery

app = Flask(__name__)

### Get parameters puts, home page
@app.route('/')
def home():
    return render_template('home.html')

### Get parameters inputs
@app.route('/process', methods=['POST'])
def process():
    # Input data
    dataset = request.form['dataset']
    columns = request.form.get('columns', '').split(',') if request.form.get('columns') else []
    predicates = request.form.get('predicates', None)
    limit = int(request.form['limit']) if request.form.get('limit') else None
    uuid = request.form.get('uuid', None)
    with_step_data = request.form['with_step_data'].lower() == 'true'

    # Input Vector Query
    vector_query_column = request.form.get('vector_query_column', None)
    vector_query_text = request.form.get('vector_query_text', None)
    vector_query_top_k = request.form.get('vector_query_top_k', None)

    ###########
    print("--------- Input Parameters ---------")
    print("vector_query_column:", vector_query_column)
    print("vector_query_text:", vector_query_text)
    print("vector_query_top_k:", vector_query_top_k)
    print(dataset)
    print(columns)
    print(predicates)
    print(limit)
    print(uuid)
    print("with_step_data:", with_step_data)
    ###########

    # vector_query = None
    # if vector_query_column and vector_query_text and vector_query_top_k:
    #     vector_query = VectorQuery(
    #         column=vector_query_column,
    #         text_query=vector_query_text,
    #         top_k=int(vector_query_top_k)
    #     )

    # # 构造 SkulkQuery 对象
    # skulk_query = SkulkQuery(
    #     dataset=dataset,
    #     columns=columns,
    #     predicates=predicates,
    #     vector_query=vector_query,
    #     limit=limit,
    #     uuid=uuid,
    #     with_step_data=with_step_data
    # )

    # # Use SkulkClient to query data
    # client = SkulkClient("localhost:5005")  # 替换为自己的 endpoint!!!
    # processed_data = client.get_dataset(skulk_query) # 这里processed_data应该是一个pandas df!!!

    # Store data as global variables
    global processed_data, mode
    processed_data = pd.DataFrame({
        '_episode_id': [1, 2, 3, 4, 5],  # _episode_id 列
        'value': [10, 20, 30, 40, 50]    # value 列
    }) # debug设置成empty df
    mode = "with-step" if with_step_data else "without-step"

    # check is or is not step data
    try:
        if mode == "with-step":
            # ensure data is step data
            if "_step_id" not in processed_data.columns:
                raise ValueError("The file does not contain 'step_id' for with-step mode.")
        elif mode == "without-step":
            # ensure data is without step data
            if "_step_id" in processed_data.columns:
                raise ValueError("The file mistakenly contain 'step_id' for without-step mode.")
        else:
            raise ValueError("Invalid mode selected.")
    except Exception as e:
        return f"Error processing file: {str(e)}"
    
    return redirect(url_for('display_data'))

### Display sample data, main page
@app.route('/index', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        return redirect(url_for('display_data'))
    return render_template('index.html')

### Display sample data
@app.route('/display_main_page', methods=['POST'])
def display_main_page():
    global mode
    # File path entered by user
    file_path = request.form['file_path']
    # User select mode, with step or without step
    mode = request.form['mode']

    try:
        # load data from pickle file
        data = pd.read_pickle(file_path)

        if mode == "with-step":
            # ensure data is step data
            if "_step_id" not in data.columns:
                raise ValueError("The file does not contain 'step_id' for with-step mode.")
        elif mode == "without-step":
            # ensure data is without step data
            if "_step_id" in data.columns:
                raise ValueError("The file mistakenly contain 'step_id' for without-step mode.")
        else:
            raise ValueError("Invalid mode selected.")

        # Set dataframe as global variable
        global processed_data
        processed_data = data

        # redirect to display data page
        return redirect(url_for('display_data'))

    except Exception as e:
        return f"Error processing file: {str(e)}"

@app.route('/display', methods=['GET', 'POST'])
def display_data():
    global processed_data, mode

    if 'processed_data' not in globals():
        return "No data to display."

    # Get unique episode_id
    episode_ids = processed_data['_episode_id'].unique()
    # Defaultly choose first episode_id
    selected_episode_id = request.form.get('episode_id', episode_ids[0])

    # Filters the currently selected Episode data
    episode_data = processed_data[processed_data['_episode_id'] == selected_episode_id].copy()

    # if "with-step"
    is_with_step = mode == "with-step"
    if is_with_step:
        # 移除 language_embedding 列, 未来可以删除
        if 'language_embedding' in episode_data.columns:
            episode_data = episode_data.drop(columns=['language_embedding'])

        # Convert observation to Base64 format
        if 'observation' in episode_data:
            episode_data['observation_image'] = episode_data['observation'].apply(
                lambda obs: f"data:image/png;base64,{base64.b64encode(obs).decode('utf-8')}"
            )
            episode_data = episode_data.drop(columns=['observation'])  # Remove original observation column

    # ** Ensure that all column data is serializable!!! **
    def convert_to_serializable(row):
        for col in row.index:
            if isinstance(row[col], np.ndarray):  # if type is ndarray
                row[col] = row[col].tolist()  # convert to list
        return row

    episode_data = episode_data.apply(convert_to_serializable, axis=1)

    # Convert to dictionary format is passed to the front end
    steps_data = episode_data.to_dict(orient='records')
    columns = episode_data.columns.tolist()

    return render_template(
        'display.html',
        episode_ids=episode_ids,
        selected_episode_id=selected_episode_id,
        columns=columns,
        steps_data=steps_data,
        is_with_step=is_with_step
    )

if __name__ == '__main__':
    app.run(debug=True)