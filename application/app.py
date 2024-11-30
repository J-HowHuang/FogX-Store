from flask import Flask, request, jsonify, render_template, redirect, url_for
import pandas as pd
import numpy as np
import base64
from io import BytesIO

# # Need import from SkulkClient
# from skulkclient.client import SkulkClient
# from skulkclient.predatorfox.cmd_pb2 import SkulkQuery, VectorQuery

app = Flask(__name__)

# ### Get parameters puts, home page
# @app.route('/')
# def home():
#     return render_template('home.html')

# ### Get parameters inputs
# @app.route('/process', methods=['POST'])
# def process():
#     # 获取表单数据
#     dataset = request.form['dataset']
#     columns = request.form.get('columns', '').split(',') if request.form.get('columns') else []
#     predicates = request.form.get('predicates', None)
#     limit = int(request.form['limit']) if request.form.get('limit') else None
#     uuid = request.form.get('uuid', None)
#     with_step_data = request.form['with_step_data'].lower() == 'true'

#     # 获取 Vector Query 数据
#     vector_query_column = request.form.get('vector_query_column', None)
#     vector_query_text = request.form.get('vector_query_text', None)
#     vector_query_top_k = request.form.get('vector_query_top_k', None)

#     ###########
#     # print("vector_query_column:", vector_query_column)
#     # print("vector_query_text:", vector_query_text)
#     # print("vector_query_top_k:", vector_query_top_k)
#     # print(dataset)
#     # print(columns)
#     # print(predicates)
#     # print(limit)
#     # print(uuid)
#     # print("with_step_data:", with_step_data)
#     ###########

#     # vector_query = None
#     # if vector_query_column and vector_query_text and vector_query_top_k:
#     #     vector_query = VectorQuery(
#     #         column=vector_query_column,
#     #         text_query=vector_query_text,
#     #         top_k=int(vector_query_top_k)
#     #     )

#     # # 构造 SkulkQuery 对象
#     # skulk_query = SkulkQuery(
#     #     dataset=dataset,
#     #     columns=columns,
#     #     predicates=predicates,
#     #     vector_query=vector_query,
#     #     limit=limit,
#     #     uuid=uuid,
#     #     with_step_data=with_step_data
#     # )

#     # # 使用 SkulkClient 获取数据
#     # client = SkulkClient("localhost:5005")  # 替换为自己的 endpoint
#     # processed_data = client.get_dataset(skulk_query) # 这里processed_data应该是一个pandas df

#     # 将数据存储为全局变量
#     global processed_data, mode
#     processed_data = pd.DataFrame({
#         '_episode_id': [1, 2, 3, 4, 5],  # _episode_id 列
#         'value': [10, 20, 30, 40, 50]    # value 列
#     }) # debug设置成empty df
#     mode = "with-step" if with_step_data else "without-step"

#     return redirect(url_for('display_data'))

### Display sample data, main page
@app.route('/')
def index():
    return render_template('index.html')

### Display sample data
@app.route('/process', methods=['POST'])
def process():
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

    # 获取所有唯一的 episode_id
    episode_ids = processed_data['_episode_id'].unique()
    # 默认选择第一个 episode_id
    selected_episode_id = request.form.get('episode_id', episode_ids[0])

    # 筛选当前选择的 Episode 数据
    episode_data = processed_data[processed_data['_episode_id'] == selected_episode_id].copy()

    # 如果是 with-step 数据
    is_with_step = mode == "with-step"
    if is_with_step:
        # 移除 language_embedding 列
        if 'language_embedding' in episode_data.columns:
            episode_data = episode_data.drop(columns=['language_embedding'])

        # 将 observation 转换为 Base64 格式
        if 'observation' in episode_data:
            episode_data['observation_image'] = episode_data['observation'].apply(
                lambda obs: f"data:image/png;base64,{base64.b64encode(obs).decode('utf-8')}"
            )
            episode_data = episode_data.drop(columns=['observation'])  # 移除原始 observation 列

    # **确保所有列数据可序列化**
    def convert_to_serializable(row):
        for col in row.index:
            if isinstance(row[col], np.ndarray):  # 如果是 ndarray 类型
                row[col] = row[col].tolist()  # 转换为 list
        return row

    episode_data = episode_data.apply(convert_to_serializable, axis=1)

    # 转换为字典格式传递到前端
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
    app.run(debug=False)