from flask import Flask, request, jsonify, render_template, redirect, url_for
import pandas as pd
import numpy as np
import base64
from io import BytesIO

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

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
    app.run(debug=True)
