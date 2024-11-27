from flask import Flask, request, jsonify, render_template, redirect, url_for
import pandas as pd
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
    global processed_data, mode  # 使用全局变量 processed_data 和 mode

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
        # 移除 language_embedding 列（用于可视化）
        if 'language_embedding' in episode_data.columns:
            episode_data = episode_data.drop(columns=['language_embedding'])

        # 将 observation 转换为 Base64 并存储到新的列中
        episode_data['observation_image'] = episode_data['observation'].apply(
            lambda obs: f"data:image/png;base64,{base64.b64encode(obs).decode('utf-8')}"
        )
        # 删除原始的 observation 列（不需要展示原始数据）
        episode_data = episode_data.drop(columns=['observation'])

    # 将表格数据转换为字典格式
    table_data = episode_data.to_dict(orient='records')  
    # 获取表头信息
    columns = episode_data.columns.tolist()

    # 渲染模板并传递必要的信息
    return render_template(
        'display.html',
        episode_ids=episode_ids,
        selected_episode_id=selected_episode_id,
        table_data=table_data,
        columns=columns,
        is_with_step=is_with_step
    )

if __name__ == '__main__':
    app.run(debug=True)
