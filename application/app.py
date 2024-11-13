from flask import Flask, request, jsonify, render_template
app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/process', methods=['POST'])
def process():
    data = request.get_json()
    query = data.get('query')
    model = data.get('model')
    
    # 模拟数据库查询结果
    if model == "phi":
        result = f"Phi model result for query: {query}"
    elif model == "qwen":
        result = f"Qwen model result for query: {query}"
    else:
        result = "Unknown model selected"

    return jsonify({'result': result})

if __name__ == '__main__':
    app.run(debug=True)
