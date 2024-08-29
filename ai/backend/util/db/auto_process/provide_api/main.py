from flask import Flask, request, jsonify, g
import logging
from logging.handlers import RotatingFileHandler
import hashlib
import time
from ai.backend.util.db.auto_process.provide_api.util.update_api import update_api
from ai.backend.util.db.auto_process.provide_api.util.create_api import create_api

app = Flask(__name__)
# 设置日志记录器
handler = RotatingFileHandler('app.log', maxBytes=10000, backupCount=1)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
app.logger.addHandler(handler)


# 验证函数
def verify_request(token, timestamp, secret_key):
    # 计算token
    calculated_token = hashlib.sha256((secret_key + str(timestamp) + secret_key).encode('utf-8')).hexdigest()
    return token == calculated_token


def validate_id(data):
    """检查数据中的ID是否有效"""
    if not data or 'ID' not in data or not data['ID']:
        return False
    return True


@app.before_request
def before_request():
    # 记录请求开始时间
    g.start_time = time.time()
    # 记录请求的基本信息
    g.request_data = {
        'method': request.method,
        'url': request.url,
        'headers': dict(request.headers),
        'data': request.get_data(as_text=True)
    }
    app.logger.info(f"Request started: {g.request_data}")


@app.after_request
def after_request(response):
    # 计算请求处理时间
    elapsed_time = time.time() - g.start_time
    # 记录响应的基本信息
    log_data = {
        'method': g.request_data['method'],
        'url': g.request_data['url'],
        'status': response.status,
        'elapsed_time': elapsed_time,
        'headers': g.request_data['headers'],
        'data': g.request_data['data']
    }
    app.logger.info(f"Request finished: {log_data}")
    return response


@app.route('/api/data/create', methods=['POST'])
def handle_insert():
    # 获取请求头和请求体
    token = request.headers.get('token')
    timestamp = request.headers.get('timestamp')
    data = request.get_json()
    print(data)
    # 验证请求头
    secret_key = "10470c3b4b1fed12c3baac014be15fac67c6e815"  # 测试环境的秘钥, 根据环境配置选择秘钥
    if not verify_request(token, timestamp, secret_key):
        return jsonify({"error": "Unauthorized"}), 401
    if not validate_id(data):
        return jsonify({"error": "Invalid or missing ID"}), 400
    code = create_api(data)
    if code == 200:
        return jsonify({"status": 200, "error": ""})
    elif code == 404:
        return jsonify({"status": 404, "error": "Resource not found"})
    elif code == 500:
        return jsonify({"status": 500, "error": "Internal Server Error"})
    else:
        return jsonify({"status": 404, "error": "Unknown error"})  # Bad Request


@app.route('/api/data/update', methods=['POST'])
def handle_update():
    # 获取请求头和请求体
    token = request.headers.get('token')
    timestamp = request.headers.get('timestamp')
    data = request.get_json()

    # 验证请求头
    secret_key = "10470c3b4b1fed12c3baac014be15fac67c6e815"  # 测试环境的秘钥, 根据环境配置选择秘钥
    if not verify_request(token, timestamp, secret_key):
        return jsonify({"error": "Unauthorized"}), 401
    if not validate_id(data):
        return jsonify({"error": "Invalid or missing ID"}), 400
    # 调用 update_api 并处理返回值
    code = update_api(data)
    if code == 200:
        return jsonify({"status":200,"error":""})
    elif code == 404:
        return jsonify({"status":404,"error": "Resource not found"})
    elif code == 500:
        return jsonify({"status":500,"error": "Internal Server Error"})
    else:
        return jsonify({"status":404,"error": "Unknown error"})  # Bad Request


@app.route('/api/data/delete', methods=['POST'])
def handle_delete():
    # 获取请求头和请求体
    token = request.headers.get('token')
    timestamp = request.headers.get('timestamp')
    data = request.get_json()

    # 验证请求头
    secret_key = "10470c3b4b1fed12c3baac014be15fac67c6e815"  # 测试环境的秘钥, 根据环境配置选择秘钥
    if not verify_request(token, timestamp, secret_key):
        return jsonify({"error": "Unauthorized"}), 401

    # 处理删除数据的逻辑
    # 在此处添加处理删除数据的逻辑
    return jsonify({"message": "Delete data received"}), 200

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5009)
