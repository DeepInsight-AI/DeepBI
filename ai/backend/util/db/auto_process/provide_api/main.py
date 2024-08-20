from flask import Flask, request, jsonify
import hashlib
import time
from ai.backend.util.db.auto_process.provide_api.util.update_api import update_api

app = Flask(__name__)

# 验证函数
def verify_request(token, timestamp, secret_key):
    # 计算token
    calculated_token = hashlib.sha256((secret_key + str(timestamp) + secret_key).encode('utf-8')).hexdigest()
    return token == calculated_token

@app.route('/api/data/create', methods=['POST'])
def handle_insert():
    # 获取请求头和请求体
    token = request.headers.get('token')
    timestamp = request.headers.get('timestamp')
    data = request.get_json()

    # 验证请求头
    secret_key = "10470c3b4b1fed12c3baac014be15fac67c6e815"  # 测试环境的秘钥, 根据环境配置选择秘钥
    if not verify_request(token, timestamp, secret_key):
        return jsonify({"error": "Unauthorized"}), 401

    # 处理插入数据的逻辑
    # 在此处添加处理插入数据的逻辑
    return jsonify({"message": "Create data received"}), 201

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
