from dotenv import load_dotenv

# 加载 .env 文件中的环境变量
load_dotenv()
from flask import Flask, request, jsonify, stream_with_context, Response, send_file
from flask_cors import CORS,cross_origin
#from ai.backend.chat_task import ChatClass
import time  # 用于模拟延迟
import json
import asyncio
import threading
import os
import pandas as pd
import tempfile


app = Flask(__name__)
# CORS(app)

# cors = CORS(app, resources={r"/api/*": {"origins": "*"}})

class MockWebSocket:
    def __init__(self):
        self.messages = []
        self.chat_id = 0
        pass

    def set_chat_id(self, chat_id):
        self.chat_id = chat_id

    async def send(self, message):
        message = json.loads(message)
        message['chat_id'] = self.chat_id
        message = json.dumps(message)
        message_with_delimiter = message + "---ENDOFMESSAGE---"
        self.messages.append(message_with_delimiter)

def generate_stream(mock_socket, user_name, user_id, message,chat_id):
    try:
        def background_task():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            master = ChatClass(mock_socket, user_name, user_id, message, chat_id)
            loop.run_until_complete(master.consume())
            loop.close()

        thread = threading.Thread(target=background_task)
        thread.start()

        while thread.is_alive() or mock_socket.messages:
            if mock_socket.messages:
                message = mock_socket.messages.pop(0)
                print('send message: ', message)
                yield f"{message}\n\n"
            else:
                time.sleep(1)

    except GeneratorExit:
        print("Client connection closed, stopping background task.")

        if thread and thread.is_alive():
            thread.join(timeout=1)


    except Exception as e:
        print("error: ", e)
        return "error"

@app.route("/api/chat", methods=["POST"])
@cross_origin()
def chat():
    data = request.get_json()
    print("data: ", data)
    user_id = data['user_id']
    user_name = data['user_name']
    message = data['message']
    chat_id = data['chat_id']
    print("user_id: ", user_id)
    print("user_name: ", user_name)
    print("message: ", message)
    print("chat_id: ", chat_id)
    mock_socket = MockWebSocket()
    mock_socket.set_chat_id(chat_id)

    # return Response(stream_with_context(asyncio.run(demo1(mock_socket))), mimetype='text/event-stream')
    return Response(stream_with_context(generate_stream(mock_socket, user_name, user_id, message,chat_id)), mimetype='text/event-stream')
    # s = ChatClass(mock_socket, user_name, user_id, message,chat_id)
    # return Response(stream(content), mimetype='text/plain')



@app.route("/api/readRag", methods=["GET"])
@cross_origin()
def readRag():
    try:
        # 获取 userid 和数据库 id 参数
        user_id = request.args.get('user_id')
        db_id = request.args.get('db_id')
        if not user_id or not db_id:
            return jsonify({"error": "Missing parameters."}), 400

        # 根据命名规范构建文件路径
        filename = f'.rag_{user_id}_db{db_id}.json'
        json_file = f'/opt/DeepBI/user_upload_files/{filename}'

        # 读取 JSON 文件
        with open(json_file, 'r',encoding='utf-8') as file:
            data = json.load(file)
        return jsonify(data)

    except FileNotFoundError:
        return jsonify({"error": "File not found."}), 404
    except json.JSONDecodeError:
        return jsonify({"error": "Invalid JSON format."}), 500


@app.route('/api/updateRag', methods=['POST'])
def updateRag():
    try:
        # 获取传递的数据参数
        user_id = request.args.get('user_id')
        db_id = request.args.get('db_id')
        data_key = request.args.get('data_key')
        data_value = request.args.get('data_value')

        # 检查参数是否存在
        if not user_id or not db_id or not data_key or not data_value:
            return jsonify({"error": "Missing parameters."}), 400

        # 根据命名规范构建文件路径
        filename = f'.rag_{user_id}_db{db_id}.json'
        json_file = f'/opt/DeepBI/user_upload_files/{filename}'

        # 如果文件不存在，则返回错误
        if not os.path.exists(json_file):
            return jsonify({"error": "File not found."}), 404

        # 如果文件已存在，则读取文件内容
        with open(json_file, 'r',encoding='utf-8') as file:
            file_data = json.load(file)

            # 检查 data 是否已存在于文件中
            if data_key in file_data and file_data[data_key] == data_value:
                return jsonify({"message": "Data already exists."}), 200
            else:
                file_data[data_key] = data_value

            # 更新文件内容
            with open(json_file, 'w',encoding='utf-8') as file:
                json.dump(file_data, file)

            return jsonify({"message": "File updated."}), 200
    except Exception as e:
            return jsonify({"error": str(e)}), 500


UPLOAD_FOLDER = '/opt/DeepBI_rag/temp'
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)


@app.route('/api/store_comment_as_csv', methods=['POST'])
@cross_origin()
def store_comment_as_csv():
    schema = request.get_json()
    if schema:
        try:
            # 创建一个空列表来存储所有表的信息
            all_tables_data = []

            for table in schema:
                table_name = table['name']
                table_comment = table.get('table_comment', '')  # 获取表的注释
                for column, comment in zip(table['columns'], table.get('comment', [])):
                    all_tables_data.append([table_name, table_comment, column, comment])

            # 将列表转换为DataFrame
            all_tables_df = pd.DataFrame(all_tables_data, columns=['table', 'table_comment', 'column', 'comment'])

            # 将DataFrame转换为CSV数据
            csv_data = all_tables_df.to_csv(index=False)

            # 创建一个临时文件
            with tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.csv',dir=UPLOAD_FOLDER) as temp_file:
                temp_file.write(csv_data)
                temp_file_path = temp_file.name

            # 构建下载链接
            download_link = f"{request.url_root}api/download_csv?file={os.path.basename(temp_file_path)}"

            # 返回下载链接
            return download_link, 200
        except Exception as e:
            return str(e), 500
    else:
        return "No schema provided in request.", 400

@app.route('/api/download_csv', methods=['GET'])
def download_csv():
    filename = request.args.get('file')
    new_filename = request.args.get('new_filename')  # 新的文件名参数

    if filename:
        # 构建文件路径
        file_path = os.path.join(UPLOAD_FOLDER, filename)
        # 检查文件是否存在
        if os.path.exists(file_path):
            # 如果新文件名未提供，则使用原始文件名
            if new_filename:
                download_filename = new_filename
            else:
                download_filename = filename

            # 返回文件作为响应，并指定下载时的文件名
            return send_file(file_path, as_attachment=True, attachment_filename=download_filename)
        else:
            return jsonify({"error": "File not found."}), 404
    else:
        return jsonify({"error": "Filename not provided."}), 400


uploaded_df = None
@app.route('/api/upload_csv', methods=['POST'])
def upload_csv():
    global uploaded_df

    # 检查文件是否被上传
    if 'file' not in request.files:
        return "No file provided.", 400

    uploaded_file = request.files['file']

    # 检查文件格式是否正确
    try:
        uploaded_df = pd.read_csv(uploaded_file)

        # 检查文件是否有四列
        if len(uploaded_df.columns) != 4:
            return "CSV file must have exactly 4 columns (table, table_comment, column, comment).", 400

        # 检查第一行的列名是否符合预期
        expected_columns = ['table', 'table_comment', 'column', 'comment']
        if uploaded_df.columns.tolist() != expected_columns:
            return "The first row of CSV file must have column names 'table', 'table_comment', 'column', 'comment'.", 400

        # 检查第一列和第三列不能为空
        if uploaded_df['table'].isnull().values.any() or uploaded_df['column'].isnull().values.any():
            return "The 'table' and 'column' columns cannot be empty.", 400

        # 返回成功消息
        return "CSV file uploaded successfully.", 200
    except Exception as e:
        return str(e), 500


@app.route('/api/update_comments', methods=['POST'])
def update_comments():
    global uploaded_df

    schema = request.get_json()  # 获取新接收的 schema


    if uploaded_df is not None and schema:
        try:
            # 匹配 table 和 column
            for index, row in uploaded_df.iterrows():
                print("Row index:", index)
                table_name = row['table']
                table_comment = row['table_comment']
                column_name = row['column']
                comment = row['comment']

                for table in schema:
                    if table['name'] == table_name:
                        table['table_comment'] = table_comment
                        for i, col in enumerate(table['columns']):
                            if col == column_name:  # 正确比较列名
                                table['comment'][i] = comment  # 通过索引更新comment列表中的元素
                                break
            # 返回更新后的 schema
            return jsonify(schema), 200
        except Exception as e:
            return str(e), 500
    else:
        return "File or schema not provided.", 400

if __name__ == '__main__':
    app.run(port=8347, host='0.0.0.0', debug=True)
    # app.run(port=8341, host='0.0.0.0', debug=True, threaded=False) # 尝试设置 主线程中调用API服务

