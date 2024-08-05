import asyncio
import json
import threading
import time

from flask import Flask, render_template, request, jsonify, send_file
from datetime import datetime
from util.access_param import access_param
import pandas as pd
import os
from util.api_auto_sp import auto_api
from ai.backend.util.db.auto_process.create_new_sd_ad_auto import Ceate_new_sd
from ai.backend.util.db.auto_process.create_new_sp_ad_auto import Ceate_new_sku
from ai.backend.util.db.db_amazon.auto_generate_new_sku_sp import AmazonMysqlRagUitl as sp
from ai.backend.util.db.db_amazon.auto_generate_new_sku_sd import AmazonMysqlRagUitl as sd

app = Flask(__name__)
lock = threading.Lock()

# 首页路由，渲染HTML模板
@app.route('/')
def index():
    return render_template('index.html')

# Query page - Render the query form
@app.route('/query')
def query():
    return render_template('query_test_rossny.html')

# Create page - Render the create form
@app.route('/create')
def create():
    return render_template('create_test_rossny.html')

@app.route('/modify')
def modify():
    return render_template('modify_test_rossny.html')


# 处理 create.html 表单提交的路由
@app.route('/process_create', methods=['POST'])
def process_create():
    with lock:
        try:
            params_create = {
                'brand': request.form.get('brand'),
                'country': request.form.get('country'),
                'create_method': request.form.get('create_method'),
                'strategy': request.form.get('strategy'),
            }
            api1 = Ceate_new_sd()
            api2 = Ceate_new_sku()
            api3 = sp(params_create['brand'])
            api4 = sd(params_create['brand'])
            if params_create['create_method'] == '新建':
                additional_params_create = {
                    'product_info': request.form.get('product_info', '')

                }
                params_create.update(additional_params_create)
                info = product_info_list = [item.strip(" '") for item in params_create['product_info'].strip('[]').split(', ')]
                if params_create['strategy'] == "0509":
                    if params_create['brand'] == 'LAPASA':
                        api1.create_new_sd_no_template2(params_create['country'], info, params_create['brand'])
                    else:
                        api1.create_new_sd_no_template(params_create['country'], info, params_create['brand'])
                elif params_create['strategy'] == "0514":
                    if params_create['brand'] == 'LAPASA':
                        api2.create_new_sp_asin_no_template_2(params_create['country'], info, params_create['brand'])
                    else:
                        api2.create_new_sp_asin_no_template(params_create['country'], info, params_create['brand'])
                elif params_create['strategy'] == "0502_auto":
                    if params_create['brand'] == 'LAPASA':
                        api2.create_new_sp_auto_no_template1(params_create['country'], info, params_create['brand'])
                    else:
                        api2.create_new_sp_auto_no_template(params_create['country'], info, params_create['brand'])
                elif params_create['strategy'] == "0502_manual":
                    api2.create_new_sp_manual_no_template(params_create['country'], info, params_create['brand'])
                return jsonify({"status": "success", "message": "处理完成，未生成 CSV 文件。"})
            elif params_create['create_method'] == '横向复刻':
                additional_params_create = {
                    'template_country': request.form.get('template_country', ''),
                    'startdate': request.form.get('startdate', ''),
                    'enddate': request.form.get('enddate', '')
                }
                params_create.update(additional_params_create)
                if params_create['strategy'] == "0502":
                    csv_filename = api3.get_new_sp_top70_sku(params_create['country'],params_create['template_country'],params_create['startdate'],params_create['enddate'])
                elif params_create['strategy'] == "0507":
                    csv_filename = api4.get_new_sd_top90_sku(params_create['country'],params_create['template_country'],params_create['startdate'],params_create['enddate'])
                csv_filepath_create = os.path.join(app.root_path, csv_filename)
                return send_file(csv_filepath_create, as_attachment=True)
        except Exception as e:
            print(e)
            return jsonify({"status": "error", "message": str(e)}), 500



params = None
csv_filepath = None


# 处理表单提交的路由
@app.route('/run_function', methods=['POST'])
def run_function():
    today = datetime.today()
    cur_time = today.strftime('%Y-%m-%d')
    # 从表单中获取参数
    with lock:
        global params
        params = {
            'country': request.form.get('country', ''),  # 使用get方法提供默认值以防参数不存在
            'brand': request.form.get('brand'),
            'ad_type': request.form.get('ad_type', ''),
            'ad_options': request.form.get('ad_options', ''),
            'cur_time': cur_time,
            'filter_type': request.form.get('filter_type', ''),
        }

    # 检查是否选择了需要额外参数的广告类型和选项
        if params['filter_type'] == '广告活动筛选':
            # 使用get方法获取额外的筛选参数，并提供默认值
            additional_params = {
                'price_adjustment_operation': request.form.get('price_adjustment_operation', ''),
                'price_adjustment': request.form.get('price_adjustment', None),  # 可能不存在，使用None作为默认值
                'order_count_operation': request.form.get('order_count_operation', ''),
                'order_count': request.form.get('order_count', None),
                'click_count_operation': request.form.get('click_count_operation', ''),
                'click_count': request.form.get('click_count', None),
                'period': request.form.get('period', ''),
                'impression_operation': request.form.get('impression_operation', ''),
                'impression_count': request.form.get('impression_count', None),
                'query_field_operation': request.form.get('query_field_operation', ''),
                'query_field_value': request.form.get('query_field_value', None)
            }
            # 将额外参数添加到params字典中
            params.update(additional_params)
        print(params)
        csv_filename = access_param(params, params['brand'])
    # 在这里可以返回响应给前端，例如确认信息或处理结果
        global csv_filepath
        csv_filepath = os.path.join(os.getcwd(), csv_filename)
    return send_file(csv_filepath, as_attachment=True, attachment_filename=csv_filename)

@app.route('/query_function', methods=['POST'])
def query_function():
    global csv_filepath
    with lock:
    # 如果 csv_filepath 为空，表示还没有执行 run_function，可以根据需要处理
        if csv_filepath is None:
            print('CSV file not generated yet.')
            return jsonify({"status": "error", "message": "CSV file not generated yet."}), 400

        # 在这里可以根据 csv_filepath 执行你的修改操作
        try:
            # 示例：读取 CSV 文件内容
            df = pd.read_csv(csv_filepath)
            df_data = json.loads(df.to_json(orient='records'))
            api1 = auto_api('LAPASA')

            # 示例：模拟耗时操作
            if params['filter_type'] == '广告活动筛选':
                if params['ad_options'] == '预算':
                    print('执行修改操作...')
                    for item in df_data:
                        campaignId = item["campaignId"]
                        api1.auto_campaign_budget(params['country'], campaignId, params['price_adjustment_operation'], params['price_adjustment'])
                elif params['ad_options'] == '广告位':
                    print('执行修改操作...')
                    for item in df_data:
                        campaignId = item["campaignId"]
                        api1.auto_campaign_targeting_group(params['country'], campaignId, params['price_adjustment_operation'], params['price_adjustment'])
                elif params['ad_type'] == 'SP-手动' and params['ad_options'] == '关键词':
                    print('执行修改操作...')
                    for item in df_data:
                        campaignId = item["campaignId"]
                        api1.auto_campaign_keyword(params['country'], campaignId, params['price_adjustment_operation'], params['price_adjustment'])
                elif params['ad_type'] == 'SP-ASIN' and params['ad_options'] == '商品投放':
                    print('执行修改操作...')
                    for item in df_data:
                        campaignId = item["campaignId"]
                        api1.auto_campaign_product_targets(params['country'], campaignId, params['price_adjustment_operation'], params['price_adjustment'])
                elif params['ad_type'] == 'SP-自动' and params['ad_options'] == '自动定位组':
                    print('执行修改操作...')
                    for item in df_data:
                        campaignId = item["campaignId"]
                        api1.auto_campaign_automatic_targeting(params['country'], campaignId, params['price_adjustment_operation'], params['price_adjustment'])
        #time.sleep(10)

            return jsonify({"status": "success", "message": "Modification completed."})

        except Exception as e:
            print(e)
            return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/modify_function', methods=['POST'])
def modify_function():
    with lock:
        try:
            # Get CSV file
            csv_file = request.files['csvFile']
            # Save CSV file to a designated folder (optional)
            uploads_dir = os.path.join(app.root_path, 'uploads')  # 'uploads' folder relative to app.py
            if not os.path.exists(uploads_dir):
                os.makedirs(uploads_dir)
            csv_path = os.path.join(uploads_dir, csv_file.filename)
            csv_file.save(csv_path)

            params_modify = {
                'brand': request.form.get('brand'),
                'country': request.form.get('country'),
                'templateCountry': request.form.get('templateCountry'),
                'strategy': request.form.get('strategy'),
            }
            api1 = Ceate_new_sd()
            api2 = Ceate_new_sku()
            if params_modify['strategy'] == "0502":
                def run_async_operation():
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    try:
                        loop.run_until_complete(api2.create_new_sku(params_modify['country'], params_modify['templateCountry'], params_modify['brand'], csv_path))
                    finally:
                        loop.close()

                thread = threading.Thread(target=run_async_operation)
                thread.start()
                thread.join()  # 等待子线程完成
            elif params_modify['strategy'] == "0507":
                api1.create_new_sd_template(params_modify['country'], params_modify['templateCountry'], params_modify['brand'], csv_path)

            # Return a response if needed
            return f'创建完成'
        except Exception as e:
            print(e)
            return jsonify({"status": "error", "message": str(e)}), 500


if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0',port=4006, threaded=True)

