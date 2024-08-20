import asyncio
import json
import threading
import time

from flask import Flask, request, session, render_template, redirect, flash, jsonify, send_file
from datetime import datetime
from util.access_param import access_param,access_param_self
import pandas as pd
import os
from ai.backend.util.db.auto_process.amazon_api_demo.util.api_auto_sp import auto_api_sp
from ai.backend.util.db.auto_process.amazon_api_demo.util.api_auto_sd import auto_api_sd
from ai.backend.util.db.auto_process.create_new_sd_ad_auto import Ceate_new_sd
from ai.backend.util.db.auto_process.create_new_sp_ad_auto import Ceate_new_sku
from ai.backend.util.db.db_amazon.auto_generate_new_sku_sp import AmazonMysqlRagUitl as sp
from ai.backend.util.db.db_amazon.auto_generate_new_sku_sd import AmazonMysqlRagUitl as sd
from ai.backend.util.db.auto_process.update_sp_ad_auto_new import auto_api
from ai.backend.util.db.auto_process.update_sd_ad_auto import auto_api_sd as auto_api_sd2
from ai.backend.util.db.db_amazon.sales_with_no_ad_spend_sku import SalesWithNoAdSpendSku
from ai.backend.util.db.db_amazon.sku_and_country_linked_four_ads_creation import SkuAndCountryLinkedFourAdsCreation
from ai.backend.util.db.db_amazon.SD_0511_Recommended_Product_Ad_Strategy import SdRecommendedProductAdStrategy
from ai.backend.util.db.db_amazon.SD_ASIN_Derivative_Strategy_Query import SdAsinDerivativeStrategyQuery
from ai.backend.util.db.db_amazon.SD_0808_Strategy import Sd0808Strategy


app = Flask(__name__)
app.config['SECRET_KEY'] = '3c2d9d261a464e4e8814c5a39aa72f1c'
lock = threading.Lock()

users = {
    'admin': 'A123123a',
    'LAPASA': 'LAPASA',
    'OutdoorMaster': 'OutdoorMaster',
    'DELOMO': 'DELOMO',
    'MUDEELA': 'MUDEELA',
    'Rossny': 'Rossny',
    'ZEN CAVE': 'ZEN CAVE',
    'Veement': 'Veement',
    'KAPEYDESI': 'KAPEYDESI',
    'Gvyugke': 'Gvyugke',
    'Uuoeebb': 'Uuoeebb',
    'syndesmos': 'syndesmos',
    'Gonbouyoku': 'Gonbouyoku',
    'gmrpwnage': 'gmrpwnage',
    'suihuooo': 'suihuooo'
}
user_permissions = {
    'admin': {
        'brands': {
            'LAPASA': ['IT', 'ES', 'DE', 'FR', 'UK', 'US', 'JP'],
            'DELOMO': ['IT', 'ES', 'DE', 'FR'],
            'OutdoorMaster': ['IT', 'ES', 'FR', 'SE'],
            'MUDEELA': ['US'],
            'Rossny': ['US'],
            'ZEN CAVE': ['US'],
            'Veement': ['UK'],
            'KAPEYDESI': ['UK', 'DE', 'FR', 'SA'],
            'Gvyugke': ['UK', 'DE', 'FR', 'IT', 'AU'],
            'Uuoeebb': ['US'],
            'syndesmos': ['DE', 'IT'],
            'Gonbouyoku': ['JP'],
            'gmrpwnage': ['JP'],
            'suihuooo': ['US']
        }
    },
    'LAPASA': {
        'brands': {
            'LAPASA': ['IT', 'ES', 'DE', 'FR', 'UK', 'US', 'JP']
        }
    },
    'DELOMO': {
        'brands': {
            'DELOMO': ['IT', 'ES', 'DE', 'FR']
        }
    },
    'OutdoorMaster': {
        'brands': {
            'OutdoorMaster': ['IT', 'ES', 'FR', 'SE']
        }
    },
    'MUDEELA': {
        'brands': {
            'MUDEELA': ['US']
        }
    },
    'Rossny': {
        'brands': {
            'Rossny': ['US']
        }
    },
    'ZEN CAVE': {
        'brands': {
            'ZEN CAVE': ['US']
        }
    },
    'Veement': {
        'brands': {
            'Veement': ['UK']
        }
    },
    'KAPEYDESI': {
        'brands': {
            'KAPEYDESI': ['UK', 'DE', 'FR', 'SA']
        }
    },
    'Gvyugke': {
        'brands': {
            'Gvyugke': ['UK', 'DE', 'FR', 'IT', 'AU']
        }
    },
    'Uuoeebb': {
        'brands': {
            'Uuoeebb': ['US']
        }
    },
    'syndesmos': {
        'brands': {
            'syndesmos': ['DE', 'IT']
        }
    },
    'Gonbouyoku': {
        'brands': {
            'Gonbouyoku': ['JP']
        }
    },
    'gmrpwnage': {
        'brands': {
            'gmrpwnage': ['JP']
        }
    },
    'suihuooo': {
        'brands': {
            'suihuooo': ['US']
        }
    }
}

# 首页路由，渲染HTML模板
@app.route('/')
def index():
    if request.method == 'GET':
        # 判断是否已经在登录状态上:判断session中是否有uname的值
        if 'uname' in session:
            # 已经登录了，直接去往首页
            return render_template('index_test.html')
        else:
            # 没有登录，继续向下判断cookie
            if 'uname' in request.cookies:
                # 曾经记住过密码,取出值保存进session
                uname = request.cookies.get('uname')
                session['uname'] = uname
                return render_template('index_test.html')
            else:
                # 　之前没有登录过,去往登录页
                return redirect('/login')

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'GET':
        # 判断是否已经在登录状态上:判断session中是否有uname的值
        if 'uname' in session:
            # 已经登录了，直接去往首页
            return redirect('/')
        else:
            # 没有登录，继续向下判断cookie
            if 'uname' in request.cookies:
                # 曾经记住过密码,取出值保存进session
                uname = request.cookies.get('uname')
                session['uname'] = uname
                return redirect('/')
            else:
                # 　之前没有登录过,去往登录页
                return render_template('ll.html')
    else:
        # 先处理登录,登录成功继续则保存进session,否则回到登录页
        uname = request.form.get('uname')
        upwd = request.form.get('upwd')
        # 本文默认正确的账号密码为:admin
        if uname in users and users[uname] == upwd:
            # 声明重定向到首页的对象
            resp = redirect('/')
            # 登录成功：先将数据保存进session
            session['uname'] = uname
            # 判断是否要记住密码
            if 'isSaved' in request.form:
                # 需要记住密码，将信息保存进cookie
                resp.set_cookie('uname', uname, 60 * 60 * 24 * 30)
            return resp
        else:
            # 登录失败
            flash("用户名或者密码不正确")
            return redirect('/login')

@app.route('/logout', methods=['GET', 'POST'])
def logout():
    resp = redirect('/')
    resp.delete_cookie('uname')
    session.pop('uname', None)
    return resp

# Query page - Render the query form
@app.route('/query')
def query():
    if 'uname' not in session:
        return redirect('/login')

    username = session['uname']
    permissions = user_permissions.get(username, {'brands': {}})

    return render_template('query_test2.html', permissions=permissions)

# Query page - Render the query form
@app.route('/strategy')
def strategy():
    if 'uname' not in session:
        return redirect('/login')

    username = session['uname']
    permissions = user_permissions.get(username, {'brands': {}})

    return render_template('strategy_test2.html', permissions=permissions)

# Create page - Render the create form
@app.route('/create')
def create():
    if 'uname' not in session:
        return redirect('/login')

    username = session['uname']
    permissions = user_permissions.get(username, {'brands': {}})

    return render_template('create_test2.html', permissions=permissions)


@app.route('/modify')
def modify():
    if 'uname' not in session:
        return redirect('/login')

    username = session['uname']
    permissions = user_permissions.get(username, {'brands': {}})

    return render_template('modify_test2.html', permissions=permissions)


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
            api3 = sp(params_create['brand'], params_create['country'])
            api4 = sd(params_create['brand'], params_create['country'])
            if params_create['create_method'] == '新建':
                additional_params_create = {
                    'product_info': request.form.get('product_info', ''),
                    'budget': request.form.get('budget', ''),
                    'bid': request.form.get('bid', ''),
                }
                params_create.update(additional_params_create)
                info = product_info_list = [item.strip(" '") for item in params_create['product_info'].strip('[]').split(', ')]
                if params_create['strategy'] == "0509":
                    api1.create_new_sd_no_template(params_create['country'], info, params_create['brand'], params_create['budget'], params_create['bid'])
                elif params_create['strategy'] == "0514":
                    api2.create_new_sp_asin_no_template(params_create['country'], info, params_create['brand'], params_create['budget'], params_create['bid'])
                elif params_create['strategy'] == "0502_auto":
                    if params_create['brand'] == "Veement" or params_create['brand'] == "KAPEYDESI" or params_create['brand'] == "Gvyugke" or params_create['brand'] == "Uuoeebb" or params_create['brand'] == "syndesmos" or params_create['brand'] == "Gonbouyoku" or params_create['brand'] == "gmrpwnage":
                        api2.create_new_sp_auto_no_template_jiutong(params_create['country'], info, params_create['brand'], params_create['budget'])
                    else:
                        api2.create_new_sp_auto_no_template(params_create['country'], info, params_create['brand'], params_create['budget'], params_create['bid'])
                elif params_create['strategy'] == "0502_manual":
                    api2.create_new_sp_manual_no_template(params_create['country'], info, params_create['brand'], params_create['budget'])
                elif params_create['strategy'] == "0511":
                    api1.create_new_sd_0511(params_create['country'], info, params_create['brand'], params_create['budget'], params_create['bid'])
                elif params_create['strategy'] == "0731":
                    api1.create_new_sd_no_template_0731(params_create['country'], info, params_create['brand'], params_create['budget'], params_create['bid'])
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


@app.route('/strategy_function', methods=['POST'])
def strategy_function():
    with lock:
        if request.method == 'POST':
            brand = request.form['brand']
            country = request.form['country']
            strategy_type = request.form['strategy_type']

            # 根据策略类型处理不同的参数
            if strategy_type == 'type1':
                date = request.form['date']
                csv_filepath = SalesWithNoAdSpendSku(brand,country).get_sales_with_no_ad_spend_sku(country,date)
            elif strategy_type == 'type7':
                date = request.form['date']
                csv_filepath = SdAsinDerivativeStrategyQuery(brand,country).get_SD_ASIN_Derivative_Strategy_Query(country, date)
            elif strategy_type == 'type6':
                start_date = request.form['start_date']
                end_date = request.form['end_date']
                csv_filepath = SdRecommendedProductAdStrategy(brand,country).get_0511_sd_recommended_product_ad(country, start_date, end_date)
            elif strategy_type == 'type2':
                sku = request.form['sku']
                sku_info = [item.strip(" '") for item in sku.strip('[]').split(', ')]
                csv_filepath = SkuAndCountryLinkedFourAdsCreation(brand,country).get_sku_and_country_sp_manual_creation(sku_info)
            elif strategy_type == 'type3':
                sku = request.form['sku']
                sku_info = [item.strip(" '") for item in sku.strip('[]').split(', ')]
                csv_filepath = SkuAndCountryLinkedFourAdsCreation(brand,country).get_sku_and_country_sp_asin_creation(sku_info)
            elif strategy_type == 'type4':
                sku = request.form['sku']
                sku_info = [item.strip(" '") for item in sku.strip('[]').split(', ')]
                csv_filepath = SkuAndCountryLinkedFourAdsCreation(brand,country).get_sku_and_country_sp_auto_creation(sku_info)
            elif strategy_type == 'type5':
                sku = request.form['sku']
                sku_info = [item.strip(" '") for item in sku.strip('[]').split(', ')]
                csv_filepath = SkuAndCountryLinkedFourAdsCreation(brand,country).get_sku_and_country_sd_creation(sku_info)
            elif strategy_type == 'type8':
                product = request.form['product']
                product_type = request.form['product_type']
                product_info = [item.strip(" '") for item in product.strip('[]').split(', ')]
                if product_type == 'asin':
                    csv_filepath = Sd0808Strategy(brand,country).get_0808_sd_ad(country,product_info)
                elif product_type == 'parent_asin':
                    csv_filepath = Sd0808Strategy(brand, country).get_0808_sd_ad(country, product_info,1)
                elif product_type == 'sspu':
                    csv_filepath = Sd0808Strategy(brand, country).get_0808_sd_ad(country, product_info,2)
            else:
                return 'Invalid strategy type'
    #     # 将额外参数添加到params字典中
    #     params.update(additional_params)
    #     print(params)
    #     if params['filter_type'] == '广告活动筛选':
    #         csv_filename = access_param(params, params['brand'])
    #     elif params['filter_type'] == '直接筛选':
    #         csv_filename = access_param_self(params, params['brand'])
    # 在这里可以返回响应给前端，例如确认信息或处理结果
        csv_filepath1 = os.path.join(os.getcwd(), csv_filepath)
    return send_file(csv_filepath1, as_attachment=True, attachment_filename=csv_filepath,mimetype='text/csv')


# 处理表单提交的路由
@app.route('/run_function', methods=['POST'])
def run_function():
    today = datetime.today()
    cur_time = today.strftime('%Y-%m-%d')
    # 从表单中获取参数
    with lock:
        params = {
            'country': request.form.get('country', ''),  # 使用get方法提供默认值以防参数不存在
            'brand': request.form.get('brand'),
            'ad_type': request.form.get('ad_type', ''),
            'ad_options': request.form.get('ad_options', ''),
            'cur_time': cur_time,
            'filter_type': request.form.get('filter_type', ''),
        }

    # 检查是否选择了需要额外参数的广告类型和选项

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
        if params['filter_type'] == '广告活动筛选':
            csv_filename = access_param(params, params['brand'])
        elif params['filter_type'] == '直接筛选':
            csv_filename = access_param_self(params, params['brand'])
    # 在这里可以返回响应给前端，例如确认信息或处理结果
        csv_filepath = os.path.join(os.getcwd(), csv_filename)
    return send_file(csv_filepath, as_attachment=True, attachment_filename=csv_filename,mimetype='text/csv')

# @app.route('/query_function', methods=['POST'])
# def query_function():
#     global csv_filepath
#     with lock:
#     # 如果 csv_filepath 为空，表示还没有执行 run_function，可以根据需要处理
#         if csv_filepath is None:
#             print('CSV file not generated yet.')
#             return jsonify({"status": "error", "message": "CSV file not generated yet."}), 400
#
#         # 在这里可以根据 csv_filepath 执行你的修改操作
#         try:
#             # 示例：读取 CSV 文件内容
#             df = pd.read_csv(csv_filepath)
#             df_data = json.loads(df.to_json(orient='records'))
#             api1 = auto_api('LAPASA')
#
#             # 示例：模拟耗时操作
#             if params['filter_type'] == '广告活动筛选':
#                 if params['ad_options'] == '预算':
#                     print('执行修改操作...')
#                     for item in df_data:
#                         campaignId = item["campaignId"]
#                         api1.auto_campaign_budget(params['country'], campaignId, params['price_adjustment_operation'], params['price_adjustment'])
#                 elif params['ad_options'] == '广告位':
#                     print('执行修改操作...')
#                     for item in df_data:
#                         campaignId = item["campaignId"]
#                         api1.auto_campaign_targeting_group(params['country'], campaignId, params['price_adjustment_operation'], params['price_adjustment'])
#                 elif params['ad_type'] == 'SP-手动' and params['ad_options'] == '关键词':
#                     print('执行修改操作...')
#                     for item in df_data:
#                         campaignId = item["campaignId"]
#                         api1.auto_campaign_keyword(params['country'], campaignId, params['price_adjustment_operation'], params['price_adjustment'])
#                 elif params['ad_type'] == 'SP-ASIN' and params['ad_options'] == '商品投放':
#                     print('执行修改操作...')
#                     for item in df_data:
#                         campaignId = item["campaignId"]
#                         api1.auto_campaign_product_targets(params['country'], campaignId, params['price_adjustment_operation'], params['price_adjustment'])
#                 elif params['ad_type'] == 'SP-自动' and params['ad_options'] == '自动定位组':
#                     print('执行修改操作...')
#                     for item in df_data:
#                         campaignId = item["campaignId"]
#                         api1.auto_campaign_automatic_targeting(params['country'], campaignId, params['price_adjustment_operation'], params['price_adjustment'])
#         #time.sleep(10)
#
#             return jsonify({"status": "success", "message": "Modification completed."})
#
#         except Exception as e:
#             print(e)
#             return jsonify({"status": "error", "message": str(e)}), 500


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
            params = {
                'operationOption': request.form.get('operationOption', ''),  # 使用get方法提供默认值以防参数不存在
            }
            print(params)
            if params['operationOption'] == 'createCampaign':
                params_modify = {
                    'brand': request.form.get('brand'),
                    'country': request.form.get('country'),
                    'templateCountry': request.form.get('templateCountry'),
                    'strategy': request.form.get('strategy'),
                    'budget': request.form.get('budget')
                }
                # params_modify.update(params)
                api1 = Ceate_new_sd()
                api2 = Ceate_new_sku()
                if params_modify['strategy'] == "0502":
                    if params_modify['brand'] == "Veement" or params_modify['brand'] == "KAPEYDESI" or params_modify['brand'] == "Gvyugke" or params_modify['brand'] == "Uuoeebb" or params_modify['brand'] == "syndesmos" or params_modify['brand'] == "Gonbouyoku" or params_modify['brand'] == "gmrpwnage":
                        api2.create_new_sp_manual_no_template_jiutong(params_modify['country'],params_modify['brand'],csv_path,params_modify['budget'])
                    else:
                        def run_async_operation():
                            loop = asyncio.new_event_loop()
                            asyncio.set_event_loop(loop)
                            try:
                                loop.run_until_complete(api2.create_new_sku(params_modify['country'], params_modify['templateCountry'], params_modify['brand'], csv_path, params_modify['budget']))
                            finally:
                                loop.close()

                        thread = threading.Thread(target=run_async_operation)
                        thread.start()
                        thread.join()  # 等待子线程完成
                elif params_modify['strategy'] == "0507":
                    api1.create_new_sd_template(params_modify['country'], params_modify['templateCountry'], params_modify['brand'], csv_path, params_modify['budget'])
                elif params_modify['strategy'] == "0808":
                    api1.create_new_sd_0808(params_modify['country'], csv_path, params_modify['brand'], params_modify['budget'])
                # Return a response if needed
                return f'创建完成'
            elif params['operationOption'] == 'priceAdjustment':
                params_modify = {
                    'brand': request.form.get('adjustmentBrand'),
                    'country': request.form.get('adjustmentCountry'),
                    'adType': request.form.get('adType'),
                    'adjustmentPosition': request.form.get('adjustmentPosition'),
                    'adjustmentMethod': request.form.get('adjustmentMethod'),
                }
                if params_modify['adType'] == 'SP':
                    api1 = auto_api_sp(params_modify['brand'])
                    if params_modify['adjustmentMethod'] == 'byCampaign':
                        if params_modify['adjustmentPosition'] == 'budget':
                            print('执行修改操作...')
                            api1.auto_campaign_budget(params_modify['country'], csv_path)
                        elif params_modify['adjustmentPosition'] == 'adPlacement':
                            print('执行修改操作...')
                            api1.auto_campaign_targeting_group(params_modify['country'], csv_path)
                        elif params_modify['adjustmentPosition'] == 'keywords':
                            print('执行修改操作...')
                            api1.auto_campaign_keyword(params_modify['country'], csv_path)
                        elif params_modify['adjustmentPosition'] == 'productTargeting':
                            print('执行修改操作...')
                            api1.auto_campaign_product_targets(params_modify['country'], csv_path)
                        elif params_modify['adjustmentPosition'] == 'autoTargeting':
                            print('执行修改操作...')
                            api1.auto_campaign_automatic_targeting(params_modify['country'], csv_path)
                    elif params_modify['adjustmentMethod'] == 'direct':
                        api2 = auto_api(params_modify['brand'], params_modify['country'])
                        if params_modify['adjustmentPosition'] == 'budget':
                            print('执行修改操作...')
                            api1.auto_campaign_budget(params_modify['country'], csv_path)
                        elif params_modify['adjustmentPosition'] == 'adPlacement':
                            print('执行修改操作...')
                            api2.update_sp_ad_placement(params_modify['country'], csv_path)
                        elif params_modify['adjustmentPosition'] == 'keywords':
                            print('执行修改操作...')
                            api2.update_sp_ad_keyword(params_modify['country'], csv_path)
                        elif params_modify['adjustmentPosition'] == 'productTargeting':
                            print('执行修改操作...')
                            api2.update_sp_ad_product_targets(params_modify['country'], csv_path)
                        elif params_modify['adjustmentPosition'] == 'autoTargeting':
                            print('执行修改操作...')
                            api2.update_sp_ad_automatic_targeting(params_modify['country'], csv_path)
                elif params_modify['adType'] == 'SD':
                    api1 = auto_api_sd(params_modify['brand'])
                    if params_modify['adjustmentMethod'] == 'byCampaign':
                        if params_modify['adjustmentPosition'] == 'budget':
                            print('执行修改操作...')
                            api1.auto_campaign_budget(params_modify['country'], csv_path)
                        elif params_modify['adjustmentPosition'] == 'productTargeting':
                            print('执行修改操作...')
                            api1.auto_campaign_product_targets(params_modify['country'], csv_path)
                    elif params_modify['adjustmentMethod'] == 'direct':
                        api2 = auto_api_sd2(params_modify['brand'], params_modify['country'])
                        if params_modify['adjustmentPosition'] == 'budget':
                            print('执行修改操作...')
                            api1.auto_campaign_budget(params_modify['country'], csv_path)
                        elif params_modify['adjustmentPosition'] == 'productTargeting':
                            print('执行修改操作...')
                            api2.update_sd_ad_product_targets(params_modify['country'], csv_path)
                return f'修改完成'
            elif params['operationOption'] == 'modifyStatus':
                params_modify = {
                    'brand': request.form.get('modifyBrand'),
                    'country': request.form.get('modifyCountry'),
                    'adType': request.form.get('modifyAdType'),
                    'modifyPosition': request.form.get('modifyPosition'),
                    'status': request.form.get('status'),
                }
                if params_modify['adType'] == 'SP':
                    api1 = auto_api_sp(params_modify['brand'])
                    if params_modify['modifyPosition'] == 'campaign':
                        print('执行修改操作...')
                        api1.auto_campaign_status(params_modify['country'], csv_path, params_modify['status'])
                    elif params_modify['modifyPosition'] == 'SKU':
                        print('执行修改操作...')
                        api1.auto_sku_status(params_modify['country'], csv_path, params_modify['status'])
                    elif params_modify['modifyPosition'] == 'keywords':
                        print('执行修改操作...')
                        api1.auto_keyword_status(params_modify['country'], csv_path, params_modify['status'])
                    elif params_modify['modifyPosition'] == 'productTargeting':
                        print('执行修改操作...')
                        api1.auto_targeting_status(params_modify['country'], csv_path, params_modify['status'])
                    elif params_modify['modifyPosition'] == 'autoTargeting':
                        print('执行修改操作...')
                        api1.auto_targeting_status(params_modify['country'], csv_path, params_modify['status'])
                elif params_modify['adType'] == 'SD':
                    api1 = auto_api_sd(params_modify['brand'])
                    if params_modify['modifyPosition'] == 'campaign':
                        print('执行修改操作...')
                        api1.auto_campaign_status(params_modify['country'], csv_path, params_modify['status'])
                    elif params_modify['modifyPosition'] == 'SKU':
                        print('执行修改操作...')
                        api1.auto_sku_status(params_modify['country'], csv_path, params_modify['status'])
                    elif params_modify['modifyPosition'] == 'productTargeting':
                        print('执行修改操作...')
                        api1.auto_targeting_status(params_modify['country'], csv_path, params_modify['status'])
                return f'修改完成'
            elif params['operationOption'] == 'createTargeting':
                params_modify = {
                    'brand': request.form.get('targetingBrand'),
                    'country': request.form.get('targetingCountry'),
                    'adType': request.form.get('targetingAdType'),
                    'targetingPosition': request.form.get('targetingPosition')
                }
                if params_modify['adType'] == 'SP':
                    api1 = auto_api_sp(params_modify['brand'])
                    if params_modify['targetingPosition'] == 'productTargeting1':
                        print('执行修改操作...')
                        api1.auto_create_targeting_category(params_modify['country'], csv_path)
                    elif params_modify['targetingPosition'] == 'productTargeting2':
                        print('执行修改操作...')
                        api1.auto_create_targeting_product(params_modify['country'], csv_path)
                    elif params_modify['targetingPosition'] == 'keywords':
                        print('执行修改操作...')
                        api1.auto_create_keyword(params_modify['country'], csv_path)
                    elif params_modify['targetingPosition'] == 'negativeProductTargeting':
                        print('执行修改操作...')
                        api1.auto_create_negative_targeting(params_modify['country'], csv_path)
                    elif params_modify['targetingPosition'] == 'negativeKeywords':
                        print('执行修改操作...')
                        api1.auto_create_negative_keyword(params_modify['country'], csv_path)
                elif params_modify['adType'] == 'SD':
                    api1 = auto_api_sd(params_modify['brand'])
                    if params_modify['targetingPosition'] == 'productTargeting1':
                        print('执行修改操作...')
                        api1.auto_create_targeting_category(params_modify['country'], csv_path)
                    elif params_modify['targetingPosition'] == 'productTargeting2':
                        print('执行修改操作...')
                        api1.auto_create_targeting_product(params_modify['country'], csv_path)
                    elif params_modify['targetingPosition'] == 'negativeProductTargeting':
                        print('执行修改操作...')
                        api1.auto_create_negative_targeting_product(params_modify['country'], csv_path)
                return f'修改完成'
        except Exception as e:
            print(e)
            return jsonify({"status": "error", "message": str(e)}), 500


if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0', port=4009, threaded=True)

