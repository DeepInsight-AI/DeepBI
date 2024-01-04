import random
import json


# bar 图表配置
def acquiesce_echarts_code_bar(x_data, y_fields, data):
    option = {
        "tooltip": {
            "show": False
        },
        "grid": {
            "top": '0%',
            "left": '50',
            "right": '50',
            "bottom": '0%',
        },
        "xAxis": {
            "min": 0,
            # "max": 12000,
            "splitLine": {
                "show": False
            },
            "axisTick": {
                "show": False
            },
            "axisLine": {
                "show": False
            },
            "axisLabel": {
                "show": False
            },
        },
        "yAxis": {
            "data": x_data,
            # "offset": 15,
            "axisTick": {
                "show": False
            },
            "axisLine": {
                "show": False
            },
            "axisLabel": {
                "color": 'rgba(255,255,255,.6)',
                "fontSize": 14,
            }
        },
        "series": []
    }
    # 对每个Y轴字段，提取数据并添加到series中
    for y_field in y_fields:
        y_data = [row[y_field] for row in data['data']['rows']]
        option['series'].append({
            "type": data['chart_type'],
            "label": {
                "show": True,
                "zlevel": 10000,
                "position": 'right',
                "padding": 6,
                "color": '#4e84a1',
                "fontSize": 14,
                "formatter": '{c}'
            },
            "itemStyle": {
                "barBorderRadius": 25,
                "color": '#3facff'
            },
            "barWidth": '15',
            "data": y_data,
            "z": 6
        })
    return option


# line 图表配置
def acquiesce_echarts_code_line(x_data, y_fields, data):
    option = {
        "tooltip": {
            "trigger": 'axis',
            "axisPointer": {"type": 'shadow'}
        },
        "grid": {
            "left": '0%',
            "top": '10px',
            "right": '0%',
            "bottom": '0px',
            "containLabel": True
        },
        "xAxis": [{
            "type": 'category',
            "data": x_data,
            "axisLine": {
                "show": True,
                "lineStyle": {
                    "color": "rgba(255,255,255,.1)",
                    "width": 1,
                    "type": "solid"
                },
            },
            "axisTick": {
                "show": False,
            },
            "axisLabel": {
                "interval": 0,
                "show": True,
                "splitNumber": 15,
                "textStyle": {
                    "color": "rgba(255,255,255,.6)",
                    "fontSize": '12',
                },
            },
        }],
        "yAxis": [{
            "type": 'value',
            "axisLabel": {
                "show": True,
                "textStyle": {
                    "color": "rgba(255,255,255,.6)",
                    "fontSize": '12',
                },
            },
            "axisTick": {
                "show": False,
            },
            "axisLine": {
                "show": True,
                "lineStyle": {
                    "color": "rgba(255,255,255,.1)",
                    "width": 1,
                    "type": "solid"
                },
            },
            "splitLine": {
                "lineStyle": {
                    "color": "rgba(255,255,255,.1)",
                }
            }
        }],
        "series": [
        ]
    }

    for y_field in y_fields:
        y_data = [row[y_field] for row in data['data']['rows']]
        option['series'].append({
            "type": data['chart_type'],
            "itemStyle": {
                "normal": {
                    "color": '#37a3ff',
                    "opacity": 1,
                    "BorderRadius": 3,
                }
            },
            "data": y_data,
        })
    return option


# pie 图表配置
def acquiesce_echarts_code_pie(x_data, y_fields, data):
    option = {
        "tooltip": {
            "trigger": 'item',
            "formatter": '{a} <br/>{b}: {c} ({d}%)'
        },
        "series": [
            {
                "name": 'Pie Chart',
                "type": 'pie',
                "radius": ['50%', '70%'],
                "avoidLabelOverlap": False,
                "label": {
                    "show": False,
                    "position": 'center'
                },
                "emphasis": {
                    "label": {
                        "show": True,
                        "fontSize": '30',
                        "fontWeight": 'bold'
                    }
                },
                "labelLine": {
                    "show": False
                },
                "data": [],
                "color": []
            }
        ]
    }
    for y_field in y_fields:
        y_data = [row[y_field] for row in data['data']['rows']]
        for i in range(len(x_data)):
            option['series'][0]['data'].append({"value": y_data[i], "name": x_data[i]})
            option['series'][0]['color'].append("#" + ''.join([random.choice('0123456789ABCDEF') for j in range(6)]))

    return option


# python生成echart代码
def acquiesce_echarts_code(data):
    # 提取X轴和Y轴的字段名
    x_field = [k for k, v in data['columnMapping'].items() if v == 'x'][0]
    y_fields = [k for k, v in data['columnMapping'].items() if v == 'y']

    # 提取X轴的数据
    x_data = [row[x_field] for row in data['data']['rows']]
    # print("x_data===== ", x_data)
    echarts_code = {}
    if data['chart_type'] == 'bar':
        echarts_code = acquiesce_echarts_code_bar(x_data, y_fields, data)
    elif data['chart_type'] == 'line':
        echarts_code = acquiesce_echarts_code_line(x_data, y_fields, data)
    elif data['chart_type'] == 'pie':
        echarts_code = acquiesce_echarts_code_pie(x_data, y_fields, data)
    elif data['chart_type'] == 'area':
        echarts_code = acquiesce_echarts_code_line(x_data,y_fields,data)
    return json.dumps(echarts_code, indent=4)
