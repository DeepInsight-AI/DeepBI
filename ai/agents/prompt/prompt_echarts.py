CSV_ECHART_TIPS_MESS = """Here are some examples of generating mysql and pyecharts Code based on the given question.
 Please generate new one based on the data and question human asks you, import the neccessary libraries and make sure the code is correct.

IMPORTANT: You need to follow the coding style, and the type of the x, y axis.Title and label are not displayed under any circumstances. In either case, the dataZoomInside and dataZoomSlider for the x-axis and y-axis, as well as the toolbox, must be displayed. The legend is set to scroll based on type_="scroll" and orient="horizontal". If the x-axis can be sorted according to certain rules (such as date and time size or value size), please sort by the x-axis, otherwise sort by size.But also need to focus on the column name of the uploaded tables(if exists). Generally, PyEcharts does not accept numpy.int or numpy.float, etc. It only supports built-in data type like int, float, and str.
Pay attention to check whether the query statement in the execution code block can correctly query the data.


    Given the same `company_sales.xlsx`.
    Q: A line chart comparing sales and profit over time would be useful. Could you help plot it?
    <code>
    import pandas as pd
    from pyecharts.charts import Line
    from pyecharts import options as opts
    import json

    df = pd.read_excel('company_sales.xlsx')
    year = [str(_) for _ in df["year"].to_list()]
    sales = [float(_) for _ in df["sales"].to_list()]
    profit = [float(_) for _ in df["profit"].to_list()]
    line = Line()
    # Add x-axis and y-axis data
    line.add_xaxis(year)
    line.add_yaxis("Sales", sales)
    line.add_yaxis("Profit", profit)
    line.set_global_opts(
        xaxis_opts=opts.AxisOpts(
            type_="category", # better use category rather than value
            name="year",
            min_=min(year),
            max_=max(year),
        ),
        yaxis_opts=opts.AxisOpts(
            type_="value",
            name="price",
        ),
        title_opts=opts.TitleOpts(title="Sales and Profit over Time",is_show=False),
        datazoom_opts=[
            opts.DataZoomOpts(
                # 设置 x 轴 dataZoom
                id_="dataZoomX",
                type_="slider",
                xAxisIndex=[0],  # 控制 x 轴
                filter_mode="empty",  # 设置数据过滤模式为 'empty'
                orient:"horizontal",
            ),
            opts.DataZoomOpts(
                # 设置 y 轴 dataZoom
                id_="dataZoomY",
                type_="slider",
                yAxisIndex=[0],  # 控制 y 轴
                filter_mode="empty",  # 设置数据过滤模式为 'empty'
                orient:"vertical",
            ),
            opts.DataZoomOpts(
                # 设置 x 轴 dataZoom
                id_="dataZoomX",
                type_="inside",
                xAxisIndex=[0],  # 控制 x 轴
                filter_mode="empty",  # 设置数据过滤模式为 'empty'
                orient:"horizontal",
            ),
            opts.DataZoomOpts(
                # 设置 y 轴 dataZoom
                id_="dataZoomY",
                type_="inside",
                yAxisIndex=[0],  # 控制 y 轴
                filter_mode="empty",  # 设置数据过滤模式为 'empty'  
                orient:"vertical",              
            ),
        ],
        legend_opts=opts.LegendOpts(
            type_="scroll",  # 设置图例类型为滚动
            pos_top="50%",   # 设置图例距离顶部的位置
            pos_left="center",  # 设置图例居中
            orient="horizontal",  # 设置图例水平显示
            # 下面是图例滚动相关的配置
            page_size=5,  # 设置每页显示的图例项个数
            page_icon_size=15,  # 设置翻页按钮的大小
            page_icon_color="#2f4554",  # 设置翻页按钮的颜色
            page_icon_inactive_color="#aaa",  # 设置翻页按钮不激活时的颜色
        ),
        toolbox_opts=opts.ToolboxOpts(
            is_show=True,
            orient="horizontal",
            pos_left="80%",
            feature={
                "dataZoom": opts.ToolBoxFeatureDataZoomOpts(),
                "dataView": opts.ToolBoxFeatureDataViewOpts(),
                "magicType": opts.ToolBoxFeatureMagicTypeOpts(type_=['line', 'bar','stack']),
                "restore": opts.ToolBoxFeatureRestoreOpts(),
                "saveAsImage": opts.ToolBoxFeatureSaveAsImageOpts(),
                pos_top= "25%",
            },
        ),
    )
    line.set_series_opts(
        areastyle_opts=opts.AreaStyleOpts(opacity=0.5),
        label_opts=opts.LabelOpts(is_show=False),
    )
    ret_json = line.dump_options()
    echart_code = json.loads(ret_json)

    out_put = [{"echart_name": "Sales and Profit over Time", "echart_code": echart_code}]
    print(out_put)
    </code>


    Given the following data:
    company_sales.xlsx
    year  sales  profit  expenses  employees
    0  2010    100      60        40         10
    1  2011    120      80        50         12
    2  2012    150      90        60         14
    3  2013    170     120        70         16
    [too long to show]

    Q: Could you help plot a bar chart with the year on the x-axis and the sales on the y-axis?
    <code>
    import pandas as pd
    from pyecharts.charts import Bar
    from pyecharts import options as opts
    df = pd.read_excel('company_sales.xlsx')
    years = [str(_) for _ in df['year'].tolist()]
    sales = [float(_) for _ in df['sales'].tolist()]
    bar = Bar()
    bar.add_xaxis(years)
    bar.add_yaxis("Sales", sales)
    bar.set_global_opts(
        xaxis_opts=opts.AxisOpts(
            type_="category",
            name="Year",
        ),
        yaxis_opts=opts.AxisOpts(
            type_="value",
            name="Sales",
        ),
        title_opts=opts.TitleOpts(title="Sales over Years",is_show=False),
        datazoom_opts=[
            opts.DataZoomOpts(
                # 设置 x 轴 dataZoom
                id_="dataZoomX",
                type_="slider",
                xAxisIndex=[0],  # 控制 x 轴
                filter_mode="empty",  # 设置数据过滤模式为 'empty'
                orient:"horizontal",
            ),
            opts.DataZoomOpts(
                # 设置 y 轴 dataZoom
                id_="dataZoomY",
                type_="slider",
                yAxisIndex=[0],  # 控制 y 轴
                filter_mode="empty",  # 设置数据过滤模式为 'empty'
                orient:"vertical",
            ),
            opts.DataZoomOpts(
                # 设置 x 轴 dataZoom
                id_="dataZoomX",
                type_="inside",
                xAxisIndex=[0],  # 控制 x 轴
                filter_mode="empty",  # 设置数据过滤模式为 'empty'
                orient:"horizontal",
            ),
            opts.DataZoomOpts(
                # 设置 y 轴 dataZoom
                id_="dataZoomY",
                type_="inside",
                yAxisIndex=[0],  # 控制 y 轴
                filter_mode="empty",  # 设置数据过滤模式为 'empty'  
                orient:"vertical",              
            ),
        ],
        legend_opts=opts.LegendOpts(
            type_="scroll",  # 设置图例类型为滚动
            pos_top="50%",   # 设置图例距离顶部的位置
            pos_left="center",  # 设置图例居中
            orient="horizontal",  # 设置图例水平显示
            # 下面是图例滚动相关的配置
            page_size=5,  # 设置每页显示的图例项个数
            page_icon_size=15,  # 设置翻页按钮的大小
            page_icon_color="#2f4554",  # 设置翻页按钮的颜色
            page_icon_inactive_color="#aaa",  # 设置翻页按钮不激活时的颜色
        ),
        toolbox_opts=opts.ToolboxOpts(
            is_show=True,
            orient="horizontal",
            pos_left="80%",
            feature={
                "dataZoom": opts.ToolBoxFeatureDataZoomOpts(),
                "dataView": opts.ToolBoxFeatureDataViewOpts(),
                "magicType": opts.ToolBoxFeatureMagicTypeOpts(type_=['line', 'bar','stack']),
                "restore": opts.ToolBoxFeatureRestoreOpts(),
                "saveAsImage": opts.ToolBoxFeatureSaveAsImageOpts(),
                pos_top= "25%",
            },
        ),
    )
    bar.set_series_opts(
        label_opts=opts.LabelOpts(is_show=False),
    )
    # Render the chart
    ret_json = bar.dump_options()
    echart_code = json.loads(ret_json)

    out_put = [{"echart_name": "Sales over Years", "echart_code": echart_code}]
    print(out_put)
    </code>

    The output should be formatted as a JSON instance that conforms to the JSON schema below, the JSON is a list of dict,
    [
    {"echart_name": "Sales over Years", "echart_code": ret_json}
    {},
    {},
    ].
    """

MYSQL_ECHART_TIPS_MESS = '''
Here are some examples of generating mysql and pyecharts Code based on the given question.
Please generate new one based on the data and question human asks you, import the neccessary libraries and make sure the code is correct.

IMPORTANT: You need to follow the coding style, and the type of the x, y axis.Title and label are not displayed under any circumstances. In either case, the dataZoomInside and dataZoomSlider for the x-axis and y-axis, as well as the toolbox, must be displayed. The legend is set to scroll based on type_="scroll" and orient="horizontal". If the x-axis can be sorted according to certain rules (such as date and time size or value size), please sort by the x-axis, otherwise sort by size.But also need to focus on the column name of the uploaded tables(if exists). Generally, PyEcharts does not accept numpy.int or numpy.float, etc. It only supports built-in data type like int, float, and str.
Pay attention to check whether the query statement in the execution code block can correctly query the data.
The sql statements that need to be executed in the python code are surrounded by ", for example: query = "SELECT year, sales, profit FROM your_table"
Pay attention to check whether the sql statement in the code block is correct and available.


    Q: A `stacked` line chart comparing sales and profit over time would be useful. Could you help plot it?
    Note: stacked line chart is more fancy in display, while the former is more neat.
    <code>
    import pymysql
    import pandas as pd
    from pyecharts.charts import Line
    from pyecharts import options as opts
    import json

    connection = pymysql.connect(
        host='your_host',
        user='your_user',
        password='your_password',
        database='your_database',
        port="your_port"
    )

    query = """select DATE_FORMAT(o.`date`, '%Y-%m') AS `month`, `sales`, `profit` FROM order_list  WHERE DATE_FORMAT(`date`, '%Y') = '2018' GROUP BY `month`"""

    df = pd.read_sql(query, con=connection)
    connection.close()

    year = df["month"].astype(str).tolist()
    sales = df["sales"].astype(float).tolist()
    profit = df["profit"].astype(float).tolist()

    line = Line()
    line.add_xaxis(month)
    line.add_yaxis("Sales", sales, stack="")
    line.add_yaxis("Profit", profit, stack="")

    line.set_global_opts(
        xaxis_opts=opts.AxisOpts(
            type_="category",
            name="Month",
            boundary_gap=False
        ),
        yaxis_opts=opts.AxisOpts(
            type_="value",
            name="Amount",
            axistick_opts=opts.AxisTickOpts(is_show=True),
            splitline_opts=opts.SplitLineOpts(is_show=True),
        ),
        title_opts=opts.TitleOpts(title="Sales and Profit over Time",is_show=False),
        datazoom_opts=[
            opts.DataZoomOpts(
                # 设置 x 轴 dataZoom
                id_="dataZoomX",
                type_="slider",
                xAxisIndex=[0],  # 控制 x 轴
                filter_mode="empty",  # 设置数据过滤模式为 'empty'
                orient:"horizontal",
            ),
            opts.DataZoomOpts(
                # 设置 y 轴 dataZoom
                id_="dataZoomY",
                type_="slider",
                yAxisIndex=[0],  # 控制 y 轴
                filter_mode="empty",  # 设置数据过滤模式为 'empty'
                orient:"vertical",
            ),
            opts.DataZoomOpts(
                # 设置 x 轴 dataZoom
                id_="dataZoomX",
                type_="inside",
                xAxisIndex=[0],  # 控制 x 轴
                filter_mode="empty",  # 设置数据过滤模式为 'empty'
                orient:"horizontal",
            ),
            opts.DataZoomOpts(
                # 设置 y 轴 dataZoom
                id_="dataZoomY",
                type_="inside",
                yAxisIndex=[0],  # 控制 y 轴
                filter_mode="empty",  # 设置数据过滤模式为 'empty'  
                orient:"vertical",              
            ),
        ],
        legend_opts=opts.LegendOpts(
            type_="scroll",  # 设置图例类型为滚动
            pos_top="50%",   # 设置图例距离顶部的位置
            pos_left="center",  # 设置图例居中
            orient="horizontal",  # 设置图例水平显示
            # 下面是图例滚动相关的配置
            page_size=5,  # 设置每页显示的图例项个数
            page_icon_size=15,  # 设置翻页按钮的大小
            page_icon_color="#2f4554",  # 设置翻页按钮的颜色
            page_icon_inactive_color="#aaa",  # 设置翻页按钮不激活时的颜色
        ),
        toolbox_opts=opts.ToolboxOpts(
            is_show=True,
            orient="horizontal",
            pos_left="80%",
            feature={
                "dataZoom": opts.ToolBoxFeatureDataZoomOpts(),
                "dataView": opts.ToolBoxFeatureDataViewOpts(),
                "magicType": opts.ToolBoxFeatureMagicTypeOpts(type_=['line', 'bar','stack']),
                "restore": opts.ToolBoxFeatureRestoreOpts(),
                "saveAsImage": opts.ToolBoxFeatureSaveAsImageOpts(),
                pos_top= "25%",
            },
        ),
    )

    line.set_series_opts(
        areastyle_opts=opts.AreaStyleOpts(opacity=0.5),
        label_opts=opts.LabelOpts(is_show=False),
    )

    ret_json = line.dump_options()
    echart_code = json.loads(ret_json)

    out_put = [{"echart_name": "Sales and Profit over Time", "echart_code": echart_code}]
    print(out_put)
    </code>

    Q: Could you help plot a bar chart with the year on the x-axis and the sales on the y-axis?
    <code>
    import pymysql
    import pandas as pd
    from pyecharts.charts import Bar
    from pyecharts import options as opts
    import json

    connection = pymysql.connect(
        host='your_host',
        user='your_user',
        password='your_password',
        database='your_database',
        port="your_port"
    )

    query = "SELECT year, sales FROM your_table  WHERE DATE_FORMAT(`date`, '%Y') = '2018' "

    df = pd.read_sql(query, con=connection)

    connection.close()

    years = [str(_) for _ in df['year'].tolist()]
    sales = [float(_) for _ in df['sales'].tolist()]

    bar = Bar()
    bar.add_xaxis(years)
    bar.add_yaxis("Sales", sales)
    bar.set_global_opts(
        xaxis_opts=opts.AxisOpts(
            type_="category",
            name="Year",
        ),
        yaxis_opts=opts.AxisOpts(
            type_="value",
            name="Sales",
        ),
        title_opts=opts.TitleOpts(title="Sales over Years",False),
        datazoom_opts=[
            opts.DataZoomOpts(
                # 设置 x 轴 dataZoom
                id_="dataZoomX",
                type_="slider",
                xAxisIndex=[0],  # 控制 x 轴
                filter_mode="empty",  # 设置数据过滤模式为 'empty'
                orient:"horizontal",
            ),
            opts.DataZoomOpts(
                # 设置 y 轴 dataZoom
                id_="dataZoomY",
                type_="slider",
                yAxisIndex=[0],  # 控制 y 轴
                filter_mode="empty",  # 设置数据过滤模式为 'empty'
                orient:"vertical",
            ),
            opts.DataZoomOpts(
                # 设置 x 轴 dataZoom
                id_="dataZoomX",
                type_="inside",
                xAxisIndex=[0],  # 控制 x 轴
                filter_mode="empty",  # 设置数据过滤模式为 'empty'
                orient:"horizontal",
            ),
            opts.DataZoomOpts(
                # 设置 y 轴 dataZoom
                id_="dataZoomY",
                type_="inside",
                yAxisIndex=[0],  # 控制 y 轴
                filter_mode="empty",  # 设置数据过滤模式为 'empty'  
                orient:"vertical",              
            ),
        ],
        legend_opts=opts.LegendOpts(
            type_="scroll",  # 设置图例类型为滚动
            pos_top="50%",   # 设置图例距离顶部的位置
            pos_left="center",  # 设置图例居中
            orient="horizontal",  # 设置图例水平显示
            # 下面是图例滚动相关的配置
            page_size=5,  # 设置每页显示的图例项个数
            page_icon_size=15,  # 设置翻页按钮的大小
            page_icon_color="#2f4554",  # 设置翻页按钮的颜色
            page_icon_inactive_color="#aaa",  # 设置翻页按钮不激活时的颜色
        ),
        toolbox_opts=opts.ToolboxOpts(
            is_show=True,
            orient="horizontal",
            pos_left="80%",
            feature={
                "dataZoom": opts.ToolBoxFeatureDataZoomOpts(),
                "dataView": opts.ToolBoxFeatureDataViewOpts(),
                "magicType": opts.ToolBoxFeatureMagicTypeOpts(type_=['line', 'bar','stack']),
                "restore": opts.ToolBoxFeatureRestoreOpts(),
                "saveAsImage": opts.ToolBoxFeatureSaveAsImageOpts(),
                pos_top= "25%",
            },
        ),
    )
    bar.set_series_opts(
        label_opts=opts.LabelOpts(is_show=False),
    )

    ret_json = bar.dump_options()
    echart_code = json.loads(ret_json)

    out_put = [{"echart_name": "Sales over Years", "echart_code": echart_code}]
    print(out_put)
    </code>


      The output should be formatted as a JSON instance that conforms to the JSON schema below, the JSON is a list of dict,
                        [
                        {"echart_name": "Sales over Years", "echart_code": ret_json}
                        {},
                        {},
                        ].

'''

POSTGRESQL_ECHART_TIPS_MESS = '''
Here are some examples of generating postgresql and pyecharts Code based on the given question.
Please generate new one based on the data and question human asks you, import the neccessary libraries and make sure the code is correct.

IMPORTANT: You need to follow the coding style, and the type of the x, y axis.Title and label are not displayed under any circumstances. In either case, the dataZoomInside and dataZoomSlider for the x-axis and y-axis, as well as the toolbox, must be displayed. The legend is set to scroll based on type_="scroll" and orient="horizontal". If the x-axis can be sorted according to certain rules (such as date and time size or value size), please sort by the x-axis, otherwise sort by size.But also need to focus on the column name of the uploaded tables(if exists). Generally, PyEcharts does not accept numpy.int or numpy.float, etc. It only supports built-in data type like int, float, and str.
Pay attention to check whether the query statement in the execution code block can correctly query the data.


    Q: A `stacked` line chart comparing sales and profit over time would be useful. Could you help plot it?
    Note: stacked line chart is more fancy in display, while the former is more neat.
    <code>
    import psycopg2
    import pandas as pd
    from pyecharts.charts import Line
    from pyecharts import options as opts
    import json

    connection = psycopg2.connect(
        dbname="your_dbname",
        user="your_username",
        password="your_password",
        host="your_host",
        port="your_port"
    )

    query = """SELECT `year`, `sales`, profit FROM your_table"""

    df = pd.read_sql(query, con=connection)
    connection.close()

    year = df["year"].astype(str).tolist()
    sales = df["sales"].astype(float).tolist()
    profit = df["profit"].astype(float).tolist()

    line = Line()
    line.add_xaxis(year)
    line.add_yaxis("Sales", sales, stack="")
    line.add_yaxis("Profit", profit, stack="")

    line.set_global_opts(
        xaxis_opts=opts.AxisOpts(
            type_="category",
            name="Year",
            boundary_gap=False
        ),
        yaxis_opts=opts.AxisOpts(
            type_="value",
            name="Amount",
            axistick_opts=opts.AxisTickOpts(is_show=True),
            splitline_opts=opts.SplitLineOpts(is_show=True),
        ),
        title_opts=opts.TitleOpts(title="Sales and Profit over Time",is_show=False),
        datazoom_opts=[
            opts.DataZoomOpts(
                # 设置 x 轴 dataZoom
                id_="dataZoomX",
                type_="slider",
                xAxisIndex=[0],  # 控制 x 轴
                filter_mode="empty",  # 设置数据过滤模式为 'empty'
                orient:"horizontal",
            ),
            opts.DataZoomOpts(
                # 设置 y 轴 dataZoom
                id_="dataZoomY",
                type_="slider",
                yAxisIndex=[0],  # 控制 y 轴
                filter_mode="empty",  # 设置数据过滤模式为 'empty'
                orient:"vertical",
            ),
            opts.DataZoomOpts(
                # 设置 x 轴 dataZoom
                id_="dataZoomX",
                type_="inside",
                xAxisIndex=[0],  # 控制 x 轴
                filter_mode="empty",  # 设置数据过滤模式为 'empty'
                orient:"horizontal",
            ),
            opts.DataZoomOpts(
                # 设置 y 轴 dataZoom
                id_="dataZoomY",
                type_="inside",
                yAxisIndex=[0],  # 控制 y 轴
                filter_mode="empty",  # 设置数据过滤模式为 'empty'  
                orient:"vertical",              
            ),
        ],
        legend_opts=opts.LegendOpts(
            type_="scroll",  # 设置图例类型为滚动
            pos_top="50%",   # 设置图例距离顶部的位置
            pos_left="center",  # 设置图例居中
            orient="horizontal",  # 设置图例水平显示
            # 下面是图例滚动相关的配置
            page_size=5,  # 设置每页显示的图例项个数
            page_icon_size=15,  # 设置翻页按钮的大小
            page_icon_color="#2f4554",  # 设置翻页按钮的颜色
            page_icon_inactive_color="#aaa",  # 设置翻页按钮不激活时的颜色
        ),
        toolbox_opts=opts.ToolboxOpts(
            is_show=True,
            orient="horizontal",
            pos_left="80%",
            feature={
                "dataZoom": opts.ToolBoxFeatureDataZoomOpts(),
                "dataView": opts.ToolBoxFeatureDataViewOpts(),
                "magicType": opts.ToolBoxFeatureMagicTypeOpts(type_=['line', 'bar','stack']),
                "restore": opts.ToolBoxFeatureRestoreOpts(),
                "saveAsImage": opts.ToolBoxFeatureSaveAsImageOpts(),
                pos_top= "25%",
            },
        ),
    )

    line.set_series_opts(
        areastyle_opts=opts.AreaStyleOpts(opacity=0.5),
        label_opts=opts.LabelOpts(is_show=False),
    )

    ret_json = line.dump_options()
    echart_code = json.loads(ret_json)

    out_put = [{"echart_name": "Sales and Profit over Time", "echart_code": echart_code}]
    print(out_put)
    </code>

    Q: Could you help plot a bar chart with the year on the x-axis and the sales on the y-axis?
    <code>
    import psycopg2
    import pandas as pd
    from pyecharts.charts import Bar
    from pyecharts import options as opts
    import json

    connection = psycopg2.connect(
        dbname="your_dbname",
        user="your_username",
        password="your_password",
        host="your_host",
        port="your_port"
    )
    query = "SELECT year, sales FROM your_table"


    df = pd.read_sql(query, con=connection)

    connection.close()

    years = [str(_) for _ in df['year'].tolist()]
    sales = [float(_) for _ in df['sales'].tolist()]

    bar = Bar()
    bar.add_xaxis(years)
    bar.add_yaxis("Sales", sales)
    bar.set_global_opts(
        xaxis_opts=opts.AxisOpts(
            type_="category",
            name="Year",
        ),
        yaxis_opts=opts.AxisOpts(
            type_="value",
            name="Sales",
        ),
        title_opts=opts.TitleOpts(title="Sales over Years",is_show=False),
        datazoom_opts=[
            opts.DataZoomOpts(
                # 设置 x 轴 dataZoom
                id_="dataZoomX",
                type_="slider",
                xAxisIndex=[0],  # 控制 x 轴
                filter_mode="empty",  # 设置数据过滤模式为 'empty'
                orient:"horizontal",
            ),
            opts.DataZoomOpts(
                # 设置 y 轴 dataZoom
                id_="dataZoomY",
                type_="slider",
                yAxisIndex=[0],  # 控制 y 轴
                filter_mode="empty",  # 设置数据过滤模式为 'empty'
                orient:"vertical",
            ),
            opts.DataZoomOpts(
                # 设置 x 轴 dataZoom
                id_="dataZoomX",
                type_="inside",
                xAxisIndex=[0],  # 控制 x 轴
                filter_mode="empty",  # 设置数据过滤模式为 'empty'
                orient:"horizontal",
            ),
            opts.DataZoomOpts(
                # 设置 y 轴 dataZoom
                id_="dataZoomY",
                type_="inside",
                yAxisIndex=[0],  # 控制 y 轴
                filter_mode="empty",  # 设置数据过滤模式为 'empty'  
                orient:"vertical",              
            ),
        ],
        legend_opts=opts.LegendOpts(
            type_="scroll",  # 设置图例类型为滚动
            pos_top="50%",   # 设置图例距离顶部的位置
            pos_left="center",  # 设置图例居中
            orient="horizontal",  # 设置图例水平显示
            # 下面是图例滚动相关的配置
            page_size=5,  # 设置每页显示的图例项个数
            page_icon_size=15,  # 设置翻页按钮的大小
            page_icon_color="#2f4554",  # 设置翻页按钮的颜色
            page_icon_inactive_color="#aaa",  # 设置翻页按钮不激活时的颜色
        ),
        toolbox_opts=opts.ToolboxOpts(
            is_show=True,
            orient="horizontal",
            pos_left="80%",
            feature={
                "dataZoom": opts.ToolBoxFeatureDataZoomOpts(),
                "dataView": opts.ToolBoxFeatureDataViewOpts(),
                "magicType": opts.ToolBoxFeatureMagicTypeOpts(type_=['line', 'bar','stack']),
                "restore": opts.ToolBoxFeatureRestoreOpts(),
                "saveAsImage": opts.ToolBoxFeatureSaveAsImageOpts(),
                pos_top= "25%",
            },
        ),
    )
    bar.set_series_opts(
        label_opts=opts.LabelOpts(is_show=False),
    )

    ret_json = bar.dump_options()
    echart_code = json.loads(ret_json)

    out_put = [{"echart_name": "Sales over Years", "echart_code": echart_code}]
    print(out_put)
    </code>


      The output should be formatted as a JSON instance that conforms to the JSON schema below, the JSON is a list of dict,
                        [
                        {"echart_name": "Sales over Years", "echart_code": ret_json}
                        {},
                        {},
                        ].

'''

MONGODB_ECHART_TIPS_MESS = '''
Here are some examples of generating mongodb and pyecharts Code based on the given question.Please beautify the generated chart to make it clear and readable.
Please generate new one based on the data and question human asks you, import the neccessary libraries and make sure the code is correct.

IMPORTANT: You need to follow the coding style, and the type of the x, y axis.Title and label are not displayed under any circumstances. In either case, the dataZoomInside and dataZoomSlider for the x-axis and y-axis, as well as the toolbox, must be displayed. The legend is set to scroll based on type_="scroll" and orient="horizontal". If the x-axis can be sorted according to certain rules (such as date and time size or value size), please sort by the x-axis, otherwise sort by size.But also need to focus on the column name of the uploaded tables(if exists). Generally, PyEcharts does not accept numpy.int or numpy.float, etc. It only supports built-in data type like int, float, and str.
Pay attention to check whether the query statement in the execution code block can correctly query the data.


    Q: A `stacked` line chart comparing sales and profit over time would be useful. Could you help plot it?
    Note: stacked line chart is more fancy in display, while the former is more neat.
    <code>
    import pymongo
    import pandas as pd
    from pyecharts.charts import Bar
    from pyecharts import options as opts
    import json


    connectionString = "mongodb://your_host:your_port/your_dbname"
    kwargs = {
        'username': 'your_username',
        'password': 'your_password',
    }

    db_connection = pymongo.MongoClient(
        connectionString, **kwargs
    )
    conn = db_connection["your_dbname"]
    res = list(conn['your_table'].find())
    years = [str(_['year']) for _ in res]
    sales = [str(_['sales']) for _ in res]
    bar = Bar()
    bar.add_xaxis(years)
    bar.add_yaxis("Sales", sales)
    bar.set_global_opts(
        xaxis_opts=opts.AxisOpts(
            type_="category",
            name="Year",
        ),
        yaxis_opts=opts.AxisOpts(
            type_="value",
            name="Sales",
        ),
        title_opts=opts.TitleOpts(title="Sales over Years",is_show=False),
        datazoom_opts=[
            opts.DataZoomOpts(
                # 设置 x 轴 dataZoom
                id_="dataZoomX",
                type_="slider",
                xAxisIndex=[0],  # 控制 x 轴
                filter_mode="empty",  # 设置数据过滤模式为 'empty'
                orient:"horizontal",
            ),
            opts.DataZoomOpts(
                # 设置 y 轴 dataZoom
                id_="dataZoomY",
                type_="slider",
                yAxisIndex=[0],  # 控制 y 轴
                filter_mode="empty",  # 设置数据过滤模式为 'empty'
                orient:"vertical",
            ),
            opts.DataZoomOpts(
                # 设置 x 轴 dataZoom
                id_="dataZoomX",
                type_="inside",
                xAxisIndex=[0],  # 控制 x 轴
                filter_mode="empty",  # 设置数据过滤模式为 'empty'
                orient:"horizontal",
            ),
            opts.DataZoomOpts(
                # 设置 y 轴 dataZoom
                id_="dataZoomY",
                type_="inside",
                yAxisIndex=[0],  # 控制 y 轴
                filter_mode="empty",  # 设置数据过滤模式为 'empty'  
                orient:"vertical",              
            ),
        ],
        legend_opts=opts.LegendOpts(
            type_="scroll",  # 设置图例类型为滚动
            pos_top="50%",   # 设置图例距离顶部的位置
            pos_left="center",  # 设置图例居中
            orient="horizontal",  # 设置图例水平显示
            # 下面是图例滚动相关的配置
            page_size=5,  # 设置每页显示的图例项个数
            page_icon_size=15,  # 设置翻页按钮的大小
            page_icon_color="#2f4554",  # 设置翻页按钮的颜色
            page_icon_inactive_color="#aaa",  # 设置翻页按钮不激活时的颜色
        ),
        toolbox_opts=opts.ToolboxOpts(
            is_show=True,
            orient="horizontal",
            pos_left="80%",
            feature={
                "dataZoom": opts.ToolBoxFeatureDataZoomOpts(),
                "dataView": opts.ToolBoxFeatureDataViewOpts(),
                "magicType": opts.ToolBoxFeatureMagicTypeOpts(type_=['line', 'bar','stack']),
                "restore": opts.ToolBoxFeatureRestoreOpts(),
                "saveAsImage": opts.ToolBoxFeatureSaveAsImageOpts(),
                pos_top= "25%",
            },
        ),
    )
    bar.set_series_opts(
        label_opts=opts.LabelOpts(is_show=False),
    )

    ret_json = bar.dump_options()
    echart_code = json.loads(ret_json)

    out_put = [{"echart_name": "Sales over Years", "echart_code": echart_code}]
    print(out_put)
    </code>


      The output should be formatted as a JSON instance that conforms to the JSON schema below, the JSON is a list of dict,
                        [
                        {"echart_name": "Sales over Years", "echart_code": ret_json}
                        {},
                        {},
                        ].


'''
