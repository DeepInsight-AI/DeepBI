EXCEL_ECHART_TIPS_MESS = """Here are some examples of generating mysql and pyecharts Code based on the given question.
 Please generate new one based on the data and question human asks you, import the neccessary libraries and make sure the code is correct.

IMPORTANT: You need to follow the coding style, and the type of the x, y axis.Title and label are not displayed under any circumstances. In either case, the datazoom and scroll legend must be displayed. The datazoom of the x-axis must be left=1, horizontal located below the x-axis, and the datazoom of the y-axis must be right=1, vertical located on the far right side of the container.  The toolbox is only shown in line charts and bar charts. The five function buttons must be located on the left side of the line chart and bar chart according to pop_left=1, pop_top=15%, and vertical. Scroll legends for line and bar charts must be placed above the chart with pop_top=1 and horizontal. The scrolling legends of other charts must be placed vertically on the right side of the chart according to pop_right=1, pop_top=15%, and avoidLabelOverlap should be turned on as much as possible. If the x-axis can be sorted according to certain rules (such as date and time size or value size), please sort by the x-axis, otherwise sort by size.But also need to focus on the column name of the uploaded tables(if exists). Generally, PyEcharts does not accept numpy.int or numpy.float, etc. It only supports built-in data type like int, float, and str.
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
        title_opts=opts.TitleOpts(title="Sales and Profit over Time",is_show=false),
        datazoom_opts=[
            opts.DataZoomOpts(
                is_show=True, type_="slider",
                xaxis_index=[0], range_start=0, range_end=100, orient="horizontal",
                pos_bottom="0px", pos_left="1%", pos_right="1%"
            ),
            opts.DataZoomOpts(
                is_show=True, type_="slider",
                yaxis_index=[0], range_start=0, range_end=100, orient="vertical",
                pos_top="0px", pos_right="1%", pos_bottom="3%"
            ),
        ],
        legend_opts=opts.LegendOpts(
            type_="scroll",  # 设置图例类型为滚动
        ),
        toolbox_opts=opts.ToolboxOpts(
            is_show=true,
            feature={
                "dataZoom": opts.ToolBoxFeatureDataZoomOpts(),
                "dataView": opts.ToolBoxFeatureDataViewOpts(),
                "magicType": opts.ToolBoxFeatureMagicTypeOpts(type_=['line', 'bar','stack']),
                "restore": opts.ToolBoxFeatureRestoreOpts(),
                "saveAsImage": opts.ToolBoxFeatureSaveAsImageOpts(),
            },
        ),
    )
    line.set_series_opts(
        areastyle_opts=opts.AreaStyleOpts(opacity=0.5),
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
        title_opts=opts.TitleOpts(title="Sales over Years",is_show=false),
        datazoom_opts=[
            opts.DataZoomOpts(
                is_show=True, type_="slider",
                xaxis_index=[0], range_start=0, range_end=100, orient="horizontal",
                pos_bottom="0px", pos_left="1%", pos_right="1%"
            ),
            opts.DataZoomOpts(
                is_show=True, type_="slider",
                yaxis_index=[0], range_start=0, range_end=100, orient="vertical",
                pos_top="0px", pos_right="1%", pos_bottom="3%"
            ),
        ],
        legend_opts=opts.LegendOpts(
            type_="scroll",  # 设置图例类型为滚动
        ),
        toolbox_opts=opts.ToolboxOpts(
            is_show=true,
            feature={
                "dataZoom": opts.ToolBoxFeatureDataZoomOpts(),
                "dataView": opts.ToolBoxFeatureDataViewOpts(),
                "magicType": opts.ToolBoxFeatureMagicTypeOpts(type_=['line', 'bar','stack']),
                "restore": opts.ToolBoxFeatureRestoreOpts(),
                "saveAsImage": opts.ToolBoxFeatureSaveAsImageOpts(),
            },
        ),
    )
    # Render the chart
    ret_json = bar.dump_options()
    echart_code = json.loads(ret_json)

    out_put = [{"echart_name": "Sales over Years", "echart_code": echart_code}]
    print(out_put)
    </code>
    When using pie charts, there must be no parameter x
    X axis dataZoom is set to orient: horizontal
    Y-axis dataZoom is set to orient: vertical"
    The output should be formatted as a JSON instance that conforms to the JSON schema below, the JSON is a list of dict,
    [
    {"echart_name": "Sales over Years", "echart_code": ret_json}
    {},
    {},
    ].
    """

CSV_ECHART_TIPS_MESS = """Here are some examples of generating pyecharts Code based on the given question.
 Please generate new one based on the data and question human asks you, import the neccessary libraries and make sure the code is correct.

IMPORTANT: You need to follow the coding style, and the type of the x, y axis.Title and label are not displayed under any circumstances. In either case, the datazoom and scroll legend must be displayed. The datazoom of the x-axis must be left=1, horizontal located below the x-axis, and the datazoom of the y-axis must be right=1, vertical located on the far right side of the container.  The toolbox is only shown in line charts and bar charts. The five function buttons must be located on the left side of the line chart and bar chart according to pop_left=1, pop_top=15%, and vertical. Scroll legends for line and bar charts must be placed above the chart with pop_top=1 and horizontal. The scrolling legends of other charts must be placed vertically on the right side of the chart according to pop_right=1, pop_top=15%, and avoidLabelOverlap should be turned on as much as possible. If the x-axis can be sorted according to certain rules (such as date and time size or value size), please sort by the x-axis, otherwise sort by size.But also need to focus on the column name of the uploaded tables(if exists). Generally, PyEcharts does not accept numpy.int or numpy.float, etc. It only supports built-in data type like int, float, and str.
Pay attention to check whether the query statement in the execution code block can correctly query the data.


    Given the same `company_sales.csv`.
    Q: A line chart comparing sales and profit over time would be useful. Could you help plot it?
    <code>
    import pandas as pd
    from pyecharts.charts import Line
    from pyecharts import options as opts
    import json

    df = pd.read_csv('company_sales.csv')
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
        title_opts=opts.TitleOpts(title="Sales and Profit over Time",is_show=false),
        datazoom_opts=[
            opts.DataZoomOpts(
                is_show=True, type_="slider",
                xaxis_index=[0], range_start=0, range_end=100, orient="horizontal",
                pos_bottom="0px", pos_left="1%", pos_right="1%"
            ),
            opts.DataZoomOpts(
                is_show=True, type_="slider",
                yaxis_index=[0], range_start=0, range_end=100, orient="vertical",
                pos_top="0px", pos_right="1%", pos_bottom="3%"
            ),
        ],
        legend_opts=opts.LegendOpts(
            type_="scroll",  # 设置图例类型为滚动
        ),
        toolbox_opts=opts.ToolboxOpts(
            is_show=true,
            feature={
                "dataZoom": opts.ToolBoxFeatureDataZoomOpts(),
                "dataView": opts.ToolBoxFeatureDataViewOpts(),
                "magicType": opts.ToolBoxFeatureMagicTypeOpts(type_=['line', 'bar','stack']),
                "restore": opts.ToolBoxFeatureRestoreOpts(),
                "saveAsImage": opts.ToolBoxFeatureSaveAsImageOpts(),
            },
        ),
    )
    line.set_series_opts(
        areastyle_opts=opts.AreaStyleOpts(opacity=0.5),
    )
    ret_json = line.dump_options()
    echart_code = json.loads(ret_json)

    out_put = [{"echart_name": "Sales and Profit over Time", "echart_code": echart_code}]
    print(out_put)
    </code>
    When using pie charts, there must be no parameter x
    X axis dataZoom is set to orient: horizontal
    Y-axis dataZoom is set to orient: vertical"

    Given the following data:
    company_sales.csv
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
    df = pd.read_csv('company_sales.csv')
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
        title_opts=opts.TitleOpts(title="Sales over Years",is_show=false),
        datazoom_opts=[
            opts.DataZoomOpts(
                is_show=True,  type_="slider",
                xaxis_index=[0], range_start=0, range_end=100, orient="horizontal",
                pos_bottom="0px", pos_left="1%", pos_right="1%"
            ),
            opts.DataZoomOpts(
                is_show=True,  type_="slider",
                yaxis_index=[0], range_start=0, range_end=100, orient="vertical",
                pos_top="0px", pos_right="1%", pos_bottom="3%"
            ),
        ],
        legend_opts=opts.LegendOpts(
            type_="scroll",  # 设置图例类型为滚动
        ),
        toolbox_opts=opts.ToolboxOpts(
            is_show=true,
            feature={
                "dataZoom": opts.ToolBoxFeatureDataZoomOpts(),
                "dataView": opts.ToolBoxFeatureDataViewOpts(),
                "magicType": opts.ToolBoxFeatureMagicTypeOpts(type_=['line', 'bar','stack']),
                "restore": opts.ToolBoxFeatureRestoreOpts(),
                "saveAsImage": opts.ToolBoxFeatureSaveAsImageOpts(),
            },
        ),
    )
    # Render the chart
    ret_json = bar.dump_options()
    echart_code = json.loads(ret_json)

    out_put = [{"echart_name": "Sales over Years", "echart_code": echart_code}]
    print(out_put)
    </code>
    When using pie charts, there must be no parameter x
    X axis dataZoom is set to orient: horizontal
    Y-axis dataZoom is set to orient: vertical"
    Set one or more dataZoom rooms based on site requirements
    Do not have any output or debug messages in the middle of the code, only output content at the end of the code
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

IMPORTANT: Please remember: you must follow the prescribed coding style and type of x and y axes. Titles and labels should not be displayed under any circumstances and data scaling and scrolling legends must be displayed. The datazoom of the x-axis must be left=1, horizontally located below the x-axis, and the datazoom of the y-axis must be right=1, located vertically at the far right side of the container.The toolbox is only shown in line charts and bar charts.The toolbox is only shown in line charts and bar charts.The five function buttons must be located on the left side of the line chart and bar chart according to pop_left=1, pop_top=15%, and vertical. Scroll legends for line and bar charts must be placed above the chart with pop_top=1 and horizontal. The scrolling legends of other charts must be placed vertically on the right side of the chart according to pop_right=1, pop_top=15%, and avoidLabelOverlap should be turned on as much as possible. If the x-axis can be sorted according to certain rules (such as date and time size or value size), please sort by the x-axis, otherwise sort by size.But also need to focus on the column name of the uploaded tables(if exists). Generally, PyEcharts does not accept numpy.int or numpy.float, etc. It only supports built-in data type like int, float, and str.
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
    line.add_xaxis(year)
    line.add_yaxis("Sales", sales, stack="")
    line.add_yaxis("Profit", profit, stack="")

    line.set_global_opts(
        title_opts=opts.TitleOpts(is_show=False),  # 确保标题不显示
        xaxis_opts=opts.AxisOpts(type_="category", name="Month", boundary_gap=False),
        yaxis_opts=opts.AxisOpts(type_="value", name="Amount", axistick_opts=opts.AxisTickOpts(is_show=True), splitline_opts=opts.SplitLineOpts(is_show=True)),
        legend_opts=opts.LegendOpts(is_show=True, type_="scroll"),  # 显示滚动图例
        datazoom_opts=[
            opts.DataZoomOpts(
                is_show=True,  type_="slider",
                xaxis_index=[0], range_start=0, range_end=100, orient="horizontal",
                pos_bottom="0px", pos_left="1%", pos_right="1%"
            ),
            opts.DataZoomOpts(
                is_show=True,  type_="slider",
                yaxis_index=[0], range_start=0, range_end=100, orient="vertical",
                pos_top="0px", pos_right="1%", pos_bottom="3%"
            ),
        ],
        toolbox_opts=opts.ToolboxOpts(
            is_show=True,
            feature={
                "dataZoom": opts.ToolBoxFeatureDataZoomOpts(),
                "dataView": opts.ToolBoxFeatureDataViewOpts(is_show=True),
                "magicType": opts.ToolBoxFeatureMagicTypeOpts(type_=['line', 'bar', 'stack']),
                "restore": opts.ToolBoxFeatureRestoreOpts(),
                "saveAsImage": opts.ToolBoxFeatureSaveAsImageOpts(),
            },
        ),
    )

    line.set_series_opts(
        areastyle_opts=opts.AreaStyleOpts(opacity=0.5),
        label_opts=opts.LabelOpts(is_show=False)  # 确保标签不显示
    )
    ret_json = line.dump_options()
    echart_code = json.loads(ret_json)

    out_put = [{"echart_name": "Sales and Profit over Time", "echart_code": echart_code}]
    print(out_put)
    </code>
    When using pie charts, there must be no parameter x
    X axis dataZoom is set to orient: horizontal
    Y-axis dataZoom is set to orient: vertical"
    
    Q: Could you help plot a bar chart with the year on the x-axis and the sales on the y-axis?
    <code>
    import pymysql
    import pandas as pd
    from pyecharts.charts import Bar
    from pyecharts import options as opts
    import json

    # 数据库连接配置
    connection = pymysql.connect(
        host='your_host',
        user='your_user',
        password='your_password',
        database='your_database',
        port=3306  # 确保端口号为整数
    )

    # SQL查询语句
    query = "SELECT YEAR(`date`) AS year, SUM(sales) AS sales FROM your_table GROUP BY year"

    # 执行查询并关闭连接
    df = pd.read_sql(query, con=connection)
    connection.close()

    # 数据排序
    df.sort_values('year', inplace=True)

    # 提取年份和销售数据
    years = [str(year) for year in df['year'].tolist()]
    sales = [float(sale) for sale in df['sales'].tolist()]

    # 绘制柱状图
    bar = Bar()
    bar.add_xaxis(years)
    bar.add_yaxis("Sales", sales, label_opts=opts.LabelOpts(is_show=False))
    bar.set_global_opts(
        title_opts=opts.TitleOpts(is_show=False),  # 隐藏标题
        xaxis_opts=opts.AxisOpts(type_="category", name="Year"),
        yaxis_opts=opts.AxisOpts(type_="value", name="Sales"),
        legend_opts=opts.LegendOpts(is_show=True, type_="scroll", pos_top="1%"),
        datazoom_opts=[
            opts.DataZoomOpts(
                is_show=True,  type_="slider",
                xaxis_index=[0], range_start=0, range_end=100, orient="horizontal",
                pos_bottom="0px", pos_left="1%", pos_right="1%"
            ),
            opts.DataZoomOpts(
                is_show=True,  type_="slider",
                yaxis_index=[0], range_start=0, range_end=100, orient="vertical",
                pos_top="0px", pos_right="1%", pos_bottom="3%"
            ),
        ],
        toolbox_opts=opts.ToolboxOpts(
            is_show=True, orient='vertical', pos_left="1%", pos_top="15%",
            feature={
                "dataZoom": opts.ToolBoxFeatureDataZoomOpts(),
                "dataView": opts.ToolBoxFeatureDataViewOpts(is_show=True),
                "magicType": opts.ToolBoxFeatureMagicTypeOpts(type_=['line', 'bar', 'stack']),
                "restore": opts.ToolBoxFeatureRestoreOpts(),
                "saveAsImage": opts.ToolBoxFeatureSaveAsImageOpts(),
            },
        ),
    )

    # 生成并输出图表配置
    ret_json = bar.dump_options()
    echart_code = json.loads(ret_json)

    output = [{"echart_name": "Sales over Years", "echart_code": echart_code}]
    print(output)
    </code>
    When using pie charts, there must be no parameter x
    X axis dataZoom is set to orient: horizontal
    Y-axis dataZoom is set to orient: vertical"
    
    Q:Create a machine learning model to predict future sales and plot historical and forecasted sales figures
    note:For such prediction problems based on machine learning, the front-end page can only be displayed based on json code. If visualization is required, be sure to package the data into json code and return it!
    <code>
    import numpy as np
    import matplotlib.pyplot as plt
    import pandas as pd
    import pymysql
    from sklearn.linear_model import LinearRegression
    from sklearn.model_selection import train_test_split
    import json
    from io import BytesIO
    import base64
    # Database connection
    connection = pymysql.connect(
        host='your_host',
        user='your_user',
        password='your_password',
        database='your_database',
        port=3306
    )
    # SQL query
    query = "SELECT YEAR(`date`) AS year, SUM(sales) AS sales FROM your_table GROUP BY year"
    # Retrieve data
    df = pd.read_sql(query, con=connection)
    connection.close()

    # Prepare data for machine learning
    df['year'] = pd.to_numeric(df['year'])
    X = df[['year']]  # Features
    y = df['sales']  # Target variable
    # Train-test split
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Fit linear regression model
    model = LinearRegression()
    model.fit(X_train, y_train)

    # Predict future sales
    future_years = np.array(range(df['year'].max() + 1, df['year'].max() + 6)).reshape(-1, 1)
    predicted_sales = model.predict(future_years)

    # Plotting historical and predicted sales
    plt.figure(figsize=(10, 6))
    plt.scatter(X, y, color='blue', label='Historical Sales')
    plt.plot(X, model.predict(X), color='red', label='Model Fit')
    plt.scatter(future_years, predicted_sales, color='green', label='Predicted Sales')
    plt.xlabel('Year')
    plt.ylabel('Sales')
    plt.title('Sales Prediction')
    plt.legend()
    # Convert plot to JSON
    buf = BytesIO()
    plt.savefig(buf, format='png')
    buf.seek(0)
    image_base64 = base64.b64encode(buf.read()).decode('utf-8')
    plot_json = json.dumps({'image_base64': image_base64})
    plt.close()

    # Output
    output = [{"echart_name": "Sales forecast chart","plot_data": plot_json}]
    print(output)
    </code>
    When using pie charts, there must be no parameter x
    X axis dataZoom is set to orient: horizontal
    Y-axis dataZoom is set to orient: vertical"
    
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

IMPORTANT: You need to follow the coding style, and the type of the x, y axis.Title and label are not displayed under any circumstances. In either case, the datazoom and scroll legend must be displayed. The datazoom of the x-axis must be left=1, horizontal located below the x-axis, and the datazoom of the y-axis must be right=1, vertical located on the far right side of the container.  The toolbox is only shown in line charts and bar charts. The five function buttons must be located on the left side of the line chart and bar chart according to pop_left=1, pop_top=15%, and vertical. Scroll legends for line and bar charts must be placed above the chart with pop_top=1 and horizontal. The scrolling legends of other charts must be placed vertically on the right side of the chart according to pop_right=1, pop_top=15%, and avoidLabelOverlap should be turned on as much as possible. If the x-axis can be sorted according to certain rules (such as date and time size or value size), please sort by the x-axis, otherwise sort by size.But also need to focus on the column name of the uploaded tables(if exists). Generally, PyEcharts does not accept numpy.int or numpy.float, etc. It only supports built-in data type like int, float, and str.
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
            boundary_gap=false
        ),
        yaxis_opts=opts.AxisOpts(
            type_="value",
            name="Amount",
            axistick_opts=opts.AxisTickOpts(is_show=true),
            splitline_opts=opts.SplitLineOpts(is_show=true),
        ),
        title_opts=opts.TitleOpts(title="Sales and Profit over Time",is_show=false),
        datazoom_opts=[
            opts.DataZoomOpts(
                is_show=True,  type_="slider",
                xaxis_index=[0], range_start=0, range_end=100, orient="horizontal",
                pos_bottom="0px", pos_left="1%", pos_right="1%"
            ),
            opts.DataZoomOpts(
                is_show=True,  type_="slider",
                yaxis_index=[0], range_start=0, range_end=100, orient="vertical",
                pos_top="0px", pos_right="1%", pos_bottom="3%"
            ),
        ],
        legend_opts=opts.LegendOpts(
            type_="scroll",  # 设置图例类型为滚动
        ),
        toolbox_opts=opts.ToolboxOpts(
            is_show=true,
            feature={
                "dataZoom": opts.ToolBoxFeatureDataZoomOpts(),
                "dataView": opts.ToolBoxFeatureDataViewOpts(),
                "magicType": opts.ToolBoxFeatureMagicTypeOpts(type_=['line', 'bar','stack']),
                "restore": opts.ToolBoxFeatureRestoreOpts(),
                "saveAsImage": opts.ToolBoxFeatureSaveAsImageOpts(),
            },
        ),
    )

    line.set_series_opts(
        areastyle_opts=opts.AreaStyleOpts(opacity=0.5),
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
        title_opts=opts.TitleOpts(title="Sales over Years",is_show=false),
        datazoom_opts=[
            opts.DataZoomOpts(
                is_show=True,  type_="slider",
                xaxis_index=[0], range_start=0, range_end=100, orient="horizontal",
                pos_bottom="0px", pos_left="1%", pos_right="1%"
            ),
            opts.DataZoomOpts(
                is_show=True,  type_="slider",
                yaxis_index=[0], range_start=0, range_end=100, orient="vertical",
                pos_top="0px", pos_right="1%", pos_bottom="3%"
            ),
        ],
        legend_opts=opts.LegendOpts(
            type_="scroll",  # 设置图例类型为滚动
        ),
        toolbox_opts=opts.ToolboxOpts(
            is_show=true,
            feature={
                "dataZoom": opts.ToolBoxFeatureDataZoomOpts(),
                "dataView": opts.ToolBoxFeatureDataViewOpts(),
                "magicType": opts.ToolBoxFeatureMagicTypeOpts(type_=['line', 'bar','stack']),
                "restore": opts.ToolBoxFeatureRestoreOpts(),
                "saveAsImage": opts.ToolBoxFeatureSaveAsImageOpts(),
            },
        ),
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

IMPORTANT: You need to follow the coding style, and the type of the x, y axis.Title and label are not displayed under any circumstances. In either case, the datazoom and scroll legend must be displayed. The datazoom of the x-axis must be left=1, horizontal located below the x-axis, and the datazoom of the y-axis must be right=1, vertical located on the far right side of the container.  The toolbox is only shown in line charts and bar charts. The five function buttons must be located on the left side of the line chart and bar chart according to pop_left=1, pop_top=15%, and vertical. Scroll legends for line and bar charts must be placed above the chart with pop_top=1 and horizontal. The scrolling legends of other charts must be placed vertically on the right side of the chart according to pop_right=1, pop_top=15%, and avoidLabelOverlap should be turned on as much as possible. If the x-axis can be sorted according to certain rules (such as date and time size or value size), please sort by the x-axis, otherwise sort by size.But also need to focus on the column name of the uploaded tables(if exists). Generally, PyEcharts does not accept numpy.int or numpy.float, etc. It only supports built-in data type like int, float, and str.
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
        title_opts=opts.TitleOpts(title="Sales over Years",is_show=false),
        datazoom_opts=[
            opts.DataZoomOpts(
                is_show=True,  type_="slider",
                xaxis_index=[0], range_start=0, range_end=100, orient="horizontal",
                pos_bottom="0px", pos_left="1%", pos_right="1%"
            ),
            opts.DataZoomOpts(
                is_show=True,  type_="slider",
                yaxis_index=[0], range_start=0, range_end=100, orient="vertical",
                pos_top="0px", pos_right="1%", pos_bottom="3%"
            ),
        ],
        legend_opts=opts.LegendOpts(
            type_="scroll",  # 设置图例类型为滚动
        ),
        toolbox_opts=opts.ToolboxOpts(
            is_show=true,
            feature={
                "dataZoom": opts.ToolBoxFeatureDataZoomOpts(),
                "dataView": opts.ToolBoxFeatureDataViewOpts(),
                "magicType": opts.ToolBoxFeatureMagicTypeOpts(type_=['line', 'bar','stack']),
                "restore": opts.ToolBoxFeatureRestoreOpts(),
                "saveAsImage": opts.ToolBoxFeatureSaveAsImageOpts(),
            },
        ),
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
