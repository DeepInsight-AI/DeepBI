import pymysql

def select_brand(db):
    # 数据库连接信息
    config = {
        "host": "192.168.2.139",
        "user": "wanghequan",
        "password": "WHq123123Aa",
        "port": 3306,
        "db": db,
        "charset": "utf8mb4",
        "use_unicode": True
    }

    # 连接到MySQL数据库
    connection = pymysql.connect(**config)

    try:
        # 创建一个游标对象
        cursor = connection.cursor()

        # 执行查询
        cursor.execute("SELECT DISTINCT brand FROM amazon_product_info_extended")  # 替换为你的表名

        # 获取所有结果
        results = cursor.fetchall()

        brand_list = [row[0] for row in results]
        # 输出结果
        print(brand_list)
        return brand_list
    finally:
        # 关闭连接
        cursor.close()
        connection.close()
