MYSQL_MATPLOTLIB_TIPS_MESS = """
Here are some examples of generating mysql and matplotlib Code based on the given question. 

IMPORTANT: You need to follow the coding style, and the type of the x, y axis. 

When using Matplotlib to save pictures, be careful to introduce the following code to avoid exceptions:
import matplotlib as mpl
mpl.use('Agg')

Q: Please give me a bar chart of the top 10 sales of subcategory products
<code>
import pandas as pd
import matplotlib.pyplot as plt
import pymysql
import matplotlib as mpl
mpl.use('Agg')

connection = pymysql.connect(
        host='your_host',
        user='your_user',
        password='your_password',
        database='your_database',
        port="your_port"
    )


sql = "SELECT sub_category, sum(quantity) as total_qty FROM order_details group by sub_category order by total_qty desc limit 10;"
data = pd.read_sql_query(sql, conn)

fig, ax = plt.subplots()
ax.bar(data['sub_category'], data['total_qty'])
plt.title('Top 10 Subcategory by Sales Volume')
plt.xlabel('Subcategory')
plt.ylabel('Sales Volume')
plt.xticks(rotation=45)
plt.grid(True)
plt.savefig("top10_subcategory.jpg")

print('[{"img_name": "top10_subcategory.jpg", "description": "Top 10 Subcategory by Sales Volume"}]')
</code>

            
The output should be formatted as a JSON instance that conforms to the JSON schema below, the JSON is a list of dict,
[
{"img_name": "report.jpg", "description":"description of the img"},
{},
{},
].
report_name is generally the name of the chart or report, and supports Chinese. If a name is specified in the question, the given name is used.
"""
