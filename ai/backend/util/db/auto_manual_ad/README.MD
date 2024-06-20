# 自动化

**目录结构**

- SQL目录，用来保存sql文件，预处理生成的csv及最终问答生成的csv文件
- test目录，用来测试环境和一些功能函数
- utils目录，工具类

## 预处理自动化

使用mysql-connector包进行开发，处理SQL目录下的所有后缀为sql的文件，将这些sql执行并将查询得到的结果导出到csv文件中

- `MySQLQueryExecuitor.py`这个文件对mysql的配置，连接，断开连接，执行查询，导出数据的操作进行封装。
- `Preprocessing_data.py`这个文件是`MySQLQueryExecuitor`对象的调用来完成数据的预处理

## 问答自动化

- `AIChat.py`是封装了与模型问答的方法
- `llm_processing.py`会遍历每一个csv文件下的所有md文件，得到答案后，保存在与md文件同目录下的位置
