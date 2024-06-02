# 自动化

## 配置环境

使用`vscode`，`conda`,`python-3.10`,在windows上开发。

### 在win上安装`conda`

```python
在vscode中需要先执行 conda init
conda create -n my_env python=3.10
conda activate my_env
pip install pandas
pip install mysql-connector
pip install openpyxl
pip install pymysql

conda remove -n env_name --all
conda env remove -n env_name


conda create -n torch --clone py3  	# 将 py3 重命名为 torch


conda deactivate

# 需要在vscode中指定解释器
```

### git的配置

```
 git clone https://github.com/DeepInsight-AI/DeepBI.git
   3 git branch -r
   4 git remote show origin
   5 dir
   6 cd .\DeepBI\
   7 git branch -r
   8 git fetch test_amazon_fun
   9 git branch -r
  10 git fetch  origin/test_amazon_fun
  11 git fetch origin
  12 git checkout origin/test_amazon_fun
  ```

## 数据库自动化

> 需求：需要提取出来某一部分的数据（自动化掉）

需要使用python模拟出一些具有模板，固定的操作，可能涉及到`mysql`的连接,`sql`的编写和执行，将查询出来的数据保存到`csv`
文件（涉及到文件的命名）

- 数据库连接自动化
- 数据库查询自动化
- 导出`csv`文件自动化，指定编码格式为`utf-8`

## `DeepBI`自动化

### 准备工作

> 需求：~~需要人为的将`csv`文件上传到`deepBI`的固定位置,上传成功之后，可能还需要配置相关的`API`
key,配置完成之后，将开始BI的数据辅助分析~~

>
> 使用后台的接口来进行上传文件，配置和辅助数据分析

### 提问环节

> 需求：问答失败(可以分析黑窗口中的代码执行结果，如果检查出哪一步出现执行失败，出现异常)，可能涉及到多次问答；
>
> 问答成功（怎样区分答案的有效和无效）保存，保存相应问题、答案、表格，需要指定保存的模板。

### 提问结束

> 释放资源

## 亚马逊的后台的动态调整

> 可能一些指标涉及到通过算法的选优，从而进行策略的调整。
