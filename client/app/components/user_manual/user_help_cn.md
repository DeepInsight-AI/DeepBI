# DeepBI 使用说明


## 1，配置 API KEY

- [设置]-[API KEY]

### 1.1, 关于 OPENAI API KEY:

- API KEY 需要 OPENAI GPT4 模型的访问权限
- 后续会陆续支持其他大模型，敬请期待

### 1.2, 关于 代理:

- OPENAI API 访问可能需要科学上网,请小伙伴们自行配置代理

- KEY和代理配置完毕后，配置完毕后，点击【连接测试】，显示 测试通过 即可。
测试不通过的情况，请自行检查 API KEY是否可用，以及代理是否配置正确。

![doc_0.png](/static/images/cn/img/doc_0.png)

### 1.3, 关于 DeepInsight API KEY:
- 可以点击 设置key 下面的连接，进行注册，获取API Key.(可以加我们微信群(首页联系我们)，申请免费的Token)

![doc_0.png](/static/images/cn/img/us_key.png)

## 2, 配置 数据源

- 目前支持的数据源有， MySql，Doris, starRocks, PostgreSql, 和 CSV
- 后续会陆续支持更多数据源，如sqlserver，clickhouse，SQLite等，敬请期待

### 2.1, CSV数据源配置:

- 【设置】-【CSV数据源】-【点击上传】- 选择需上传的csv文件即可

![doc.png](/static/images/cn/img/doc_1.png)

### 2.2, [MySql，Doris, starRocks, PostgreSql] 数据源配置:

- 【设置】-【数据源】-【新建一个数据源】
- 选择数据源，填写数据库信息，并保存
- 配置完毕后，点击【连接测试】，显示 连接成功 即可

![doc.png](/static/images/cn/img/doc_2.png)

![doc.png](/static/images/cn/img/doc_3.png)

## 3, 数据分析

### 3.1, [数据分析] - [对话]

#### 3.1.1. 勾选数据（数据源和表）

- 勾选的数据会作为对话数据分析中AI的基础数据

#### 3.1.2, 填写注释，提交AI检测

- 尽量完善表格和字段注释，协助AI更好的理解这些数据，使Agent更好的完成数据分析任务

![doc.png](/static/images/cn/img/doc_4.png)

#### 3.1.3, 修改未通过的注释，再次提交

- AI会反馈未通过的注释，请修改补充后，再次提交，直到所有注释通过检测

![doc.png](/static/images/cn/img/doc_5.png)

#### 3.1.4, 所有注释检测通过后，开始对话

- 🔥 注: 若要生成持久化报表，请使用【报表】-【报表生成】，数据分析中出现的报表均为临时报表，不支持持久化

![doc.png](/static/images/cn/img/doc_6.png)

#### 3.1.5, 重选数据源

- 若要重选数据源，进行新一轮对话，请点击【新对话】，重置当前对话，当前对话记录会存储进【历史对话】中

![doc.png](/static/images/cn/img/doc_7.png)

### 3.2, 【数据分析】-【历史对话】
- 可查看历史对话记录

![doc.png](/static/images/cn/img/doc_8.png)

## 4, 报表 

### 4.1,【报表】-【报表生成】

#### 4.1.1. Check the data (data source and table)

- 勾选的数据会作为报表生成中AI的基础数据
- 🔥注: 目前CSV数据源不支持【报表生成】

#### 4.1.2, 填写注释，提交AI检测

- 尽量完善表格和字段注释，协助AI更好的理解这些数据，使Agent更好的完成报表生成任务

![doc.png](/static/images/cn/img/doc_9.png)

#### 4.1.3, 修改未通过的注释，再次提交

- AI会反馈未通过的注释，请修改补充后，再次提交, 直到所有注释检测通过

![doc.png](/static/images/cn/img/doc_10.png)

#### 4.1.4, 所有注释检测通过后，开始对话生成报表

- 点击【编辑报表】，可直接编辑 新生成的报表.
- 🔥 注: 【报表生成】模块目前仅支持持久化报表生成任务，分析类问题，请使用【数据分析】-【对话】

![doc.png](/static/images/cn/img/doc_11.png)

#### 4.1.5, 重选数据源

- 若要重选数据源，进行新一轮对话，请点击【新对话】，重置当前对话

![doc.png](/static/images/cn/img/doc_12.png)

### 4.2, 【报表】-【报表列表】

#### 4.2.1, 报表状态

- 【报表生成】中新生成的报表会出现在【报表列表】中，此时报表为 草稿 状态，若想要在【仪表盘】展示该报表，请点击 【发布】 按钮，将报表状态变更为 已发布 状态

![doc.png](/static/images/cn/img/doc_13.png)

![doc.png](/static/images/cn/img/doc_14.png)

#### 4.2.2, 修改SQL语句

- 点击【编辑源】，可自定义修改报表的SQL语句.

![doc.png](/static/images/cn/img/doc_15.png)

#### 4.2.3, 修改图表样式

- 点击【编辑可视化】，可自定义编辑 可视化图表样式
- 点击【新增可视化】，可增加可视化图表

![doc.png](/static/images/cn/img/doc_16.png)

![doc.png](/static/images/cn/img/doc_17.png)

#### 4.2.4, 删除报表

- 点击【归档】，可将报表状态变更为 归档（删除）状态

![doc.png](/static/images/cn/img/doc_18.png)

### 5, 仪表盘

#### 5.1, 新建仪表盘

- 【仪表盘】-【新建】- 编辑仪表盘

![doc.png](/static/images/cn/img/doc_19.png)

- 将刚生成的【已发布 】报表，添加进仪表盘

![doc.png](/static/images/cn/img/doc_20.png)

- 点击【发布】仪表盘

![doc.png](/static/images/cn/img/doc_21.png)

#### 5.1.2 分享仪表盘 

- 点击【发布】后，即可分享仪表盘

![doc.png](/static/images/cn/img/doc_22.png)

#### 5.2 仪表盘美化
- 选中一个已经存在的仪表盘
![doc.png](/static/images/cn/img/doc_22_1.png)
- 点击进入
![doc.png](/static/images/cn/img/doc_22_2.png)
- 选择模板，点击应用
![doc.png](/static/images/cn/img/doc_22_3.png)
- 进入美化大屏，等待AI自动转换完成
![doc.png](/static/images/cn/img/doc_22_4.png)
![doc.png](/static/images/cn/img/doc_22_5.png)
- 转换完成
![doc.png](/static/images/cn/img/doc_22_6.png)
- 点击查看大屏
![doc.png](/static/images/cn/img/doc_22_7.png)

## 6, 自动数据分析 

#### 6.1 添加分析任务 
1. 点击自动数据分析
2. 选择自动数据分析
3. 点击下拉框
4. 选择数据源

![doc.png](/static/images/cn/img/doc_23.png)

#### 6.2 确认报告所需表  
1. 选择数据表备注表字段信息
2. 选择报告使用的表，可以选择多个表
3. 备注表字段信息
4. 然后提交

![doc.png](/static/images/cn/img/doc_25.png)

#### 6.3 等待报告完成  
1. 历史分析，查看报告状态

![doc.png](/static/images/cn/img/doc_26.png)

2. 自动更改为生成中
![doc.png](/static/images/cn/img/doc_27.png)

4. 当状态更换为成功时
![doc.png](/static/images/cn/img/doc_28.png)

6. 查看报告详情
![doc.png](/static/images/cn/img/doc_29.png)
