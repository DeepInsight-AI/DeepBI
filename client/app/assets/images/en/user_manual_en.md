<h1 align="center">DeepBI User Manual</h1>

<div align="center">

</div>

<div align="center">

Languages： [中文](../cn/user_manual_cn.md) English


</div>


<h2 align="left">1，Configure API KEY</h2>

- [Setting]-[API KEY]

### 1.1, About OPENAI API KEY:

- Requires access to gpt4 models.
- You can apply for our Key to use

### 1.2, About the proxy:

- OPENAI API access may require proxy configuration, please configure the proxy yourself.

- After the KEY and proxy are configured, click [Connection Test] and "Test success" will be displayed. If the test
  fails, please check whether the API KEY is available and whether the proxy is configured correctly.

![doc_0.png](img/doc_0.png)

### 1.3, About DeepInsight API KEY:
- You can click the connection below the set key to register and get the API Key.(You can add our wechat group (contact us on the home page) to apply for a free Token.)

![doc_0.png](img/us_key.png)

<h2 align="left">2,Configure Data Source</h2>

- Currently supported data sources are MySql, Doris, starRocks, PostgreSql, and CSV. More data sources will be supported
  in the future, such as sqlserver, clickhouse, SQLite, etc., so stay tuned.

### 2.1, CSV data source configure:

- [Setting]-[CSV Data Source]-[Click to upload]-select the csv file to be uploaded

![doc.png](img/doc_1.png)

### 2.2, [MySql, Doris, starRocks, PostgreSql] data source configure:

- [Setting]-[Data Sources]-[New Data Source]
- Select the data source, fill in the database information, and save. After the configuration is completed,
  click [Test Connection ] and "Connection Success" will be displayed.

![doc.png](img/doc_2.png)

![doc.png](img/doc_3.png)

<h2 align="left">3,Chat Builder</h2>

<h3 align="left">3.1, [Chat Builder] - [Dialogue]</h3>

#### 3.1.1. Check the data (data source and table)

- The checked data will be used as the basic data for AI in conversation data analysis.

#### 3.1.2, fill in the comments and submit for AI detection

- Try to improve the form and field annotations as much as possible to help AI better understand the data and enable the
  Agent to better complete the data analysis task.

![doc.png](img/doc_4.png)

#### 3.1.3, modify the failed comments and submit again

- AI will feedback the comments that have not passed. Please revise and add them and submit again until all the comments
  pass.

![doc.png](img/doc_5.png)

#### 3.1.4, after all annotations pass the detection, start the conversation

- 🔥 Note: If you want to generate a persistent report, please use [Query Builder]-[Report Generation]. The reports that
  appear in [Chat Builder] are temporary reports and do not support persistence.

![doc.png](img/doc_6.png)

#### 3.1.5, reselect data source

- If you want to reselect the data source and start a new round of dialogue, please click [New Dialogue] to reset the
  current conversation. The current conversation record will be stored in [History Dialogue].

![doc.png](img/doc_7.png)

<h3 align="left">3.2, [Chat Builder] - [History Dialogue]</h3>
Can view historical conversation records

![doc.png](img/doc_8.png)

<h2 align="left">4, Query Builder </h2>

<h3 align="left">4.1, [Query Builder]-[Report Generation]</h3>

#### 4.1.1. Check the data (data source and table)

- The checked data will be used as the basic data of AI in report generation.
- 🔥Note: Currently the CSV data source does
  not support [Report Generation]

#### 4.1.2, fill in the comments and submit for AI detection

- Try to improve the form and field annotations as much as possible to help AI better understand the data and enable the
  Agent to better complete the report generation task.

![doc.png](img/doc_9.png)

#### 4.1.3, modify the failed comments and submit again

- AI will feedback the annotations that have failed. Please revise and add them and submit again until all annotations
  pass the test.

![doc.png](img/doc_10.png)

#### 4.1.4, after all annotations pass the detection, start the dialogue to generate reports

- Click [Edit Report] to directly edit the newly generated report.
- 🔥 Note: The [Report Generation] module currently only supports persistent report generation tasks. For analysis
  questions, please use [Chat Builder] - [Dialogue].

![doc.png](img/doc_11.png)

#### 4.1.5, reselect data source

- If you want to reselect the data source and start a new round of dialogue, please click [New Dialogue] to reset the current dialogue.

![doc.png](img/doc_12.png)

<h3 align="left">4.2, [Query]-[Report List]</h3>

#### 4.2.1, Report status

- The newly generated report in [Report Generation] will appear in the [Report List]. At this time, the report is in
  draft status. If you want to display the report in the [Dashboards], please click the [Publish] button, change report
  status to published status.

![doc.png](img/doc_13.png)

![doc.png](img/doc_14.png)

#### 4.2.2, Modify SQL statement

- Click [Edit Source] to customize the SQL statement of the report.

![doc.png](img/doc_15.png)

#### 4.2.3, modify chart style

- Click [Edit Visualization] to customize and edit the visualization chart style.
- Click [Add Visualization] to add a visual chart.

![doc.png](img/doc_16.png)

![doc.png](img/doc_17.png)

#### 4.2.4. Deleting a report

- Click [Archive] to change the report status to archive (delete) status.

![doc.png](img/doc_18.png)

<h3 align="left">5, Dashboards</h3>

<h4 align="left">5.1, create a new dashboard</h4>

- [Dashboards]-[Create]-Edit Dashboard

![doc.png](img/doc_19.png)

- Add the newly generated Published report to the dashboard

![doc.png](img/doc_20.png)

- Click [Publish] Dashboard

![doc.png](img/doc_21.png)

<h4 align="left">5.1.2 Share Dashboard </h4>

- After clicking [Publish], you can share the dashboard

![doc.png](img/doc_22.png)

<h4 align="left">5.2 Dashboards Prettify </h4>
- Select an existing dashboard
![doc.png](img/doc_22_1.png)
- Click to enter
![doc.png](img/doc_22_2.png)
- Select the template and click Apply
![doc.png](img/doc_22_3.png)
- Enter the beautification screen and wait for the AI automatic conversion to complete
![doc.png](img/doc_22_4.png)
![doc.png](img/doc_22_5.png)
- Conversion complete
![doc.png](img/doc_22_6.png)
- Click to view large screen
![doc.png](img/doc_22_7.png)



<h2 align="left">6. Automatic Data Analysis</h2>

<h4 align="left">6.1 Add Analysis Task</h4>
1. Click on Automatic Data Analysis.
2. Choose Automatic Data Analysis.
3. Click on the dropdown.
4. Select the data source.

![doc.png](img/doc_23.png)

<h4 align="left">6.2 Confirm Required Tables for the Report</h4>
1. Select table remarks and field information.
2. Choose the tables to be used in the report; multiple tables can be selected.
3. Provide remarks for table fields.
4. Submit.

![doc.png](img/doc_25.png)

<h4 align="left">6.3 Wait for Report Completion</h4>
1. In historical analysis, check the report status.

![doc.png](img/doc_26.png)
2. Automatically changes to generating.

![doc.png](img/doc_27.png)
3. When the status changes to successful.

![doc.png](img/doc_28.png)
4. View the report details.

![doc.png](img/doc_29.png)



