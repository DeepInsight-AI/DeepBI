<h1 align="center">DeepBI</h1>

<div align="center">

DeepBI is an AI-native data analysis platform. DeepBI leverages the power of large language models to explore, query, visualize, and share data from any data source. Users can use DeepBI to gain data insight and make data-driven decisions.


</div>

<div align="center">

  Languages： English [中文](README_CN.md)<br>
Developer：dev@deepbi.com  Business：hi@deepbi.com

  <div style="display: flex; align-items: center;">

If you think DeepBI is helpful to you, please help by clicking <a style="display: flex; align-items: center;margin:0px 6px" target="_blank" href='https://github.com/DeepInsight-AI/DeepBI'>here</a> on the ⭐ Star and Fork in the upper right corner. Your support is the greatest driving force for DeepBI to become better.


  </div>
</div>



## Video example

https://github.com/DeepInsight-AI/DeepBI/assets/151519374/d1effbe1-5c11-4c77-86ef-e01b1ea7f2f6


## user manual
[DeepBI user manual](client/app/assets/images/en/user_manual_en.md)


## ✨ Features

1 Conversational data analysis: Users can get arbitrary data results and analysis results through dialogues.\
2 Conversational query generation: Generates persistent queries and visualizations through dialogues.\
3 Dashboard : Assemble persistent visualizations into dashboards.\
4 Automated data analysis reports (to be developed) : Complete data analysis reports automatically according to user instructions.\
5 Support multiple data sources, including MySQL, PostgreSQL, Doris, StarRocks, CSV/Excel, etc.\
6 Multi-platform support, support Windows-WSL,Windows, Linux, Mac. \
7 International, support Chinese, English.


## 🚀 Supported Databases

The database connections supported by DeepBI are:
- MySQL
- PostgreSQL
- csv/Excel Import
- Doris
- StarRocks
- MongoDB


## 📦 Windows exe installation
- Download ```window_install_exe_EN.zip``` from the <a href="https://github.com/DeepInsight-AI/DeepBI/releases">tag list</a>.The current test supports Win10 and Win11
- Unzip the zip package and double-click the.exe file to run DeepBI
- Local installation instructions [Installl exe](README_window_en.md)

## 📦 Docker build

- The local environment needs to have docker and docker-compose. <br>
- [Installl docker](Docker_install.md)
- Download project files by git:``` git clone https://github.com/DeepInsight-AI/DeepBI.git ``` <br>
  or drirect download zip file, unzip it. <br>
  ![download.png](user_manual/cn/img/download.png)

- Enter the project directory:``` cd DeepBI ```
- Just run``` ./Install.sh ```directly
- Default port: 8338 8339
- Web access: http://ip:8338
#### DeepBI docker command
- Enter project DeepBI dir:
```
    docker-compose start # start DeepBI servie
    docker-compose stop # stop DeepBI servie
    docker-compose ps # see DeepBI servie states
```
- If it appears... PermissionError ... ' or ' Permission denied', please add 'sudo' before executing the command
```
    sudo docker-compose start # start DeepBI servie
    sudo docker-compose stop # stop DeepBI servie
    sudo docker-compose ps # see DeepBI servie states
```

## Ubuntu build
Install directly on the ubuntu system, you need to install redis, postgresql python3.8.17 environment.

- Redis can be accessed directly through the 127.0.0.1 password-free command line.
- Require python version  3.8.x
- Recommend using virtual environments such as pyenv coda
- postgresql needs to install postgresql-16 version

- Download the DeepBI code by the command 

```
git clone https://github.com/DeepInsight-AI/DeepBI.git
```
If the download fails to replace the protocol, run the following code
```
git clone http://github.com/DeepInsight-AI/DeepBI.git
 ```

- Just run ```. ubuntu_install.sh``` directly (note that you run . ubuntu_install.sh instead of sh xxx here, because you need to run the python virtual environment)
- Default port is 8338 and 8339
- Web access: http://ip:8338





## Contact Us
<a><img src="https://github.com/user-attachments/assets/ead686d5-c0fd-4b12-b7be-825149eb4cdb" width="40%"/></a>





## 📑 Other
- We have tested on Mac OS 12.7/13.X /14.1.1, Ubuntu 20.04/22.04, and Windows11 WSL 22.04.
- Windows 10 requires version 22H2 or higher to install WSL
- The minimum memory requirement for server operation is 1 core 2G memory, and 2 core 4G memory is recommended
- If you have any question, please contact us at dev@deepbi.com
- <a href="https://github.com/DeepInsight-AI/DeepBI/issues">Issue</a>

