<h1 align="center">DeepBI</h1>

<div align="center">

DeepBi is an AI-native data analysis platform. DeepBi leverages the power of large language models to explore, query, visualize, and share data from any data source. Users can use DeepBi to gain data insight and make data-driven decisions.


</div>

<div align="center">

  Languages： English [中文](README_CN.md)<br>
Developer：dev@deep-insight.co  Business：hi@deep-insight.co

  <div style="display: flex; align-items: center;">

If you think DeepBi is helpful to you, please help by clicking <a style="display: flex; align-items: center;margin:0px 6px" target="_blank" href='https://github.com/DeepThought-AI/DeepBi'>here</a> on the ⭐ Star and Fork in the upper right corner. Your support is the greatest driving force for DeepBi to become better.


  </div>
</div>



## Video example

https://github.com/DeepThought-AI/DeepBi/assets/151519374/d1effbe1-5c11-4c77-86ef-e01b1ea7f2f6


## user manual
[DeepBi user manual](client/app/assets/images/en/user_manual_en.md)


## ✨ Features

1 Conversational data analysis: Users can get arbitrary data results and analysis results through dialogues.\
2 Conversational query generation: Generates persistent queries and visualizations through dialogues.\
3 Dashboard : Assemble persistent visualizations into dashboards.\
4 Automated data analysis reports (to be developed) : Complete data analysis reports automatically according to user instructions.\
5 Support multiple data sources, including MySQL, PostgreSQL, Doris, StarRocks, CSV/Excel, etc.\
6 Multi-platform support, support Windows-WSL, Linux, Mac. \
7 International, support Chinese, English.


## 🚀 Supported Databases

The database connections supported by DeepBi are:
- MySQL
- PostgreSQL
- csv/Excel Import

## 📦 Docker build

- The local environment needs to have docker and docker-compose. <br>
- [Installl docker](Docker_install.md)
- Download project files by git:``` git clone https://github.com/DeepThought-AI/DeepBi.git ``` <br>
  or drirect download zip file, unzip it. <br>
  ![download.png](user_manual/cn/img/download.png)

- Enter the project directory:``` cd DeepBi ```
- Just run``` ./Install.sh ```directly
- Default port: 8338 8339
- Web access: http://ip:8338


## Ubuntu build
Install directly on the ubuntu system, you need to install redis, postgresql python3.8.17 environment.

- Redis can be accessed directly through the 127.0.0.1 password-free command line.
- Require python version  3.8+
- Recommend using virtual environments such as pyenv coda
- postgresql needs to install postgresql-16 version
- Just run ```. ubuntu_install.sh``` directly (note that you run . ubuntu_install.sh instead of sh xxx here, because you need to run the python virtual environment)
- Default port is 8338 and 8339
- Web access: http://ip:8338


## DeepBi command
- Enter project DeepBi dir:
```
    docker-compose start # start DeepBi servie
    docker-compose stop # stop DeepBi servie
    docker-compose ps # see DeepBi servie states
```
- If it appears... PermissionError ... ' or ' Permission denied', please add 'sudo' before executing the command
```
    sudo docker-compose start # start DeepBi servie
    sudo docker-compose stop # stop DeepBi servie
    sudo docker-compose ps # see DeepBi servie states
```


## 📑 Other
- We have tested on Mac OS 12.7/13.X /14.1.1, Ubuntu 20.04/22.04, and Windows11 WSL 22.04. <br>If you have any question, please contact us at dev@deep-insight.co
- <a href="https://github.com/DeepThought-AI/DeepBi/issues">Issue</a>

