<h1 align="center">Holmes</h1>

<div align="center">

Holmes is an AI-native data analysis platform. Holmes leverages the power of large language models to explore, query, visualize, and share data from any data source. Users can use Holmes to gain data insight and make data-driven decisions.


</div>

<div align="center">

  Languages： English [中文](README_CN.md)<br>
Developer：dev@deep-thought.io  Business：hi@deep-thought.io Twitter:https://twitter.com/DeepThoughAI

  <div style="display: flex; align-items: center;">

If you think Holmes is helpful to you, please help by clicking <a style="display: flex; align-items: center;margin:0px 6px" target="_blank" href='https://github.com/DeepThought-AI/Holmes'>here</a> on the ⭐ Star and Fork in the upper right corner. Your support is the greatest driving force for Holmes to become better.


  </div>
</div>



## Video example

https://github.com/DeepThought-AI/Holmes/assets/151519374/d1effbe1-5c11-4c77-86ef-e01b1ea7f2f6


## User manual
[Holmes user manual](client/app/assets/images/en/user_manual_en.md)


## ✨ Features

1 Conversational data analysis: Users can get arbitrary data results and analysis results through dialogues.\
2 Conversational query generation: Generates persistent queries and visualizations through dialogues.\
3 Dashboard : Assemble persistent visualizations into dashboards.\
4 Automated data analysis reports (to be developed) : Complete data analysis reports automatically according to user instructions.\
5 Support multiple data sources, including MySQL, PostgreSQL, Doris, StarRocks, CSV/Excel, etc.\
6 Multi-platform support, support Windows-WSL, Linux, Mac. \
7 International, support Chinese, English.


## 🚀 Supported Databases

The database connections supported by Holmes are:
- MySQL
- PostgreSQL
- csv/Excel Import

## 📦 Docker build

Follow the steps corresponding to your system in "[Install with Docker](Docker_install.md)" , and finally obtain the Holmes URL, which can be accessed through a browser


## Ubuntu build
Install directly on the ubuntu system, you need to install redis, postgresql python3.8.17 environment.

- Redis can be accessed directly through the 127.0.0.1 password-free command line.
- Require python version  3.8+
- Recommend using virtual environments such as pyenv coda
- postgresql needs to install postgresql-16 version
- Just run ```. ubuntu_install.sh``` directly (note that you run . ubuntu_install.sh instead of sh xxx here, because you need to run the python virtual environment)
- Default port is 8338 and 8339
- Web access: http://ip:8338


## Holmes command
- Enter project Holmes dir:
```
    docker-compose start # start Holmes servie
    docker-compose stop # stop Holmes servie
    docker-compose ps # see Holmes servie states
```
- If it appears... PermissionError ... ' or ' Permission denied', please add 'sudo' before executing the command
```
    sudo docker-compose start # start Holmes servie
    sudo docker-compose stop # stop Holmes servie
    sudo docker-compose ps # see Holmes servie states
```
## Running hardware recommendation
- 2 core 4G memory

## Contact Us
 <img src="http://www.deep-thought.io/wechat.png?2" width="30%">

## 📑 Other
- At present, the recommended environment is Ubuntu, other environments have not had time to test, the next step will be gradually improved, windows10 WSL window11 WSL, MacOS 13.x, MacOS 14.x, Ubuntu 20.04 22.04 are no problem
- <br>If you have any question, please contact us at dev@deep-thought.io
- <a href="https://github.com/DeepThought-AI/Holmes/issues">Issue</a>

