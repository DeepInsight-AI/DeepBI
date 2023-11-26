<h1 align="center">Holmes</h1>

<div align="center">

Holmes is an AI-native data analysis platform. Holmes leverages the power of large language models to explore, query, visualize, and share data from any data source. Users can use Holmes to gain data insight and make data-driven decisions.


</div>

<div align="center">

  Languages： English [中文](中文说明.md)<br>
Developer Connect：dev@deep-thought.io Business connect：hi@deep-thought.io

  <div style="display: flex; align-items: center;">

If you think Holmes is helpful to you, please help by clicking <a style="display: flex; align-items: center;margin:0px 6px" target="_blank" href='https://github.com/Deep-thoughtIO/Holmes'>here</a> on the ⭐ Star and Fork in the upper right corner. Your support is the greatest driving force for Holmes to become better.


  </div>
</div>



## Video example

https://github.com/Deep-thoughtIO/Holmes/assets/151519374/d1effbe1-5c11-4c77-86ef-e01b1ea7f2f6


## user manual
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

```bash
    The local environment needs to have docker and docker-compose. How to installl docker :https://github.com/DeepThought-AI/Holmes/blob/main/InstallDocker.md
    Just run ./Install.sh directly
    Default port is 8338 and 8339
    Web access: http://ip:8338
```
## Docker command
    entrance project Holmes dir:
    docker-compose start # start Holmes servie
    docker-compose stop # stop Holmes servie
    docker-compose ps # see Holmes servie states

## Ubuntu build
Install directly on the ubuntu system, you need to install redis, postgresql python3.8.17 environment
If there is a need for the above environment locally

- Redis can be accessed directly through the 127.0.0.1 password-free command line.
- Python version requirements 3.8+ Recommend using virtual environments such as pyenv coda
postgresql needs to install postgresql-16 version
- Just run . ubuntu_install.sh directly (note that you run . ubuntu_CN_install.sh instead of sh xxx here, because you need to run the python virtual environment)
- Default port is 8338 and 8339
- Web access: http://ip:8338


## 📑 Other

- <a href="https://github.com/Deep-thoughtIO/Holmes/issues">Issue</a>

