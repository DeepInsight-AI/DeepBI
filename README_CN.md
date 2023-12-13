<h1 align="center">Holmes</h1>

<div align="center">

Holmes是一款AI原生的数据分析平台。Holmes充分利用大语言模型的能力来探索、查询、可视化和共享来自任何数据源的数据。用户可以使用Holmes洞察数据并做出数据驱动的决策。


</div>




<div align="center">

  Languages： 中文 [English](README.md)<br>
 开发：dev@deep-thought.io，工作及商务：hi@deep-thought.io

  <div style="display: flex; align-items: center;">
    如果觉得 Holmes 对您有帮助的话，请帮忙<a style="display: flex; align-items: center;margin:0px 6px" target="_blank" href='https://github.com/DeepThought-AI/Holmes'></a>
    的右上角点个⭐ Star 和 Fork，您的支持是 Holmes 变得更好最大的动力
  </div>
</div>



## 案例视频

https://github.com/DeepThought-AI/Holmes/assets/151519374/f0d2fcd4-32b0-4095-a892-b9bbf8a51602

## 使用说明
[Holmes 使用说明](client/app/assets/images/cn/user_manual_cn.md)


## ✨ 特性

1 对话式数据分析: 用户可以通过对话，得到任意的数据结果和分析结果。\
2 对话式报表生成：通过对话生成持久化的报表和可视化图形。\
3 仪表板大屏：将持久化的可视化图组装为仪表板。\
4 自动化数据分析报告（待开发)：根据用户指令自动完成完整的数据分析报告。\
5 多数据源支持，支持 MySQL、PostgreSQL、Doris，Starrocks, CSV/Excel等。\
6 多平台支持，支持 Windows-WSL、Linux、Mac。\
7 国际化，支持中文、英文。


## 🚀 支持的数据库

Holmes 支持的数据库连接有:
- MySQL
- PostgreSQL
- csv/Excel导入

## 📦 Docker 安装部署

按照 [使用 Docker 安装](Docker_install_CN.md) 中对应自己的系统操作步骤操作，最后获取Holmes网址，即可使用

## 📦 Ubuntu 直接安装


- 直接在ubuntu 系统安装,需要将安装redis,postgresql python3.8.17 环境
- 环境建议
    1. redis 可以直接通过127.0.0.1,无密码命令行访问
    2. python版本要求3.8+ 建议使用pyenv coda 等虚拟环境
    3. postgresql 需要安装postgresql-16 版本
- 下载代码``` git clone git@github.com:DeepThought-AI/Holmes.git ```
- 直接运行 ```. ubuntu_CN_install.sh ```即可 <br>(注意，这里运行的是 . ubuntu_CN_install.sh 而不是sh xxx， 因为需要运行python 虚拟环境)
- 默认使用端口 8338 8339
- web访问: http://[ip]:8338
- 如果数据库或者redis本地已经安装,可以在.env 中修改
    完成安装后, 请看上面的使用说明进行设置和使用
- 查看本机IP ```ifconfig | grep -Eo 'inet (addr:)?([0-9]*\.){3}[0-9]*' | grep -vE '^inet 127(\.[0-9]{1,3}){3}'```

## Holmes 命令

- 进入项目目录后运行命令：
```
    docker-compose start # 启动Holmes 服务
    docker-compose stop # 停止Holmes 服务
    docker-compose ps # 查看 Holmes 服务
```
- 如果出现 ```... PermissionError ...``` 或者 ```... Permission denied ...``` 请在执行命令前 加sudo
```
    sudo docker-compose start # 启动Holmes 服务
    sudo docker-compose stop # 停止Holmes 服务
    sudo docker-compose ps # 查看 Holmes 服务
```

## 运行硬件建议:
   - 2核心4G内存

## Contact Us
 <img src="http://www.deep-thought.io/wechat.png?1"  width="30%">


## 📑 文档
- 目前推荐环境用的是Ubuntu，其他环境还没来得及测试，下一步会逐步完善，使用的话 windows10 WSL、window11 WSL、 macOS13.X、macOS14.X、ubuntu20.04 ubuntu22.04都没有问题。
-
- 如果有什么问题，可以联系我们 dev@deep-thought.io
- <a href="https://github.com/DeepThought-AI/Holmes/issues">Issue</a>

