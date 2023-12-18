<h1 align="center">DeepBI</h1>

<div align="center">

DeepBi是一款AI原生的数据分析平台。DeepBi充分利用大语言模型的能力来探索、查询、可视化和共享来自任何数据源的数据。用户可以使用DeepBi洞察数据并做出数据驱动的决策。


</div>




<div align="center">

  Languages： 中文 [English](README.md)<br>
 开发：dev@deep-insight.co，工作及商务：hi@deep-insight.co

  <div style="display: flex; align-items: center;">
    如果觉得 DeepBi 对您有帮助的话，请帮忙<a style="display: flex; align-items: center;margin:0px 6px" target="_blank" href='https://github.com/DeepInsight-AI/DeepBi'></a>
    的右上角点个⭐ Star 和 Fork，您的支持是 DeepBi 变得更好最大的动力
  </div>
</div>



## 案例视频

https://github.com/DeepInsight-AI/DeepBi/assets/151519374/f0d2fcd4-32b0-4095-a892-b9bbf8a51602

## 使用说明
- [DeepBi 在线使用说明](client/app/assets/images/cn/user_manual_cn.md)
- [PDF 中文软件说明下载](./user_manual/中文软件说明.pdf)
- [PDF 中文Docker安装下载](./user_manual/中文docker安装说明.pdf)
- [PDF 使用说明下载](./user_manual/中文使用说明.pdf)


## ✨ 特性

1 对话式数据分析: 用户可以通过对话，得到任意的数据结果和分析结果。\
2 对话式报表生成：通过对话生成持久化的报表和可视化图形。\
3 仪表板大屏：将持久化的可视化图组装为仪表板。\
4 自动化数据分析报告（待开发)：根据用户指令自动完成完整的数据分析报告。\
5 多数据源支持，支持 MySQL、PostgreSQL、Doris，Starrocks, CSV/Excel等。\
6 多平台支持，支持 Windows-WSL、Linux、Mac。\
7 国际化，支持中文、英文。


## 🚀 支持的数据库

DeepBi 支持的数据库连接有:
- MySQL
- PostgreSQL
- csv/Excel导入

## 📦 Docker 安装部署

按照 [使用 Docker 安装](Docker_install_CN.md) 中对应自己的系统操作步骤操作，最后获取DeepBi网址，即可使用

## 📦 Ubuntu 直接安装


- 直接在ubuntu 系统安装,需要将安装redis,postgresql python3.8.17 环境
- 环境建议
    1. redis 可以直接通过127.0.0.1,无密码命令行访问
    2. python版本要求3.8+ 建议使用pyenv coda 等虚拟环境
    3. postgresql 需要安装postgresql-16 版本
- 下载代码``` git clone git@github.com:DeepInsight-AI/DeepBi.git ```
- 直接运行 ```. ubuntu_CN_install.sh ```即可 <br>(注意，这里运行的是 . ubuntu_CN_install.sh 而不是sh xxx， 因为需要运行python 虚拟环境)
- 默认使用端口 8338 8339
- web访问: http://[ip]:8338
- 如果数据库或者redis本地已经安装,可以在.env 中修改
    完成安装后, 请看上面的使用说明进行设置和使用
- 查看本机IP ```ifconfig | grep -Eo 'inet (addr:)?([0-9]*\.){3}[0-9]*' | grep -vE '^inet 127(\.[0-9]{1,3}){3}'```

## DeepBi 命令

- 进入项目目录后运行命令：
```
    docker-compose start # 启动DeepBi 服务
    docker-compose stop # 停止DeepBi 服务
    docker-compose ps # 查看 DeepBi 服务
```
- 如果出现 ```... PermissionError ...``` 或者 ```... Permission denied ...``` 请在执行命令前 加sudo
```
    sudo docker-compose start # 启动DeepBi 服务
    sudo docker-compose stop # 停止DeepBi 服务
    sudo docker-compose ps # 查看 DeepBi 服务
```

## Contact Us
<a><img src="https://github.com/DeepInsight-AI/DeepBI/assets/151519374/b0ba1fc3-8c71-4bf7-bd53-ecf17050581a" width="40%"/></a>




## 📑 文档
- 我们已经在 Mac OS 12.7/13.X /14.1.1 , Ubuntu 20.04/22.04 和  Windows11 WSL 22.04 系统测试。
  <br> 如果有什么问题，可以联系我们 dev@deep-insight.co
- <a href="https://github.com/DeepInsight-AI/DeepBi/issues">Issue</a>

