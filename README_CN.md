<h1 align="center">DeepBI</h1>

<div align="center">

DeepBI是一款AI原生的数据分析平台。DeepBI充分利用大语言模型的能力来探索、查询、可视化和共享来自任何数据源的数据。用户可以使用DeepBI洞察数据并做出数据驱动的决策。


</div>




<div align="center">

  Languages： 中文 [English](README.md)<br>
 开发：dev@deepbi.com，工作及商务：hi@deepbi.com

  <div style="display: flex; align-items: center;">
    如果觉得 DeepBI 对您有帮助的话，请帮忙<a style="display: flex; align-items: center;margin:0px 6px" target="_blank" href='https://github.com/DeepInsight-AI/DeepBI'></a>
    的右上角点个⭐ Star 和 Fork，您的支持是 DeepBI 变得更好最大的动力
  </div>
</div>
<div align="center">
    DeepBI官网 <a href="http://www.deepbi.com/" target="_blank">http://www.deepbi.com/</a>
</div>



## 案例视频

https://github.com/DeepInsight-AI/DeepBI/assets/151519374/f0d2fcd4-32b0-4095-a892-b9bbf8a51602

## 使用说明
- [DeepBI 在线使用说明](client/app/assets/images/cn/user_manual_cn.md)
- [PDF 中文软件说明下载](./user_manual/中文软件说明.pdf)
- [PDF 中文Docker安装下载](./user_manual/中文docker安装说明.pdf)
- [PDF 使用说明下载](./user_manual/中文使用说明.pdf)


## ✨ 特性

1 对话式数据分析: 用户可以通过对话，得到任意的数据结果和分析结果。\
2 对话式报表生成：通过对话生成持久化的报表和可视化图形。\
3 仪表板大屏：将持久化的可视化图组装为仪表板。\
4 自动化数据分析报告（待开发)：根据用户指令自动完成完整的数据分析报告。\
5 多数据源支持，支持 MySQL、PostgreSQL、Doris，Starrocks, CSV/Excel等。\
6 多平台支持，支持 Windows-WSL、Windows、Linux、Mac。\
7 国际化，支持中文、英文。


## 🚀 支持的数据库

DeepBI 支持的数据库连接有:
- MySQL
- PostgreSQL
- csv/Excel导入
- Doris
- Starrocks
- MongoDB

## 📦 Windows exe 安装文件安装
- 下载最新版本的 ```window_install_exe_CN.zip``` 安装包. <a href="https://github.com/DeepInsight-AI/DeepBI/releases">点击这里去下载</a>，目前测试支持 Win10 Win11
- 解压zip 文件后,双击运行.exe 文件安装即可。
- 本地安装版本说明 [PDF 使用说明下载](./user_manual/exe安装.pdf)

## 📦 Docker 安装和DeepBI部署

按照 [使用 Docker 安装](Docker_install_CN.md) 中对应自己的系统操作步骤操作，最后获取DeepBI网址，即可使用

## 📦 Ubuntu 直接安装


- 直接在ubuntu 系统安装,需要将安装redis,postgresql python3.8.17 环境
- 环境建议
    1. redis 可以直接通过127.0.0.1,无密码命令行访问
    2. python版本要求3.8.x 建议使用pyenv coda 等虚拟环境
    3. postgresql 需要安装postgresql-16 版本
- 下载我们的代码
 ```
git clone https://github.com/DeepInsight-AI/DeepBI.git
 ```
如果下载失败更换协议，运行下面的代码
 ```
git clone http://github.com/DeepInsight-AI/DeepBI.git
 ```
- 直接运行 ```. ubuntu_CN_install.sh ```即可 <br>(注意，这里运行的是 . ubuntu_CN_install.sh 而不是sh xxx， 因为需要运行python 虚拟环境)
- 默认使用端口 8338 8339
- web访问: http://[ip]:8338
- 如果数据库或者redis本地已经安装,可以在.env 中修改
    完成安装后, 请看上面的使用说明进行设置和使用
- 查看本机IP ```ifconfig | grep -Eo 'inet (addr:)?([0-9]*\.){3}[0-9]*' | grep -vE '^inet 127(\.[0-9]{1,3}){3}'```

## 🚀  Using different LLM

-- 支持不同的LLM，详见下表<使用不同LLM DeepBI 适配情况>
<table>
  <tr>
    <th>服务提供商</th>
    <th>模型名称</th>
    <th>辅助数据分析</th>
    <th>报表分析</th>
    <th>仪表盘</th>
    <th>自动数据分析报告</th>
    <th>备注(DeepBI2.0.0开始)</th>
    <th>最大输入/价格/千token</th>
  </tr>
  <tr>
    <td>OpenAI</td>
    <td>gpt-4</td>
    <td>支持</td>
    <td>支持</td>
    <td>支持</td>
    <td>支持</td>
    <td></td>
    <td>8K / 输入0.219元输出$0.438元（约）</td>
  </tr>
  <tr>
    <td>OpenAI</td>
    <td>gpt-4o</td>
    <td>支持</td>
    <td>支持</td>
    <td>支持</td>
    <td>支持</td>
    <td>价格更便宜</td>
    <td>128k/ 输入0.036元输出0.109元（约）</td>
  </tr>
  <tr>
    <td>DeepInsight</td>
    <td>gpt-4o</td>
    <td>支持</td>
    <td>支持</td>
    <td>支持</td>
    <td>支持</td>
    <td>目前只支持gpt-4o</td>
    <td>128k/ 输入0.036元输出0.109元（约）</td>
  </tr>
  <tr>
    <td>Microsoft Azure</td>
    <td>gpt-4 (自定义的名称)</td>
    <td>支持</td>
    <td>支持</td>
    <td>支持</td>
    <td>支持</td>
    <td></td>
    <td>8K / 输入0.219元输出$0.438元（约）</td>
  </tr>
  <tr>
    <td>Microsoft Azure</td>
    <td>gpt-4o(自定义的名称)</td>
    <td>支持</td>
    <td>支持</td>
    <td>支持</td>
    <td>支持</td>
    <td>价格更便宜</td>
    <td>128k/ 输入0.036元输出0.109元（约）</td>
  </tr>
  <tr>
    <td>AwsBendrock</td>
    <td>Claude3 sonet</td>
    <td>支持</td>
    <td>不适配</td>
    <td>不适配</td>
    <td>不适配</td>
    <td>价格更便宜</td>
    <td>200k/ 输入0.0219元输出0.109元（约）</td>
  </tr>
  <tr>
    <td>AwsBendrock</td>
    <td>Claude3 Opus</td>
    <td>支持</td>
    <td>不适配</td>
    <td>不适配</td>
    <td>不适配</td>
    <td></td>
    <td>200k/ 输入0.109元输出0.545元（约）</td>
  </tr>
  <tr>
    <td>DeepSeek</td>
    <td>deepseek-chat</td>
    <td>支持</td>
    <td>不适配</td>
    <td>不适配</td>
    <td>不适配</td>
    <td>价格十分便宜</td>
    <td>32k/ 输入0.001元输出0.002元</td>
  </tr>
  <tr>
    <td>阿里百炼</td>
    <td>qwen2-72b-instruct</td>
    <td>支持</td>
    <td>不适配</td>
    <td>不适配</td>
    <td>不适配</td>
    <td>DeepBi 2.0.1</td>
    <td>182k/ 尚未给出，根据历史估计0.12元</td>
  </tr>
  <tr>
    <td>百度千帆</td>
    <td>ernie-4.0-8k-0329</td>
    <td>支持</td>
    <td>不适配</td>
    <td>不适配</td>
    <td>不适配</td>
    <td>单数据表分析相对可以，多表适应差，DeepBI2.0.1</td>
    <td>5k / 输入输出均为0.12元</td>
  </tr>
  <tr>
    <td colspan="7">声明: 测试数据和问题具有局限性以及结论理解上非标准化，仅供参考。</td><td>价格/汇率以官方为准</td>
  </tr>
</table>

## Contact Us
<a><img src="https://github.com/user-attachments/assets/ead686d5-c0fd-4b12-b7be-825149eb4cdb" width="40%"/></a>
<br>
为感谢各位支持，本地化部署完成可以联系群内DeepBI小助手，免费领取Token。




## 📑 文档
- 我们已经在 Mac OS 12.7/13.X /14.1.1 , Ubuntu 20.04/22.04 和  Windows11 WSL 22.04 系统测试。
- Windows 10 安装WSL需要 22H2版本，详见  [使用 Docker 安装](Docker_install_CN.md)
- 服务器运行需求 最低 1核心 2G内存，建议2核心 4G内存 以上
- 如果有什么问题，可以联系我们 dev@deepbi.com
- <a href="https://github.com/DeepInsight-AI/DeepBI/issues">Issue</a>

