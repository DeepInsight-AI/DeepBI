#如何安装docker和docker-compose

##Windows

- 从这里下载docker桌面：https://docs.docker.com/desktop/install/windows-install/，点击蓝色按钮
- 安装docker桌面
- 打开docker desktop，点击“settings”，然后点击“resources”，然后点击“advanced”，然后点击“docker engine”，然后点击“settings”，然后点击“advanced”，然后点击“daemon”，然后点击“tls”，然后点击“enable tls verification”，然后点击“ok”，然后点击“ok”

#Ubuntu
- 运行命令如下：
1. sudo sudo apt-get clean
2. sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"
3. sudo apt-get update
4. sudo apt-get install docker-ce docker-ce-cli containerd.io
5. sudo systemctl start docker
6. sudo systemctl enable docker

# Mac
##安装docker的两种方法
### 1. 下载docker desktop
- 从这里：https://docs.docker.com/desktop/install/mac-install/
- 打开docker desktop，点击“设置”，然后点击“资源”，然后点击“高级”，然后点击“docker engine”，然后点击“设置”，然后点击“高级”，然后点击“daemon”，然后点击“tls”，然后点击“启用tls验证”，然后保存即可。

### 2.使用系统偏好设置安装docker（推荐）
- 安装命令：brew install docker
- 运行命令：brew services start docker
- 检查命令：brew services list |grep docker

# 最后
以上，安装完成。运行docker --version && docker-compose --version查看Docker版本，表明Docker已正确安装。如果您有任何其他问题，请参阅：https://docs.docker.com/desktop/
