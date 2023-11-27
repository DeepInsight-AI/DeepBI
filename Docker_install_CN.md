## Windows

- 你可以通过 https://learn.microsoft.com/en-us/windows/wsl/install 或者在微软商店进行安装 WSL
  或者通过管理员命令行安装
  ```
  WSL --install
  ```
- 如果出现，下面的提示
```
版权所有(c) Microsoft Corporation。保留所有权利。

用法: wsl.exe [Argument] [Options...] [CommandLine]

运行 Linux 二进制文件的参数:

    如果未提供命令行，wsl.exe 将启动默认 shell......
```
- 解决方案
```
 运行: wsl --list --online 查看可以安装的版本
 然后运行: wsl --install -d Ubuntu-20.04 # 这里的Ubuntu-20.04 就是版本号
```
- 然后在管理员命令行运行

- ![cmd.jpg](user_manual/cn/img/cmd.jpg)
- 需要调整本地主板虚拟化选项
  电脑按del键进入Bios,进入高级选项"Intel (VMX) Virtualization Technology" or "Intel (VMX) Virtualization Technology" or "AMD-V" ，并设置"enable on"，然后重启电脑" (或像下图)<br>
  ![bios.png](user_manual/cn/img/bios.png)

- 打开管理员命令行，登录 WSL
- 运行命令```wsl```或者 ```wsl -u [username]``` username 是你之前安装的用户名
- 登录后，我们开始安装 docker（每行为一个命令依次运行，下同）
```
   sudo apt update
   sudo apt install apt-transport-https ca-certificates curl software-properties-common
   curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
   sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"
   sudo apt update
   sudo apt install docker-ce
   sudo service docker start
   sudo service docker status
```
- 安装完毕docker，将权限给当前登录用户
```sudo usermod -aG docker $USER```
- 启动 docker 命令```sudo service docker start```
- 检查docker 运行状态 ```service docker status``` 为 "active (running)" 或者 "Docker is running" 则为正常
- 如果遇到```Docker is not running```
- 解决方案
```
sudo update-alternatives --set iptables /usr/sbin/iptables-legacy
sudo update-alternatives --set ip6tables /usr/sbin/ip6tables-legacy
```
- 安装 docker-compose
```
sudo curl -L https://download.fastgit.org/docker/compose/releases/download/2.23.3/docker-compose-`uname -s`-`uname -m` > /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
```

- 安装网络管理 命令: ```sudo apt install net-tools```
- 运行命令 ```service docker status``` 确认docker是在运行 "active (running)" <br>
- 获取本机内网IP地址,记录下来，一般是192.168.1.xxx,稍后可以用在安装路径下 如下图:<br>
![ip.png](user_manual/cn/img/ip.png)

然后，安装Holmes有两种方式，任选其一 如下

- （1）直接下载压做包 (推荐)
- 在WSL命令行中运行命令 : ```pwd``` 你会看到你目前的文件夹地址 比如 ```/mnt/c/Windows/system32```
- 切换到桌面，点击”<a href="https://github.com/DeepThought-AI/Holmes" target='_blank'>链接</a>“通过网页下载我们的代码 如下图
- ![download.png](user_manual/cn/img/download.png)
- 解压后的文件夹 "Holmes" 移动到  C:/Windows/system32 (也就是上面看到的文件夹地址,c表示C盘)
- 回到WSL命令行，运行命令```cd Holmes ```进入项目文件夹
- 修改权限 ```sudo chmod +x ./Install.sh```
- 运行命令```sudo ./Install_cn.sh ``` 开始安装，安装结束后会有一个网址提示，直接浏览器访问即可

- （2）通过命令下载代码 (需要本地github公钥)
- 下载我们的代码，```git clone git@github.com:DeepThought-AI/Holmes.git```
- 出现 ```Are you sure you want to continue connecting (yes/no/[fingerprint])? ```
- 输入 ```yes```  回车
- 进入项目文件夹 ```cd Holmes ```
- 修改权限 ```sudo chmod +x ./Install.sh```
- 运行命令```sudo ./Install_cn.sh ``` 开始安装<br>
安装结束后会有一个网址提示，直接浏览器访问即可



# Ubuntu 安装docker
- 系统下，进入命令行，逐步运行命令如下（如果已经安装docker 直接跳过前两步）：
```
sudo apt-get clean
sudo apt-get update
sudo apt-get install ca-certificates curl gnupg lsb-release
sudo curl -fsSL http://mirrors.aliyun.com/docker-ce/linux/ubuntu/gpg | sudo apt-key add -
sudo add-apt-repository "deb [arch=amd64] http://mirrors.aliyun.com/docker-ce/linux/ubuntu $(lsb_release -cs) stable"
sudo apt-get update
apt-get install docker-ce docker-ce-cli containerd.io
sudo usermod -aG docker $USER
```
- 需要退出当前账户重新登录， 重新登录后，进入命令行
```
systemctl start docker
sud apt-get -y install apt-transport-https ca-certificates curl software-properties-common
sudo systemctl enable docker
```
- 开始下载我们的代码文件
```
git clone git@github.com:DeepThought-AI/Holmes.git
```
- 解压后重命名为 "Holmes" 然后进入目录
```
 cd Holmes && sudo chmod+x./Install.sh
 . Install_cn.sh
```
- 注意上面运行的是 . Install_cn.sh
- 安装结束后会有一个网址提示，直接浏览器访问即可


# Mac
##安装docker
### 1. 下载docker desktop（推荐）
- 从这里：https://docs.docker.com/desktop/install/mac-install/
  - 打开docker desktop，点击“setting”, 选择 Docker engine
  ```
  {
    "builder": {
      "gc": {
        "defaultKeepStorage": "20GB",
        "enabled": true
      }
    },
    "experimental": false,
    "registry-mirrors": [
      "https://docker.mirrors.ustc.edu.cn" # 新增国内源，建议用阿里云自定义源
    ]
  }
  ```
### 2.使用系统偏好设置安装docker
- 安装命令：```brew install docker```
- 运行命令：```brew services start docker```
- 检查命令：```brew services list |grep docker```

## 安装完毕 docker后，运行以下命令
- 下载代码```git clone git@github.com:DeepThought-AI/Holmes.git```
- 运行命令到对应文件夹 ```cd Holmes ```
- 修改权限 ```sudo chmod+x ./Install.sh```
- 运行命令```sudo ./Install_cn.sh ``` 开始安装，安装结束后会有一个网址提示，直接浏览器访问即可

