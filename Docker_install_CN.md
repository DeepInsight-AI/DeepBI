#如何安装docker和docker-compose

## Windows
- 你可以通过 https://learn.microsoft.com/en-us/windows/wsl/install 或者在微软商店进行安装 WSL
  或者通过管理员命令行安装
  ```
  WSL --install
  ```
- ![cmd.jpg](user_manual/cn/img/cmd.jpg)
- 需要调整本地主板虚拟化选项
  电脑按del键进入Bios,进入高级选项"Intel (VMX) Virtualization Technology" or "Intel (VMX) Virtualization Technology" or "AMD-V" ，并设置"enable on"，然后重启电脑" (或像下图)<br>
  ![bios.png](user_manual/cn/img/bios.png)

- 打开管理员命令行，登录 WSL
- 运行命令```wsl```或者 ```wsl -u [username]``` username 是你之前安装的用户名
- 登录后，你现在已经在WSL 系统中了。运行 ```lsb_release -a ```, 你会看到 ubuntu版本 ，根据版本号 ，运行以下命令安装 docker
- 根据上面 ubuntu 的版本号，运行以下命令安装 docker :<br>
```
   sudo snap install docker           # 版本号 20.10.24 安装命令 <br>
   sudo apt  install podman-docker    # 版本号 3.4.4+ds1-1ubuntu1.22.04.2<br>
   sudo apt install docker.io         # 版本号 24.0.5-0ubuntu1~22.04.1<br>
```
- 安装完毕docker，将权限给当前登录用户
```sudo usermod -aG docker $USER```
- 启动 docker 命令```sudo service docker start```
- 检查docker 运行状态 ```service docker status``` 为 "active (running)"
- 安装网络管理 命令: ```sudo apt install net-tools```
- 运行命令 ```service docker status``` 确认docker是在运行 "active (running)"
- 两种下载方式，任选其一 如下


- （1）通过命令下载代码
- 下载我们的代码，```git clone git@github.com:DeepThought-AI/Holmes.git```
- 进入项目文件夹 ```cd Holmes ```
- 修改权限 ```sudo chmod+x ./Install.sh```
- 运行命令```sudo ./Install_cn.sh ``` 开始安装<br>



- （2）直接下载压做包
- 在WSL命令行中运行命令 : ```pwd``` 你会看到你目前的文件夹地址 比如 ```/mnt/c/Windows/system32```
- 切换到 Windows 系统中，通过网页下载我们的代码 如下图
- ![download.png](user_manual/cn/img/download.png)
- 解压后的文件夹 "Holmes" 移动到  /mnt/c/Windows/system32 (也就是上面看到的文件夹地址)
- 回到WSL命令行，运行命令进入项目文件夹 ```cd Holmes ```
- 修改权限 ```sudo chmod+x ./Install.sh```
- 运行命令```sudo ./Install_cn.sh ``` 开始安装

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
- 运行命令```sudo ./Install_cn.sh ``` 开始安装

