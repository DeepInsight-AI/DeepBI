#如何安装docker和docker-compose

## Windows
- 你可以通过 https://learn.microsoft.com/en-us/windows/wsl/install 或者在微软商店进行安装 WSL
  或者通过管理员命令行安装 WSL :wsl --install
- ![cmd.jpg](user_manual/cn/img/cmd.jpg)
- 需要调整本地主板虚拟化选项
  电脑按del键进入Bios,进入高级选项"Intel (VMX) Virtualization Technology" or "Intel (VMX) Virtualization Technology" or "AMD-V" ，并设置"enable on"，然后重启电脑" (或像下图)
- ![bios.png](user_manual/cn/img/bios.png)

- 打开管理员命令行，登录 WSL
- 运行命令 "wsl" 或者 "wsl -u [username] username 是你之前安装的用户名
- 运行 "lsb_release -a", 你会看到 ubuntu版本 ，根据版本号 ，运行以下命令安装 docker
- 根据上面 ubuntu 的版本号，运行以下命令安装 docker :<br>
   sudo snap install docker           # 版本号 20.10.24 安装命令 <br>
   sudo apt  install podman-docker    # 版本号 3.4.4+ds1-1ubuntu1.22.04.2<br>
   sudo apt install docker.io         # 版本号 24.0.5-0ubuntu1~22.04.1<br>
- 安装完毕docker，将权限给当前登录用户 "sudo usermod -aG docker $USER"
- 启动 docker 命令"sudo service docker start"
- 检查docker 运行状态 "service docker status" 为 "active (running)"
- 安装网络管理 命令: "sudo apt install net-tools"
- 运行命令 "service docker status" 确认docker是在运行 "active (running)"
- 运行命令 : "pwd" 你会看到你目前的文件夹地址 比如 "/mnt/c/Windows/system32"
- 下载我们的代码，解压重命名为 "Holmes" 移动到  /mnt/c/Windows/system32 (也就是你之前看到的文件夹地址)
- 运行命令 "cd Holmes && sudo chmod+x ./Install.sh &&sudo ./Install.sh " 开始安装

# Ubuntu 安装docker
- 运行命令如下：
- sudo apt-get clean
- sudo apt update
- sudo apt-get install ca-certificates curl gnupg lsb-release
- sudo curl -fsSL http://mirrors.aliyun.com/docker-ce/linux/ubuntu/gpg | sudo apt-key add -
- sudo add-apt-repository "deb [arch=amd64] http://mirrors.aliyun.com/docker-ce/linux/ubuntu $(lsb_release -cs) stable"
- sudo apt-get update
- apt-get install docker-ce docker-ce-cli containerd.io
- sudo usermod -aG docker $USER
- 需要退出当前账户重新登录
- systemctl start docker
- sud apt-get -y install apt-transport-https ca-certificates curl software-properties-common
- sudo systemctl enable docker


# Mac
##安装docker的两种方法
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
- 安装命令：brew install docker
- 运行命令：brew services start docker
- 检查命令：brew services list |grep docker

# 最后
以上，安装完成。运行docker --version && docker-compose --version查看Docker版本，表明Docker已正确安装。如果您有任何其他问题，请参阅：https://docs.docker.com/desktop/
