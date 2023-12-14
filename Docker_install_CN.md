# Windows

## 安装docker
1. 需要调整本地主板虚拟化选项<br>
  首先重启电脑，重启电脑时不断按“F1”和“F2”和“DEL”这三个键进入Bios（因为大部分主板是这三个按键的其中一个，所以一起按，另外注意笔记本是按“ESC”键）<br>
  ![bios.png](user_manual/cn/img/bios1.png)
  ![bios.png](user_manual/cn/img/bios2.png)
  ![bios.png](user_manual/cn/img/bios3.png)


2. 你可以通过 https://learn.microsoft.com/en-us/windows/wsl/install 或者在微软商店进行安装 WSL
  或者通过管理员命令行安装如下图：
  ![cmd.jpg](user_manual/cn/img/cmd.jpg)

- 进入到命令符窗口运行下面的命令
```
  WSL --install
```


- 如果出现，下面的提示<br>
 ![docker_err.jpg](user_manual/cn/img/docker_err.jpg)
- 解决方案
```
 运行: wsl --list --online 查看可以安装的版本
 然后运行: wsl --install -d Ubuntu-20.04 # 这里的Ubuntu-20.04 就是版本号
```
3. 打开管理员命令行，登录 WSL
4. 运行命令```wsl```或者 ```wsl -u [username]``` username 是你之前安装的用户名
5. 登录后，我们开始安装 docker（每行为一个命令依次运行，下同）
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
6. 安装完毕docker，将权限给当前登录用户
```sudo usermod -aG docker $USER```
7. 启动 docker 命令```sudo service docker start```
8. 检查docker 运行状态 ```service docker status``` 为 "active (running)" 或者 "Docker is running" 则为正常
9. 如果遇到```Docker is not running```
- 解决方案，运行下命令
```
sudo update-alternatives --set iptables /usr/sbin/iptables-legacy
sudo update-alternatives --set ip6tables /usr/sbin/ip6tables-legacy
```
10. 安装 docker-compose
```
sudo apt-get install docker-compose
sudo rm /usr/local/bin/docker-compose
sudo ln -s /usr/bin/docker-compose /usr/local/bin/docker-compose
```
11. 安装网络管理 命令: ```sudo apt install net-tools```
12. 上面安装完毕docker ,以后就不用再次安装
## 配置DeepBi
13. 运行命令 ```service docker status``` 确认docker是在运行 "active (running)" <br>
14. 获取本机内网IP地址,记录下来，一般是192.168.1.xxx,稍后可以用在安装路径下 如下图:<br>
![ip.png](user_manual/cn/img/ip.png)

15.然后，安装DeepBi有两种方式，任选其一 如下

- （1）直接下载压缩包 (推荐)
- 在WSL命令行中运行命令 : ```pwd``` 你会看到你目前的文件夹地址 比如 ```/mnt/c/Windows/system32```
- 点击”<a href="https://github.com/DeepThought-AI/DeepBi" target='_blank'>链接</a>“通过网页下载我们的代码 如下图
- ![download.png](user_manual/cn/img/download.png)
- 解压后的文件夹 "DeepBi" 移动到  C:/Windows/system32 (也就是上面看到的文件夹地址,c表示C盘)
- 回到WSL命令行，运行命令```cd DeepBi ```进入项目文件夹
- 修改权限 ```sudo chmod +x ./Install.sh```
- 运行命令```sudo ./Install_cn.sh ``` 开始安装，安装结束后会有一个网址提示，直接浏览器访问即可

- （2）通过命令下载代码 (需要本地github公钥)
- 下载我们的代码，```git clone git@github.com:DeepThought-AI/DeepBi.git```
- 出现 ```Are you sure you want to continue connecting (yes/no/[fingerprint])? ```
- 输入 ```yes```  回车
- 进入项目文件夹 ```cd DeepBi ```
- 修改权限 ```sudo chmod +x ./Install.sh```
- 运行命令```sudo ./Install_cn.sh ``` 开始安装<br>
16. 安装结束后会有一个网址提示，直接浏览器访问即可<br>
（注意*关闭命令符窗口将无法访问属于DeepBi网址
   再次使用DeepBi，打开“命令提示符”窗口“以管理员身份运行”
  ```
   1.运行“wsl”命令
   2.运行“cd DeepBi”命令
   3.运行“sudo docker-compose start”命令
  ```
   就可以去浏览器中打开自己DeepBi网址了【网址都是http://‘本机内网IP地址’:8338】


# Ubuntu
## 安装docker
1. 系统下，进入命令行，逐步运行命令如下（如果已经安装docker 直接跳过前两步）：
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
2. 需要退出当前账户重新登录， 重新登录后，进入命令行
```
systemctl start docker
sud apt-get -y install apt-transport-https ca-certificates curl software-properties-common
sudo systemctl enable docker
```
## 配置DeepBi
3. 开始下载我们的代码文件
```
git clone git@github.com:DeepThought-AI/DeepBi.git
```
4. 解压后重命名为 "DeepBi" 然后进入目录
```
 cd DeepBi && sudo chmod+x./Install.sh
 . Install_cn.sh
```
5. 注意上面运行的是 . Install_cn.sh
6. 安装结束后会有一个网址提示，直接浏览器访问即可


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

## 配置DeepBi
- 下载代码```git clone git@github.com:DeepThought-AI/DeepBi.git```
- 运行命令到对应文件夹 ```cd DeepBi ```
- 修改权限 ```sudo chmod+x ./Install.sh```
- 运行命令```sudo ./Install_cn.sh ``` 开始安装，安装结束后会有一个网址提示，直接浏览器访问即可

