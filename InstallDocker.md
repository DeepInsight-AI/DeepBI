# How to install docker and docker-compose

## Windows
- In widows system ,we need install WSL server, https://learn.microsoft.com/en-us/windows/wsl/install or in Microsoft Store
  search Ubuntu and install it
- Please enable the Virtual lachine Platform findows feature and ensure virtualization is enabled in the BI0S. (Restart the computer, press the del key (usually based on the motherboard) to enter the BIOS, then enter Advanced Settings and find cpu setting, find "Intel (VMX) Virtualization Technology" or "AMD-V"  set it to enable)
- Install WSL success then open command prompt and type "lsb_release -a",you will see the version of WSL,install docker based on the version
- 'docker' install command :<br>
   sudo snap install docker         # version 20.10.24 <br>
   sudo apt  install podman-docker  # version 3.4.4+ds1-1ubuntu1.22.04.2<br>
   sudo apt install docker.io         # version 24.0.5-0ubuntu1~22.04.1<br>
- Open the command prompt as an administrator
- login in WSL by command "wsl" or "wsl -u [username], username is your username
- Run command "sudo apt install net-tools"
- Run command "sudo service docker start" to start docker server
- Run command "service docker status" to check docker status,it must be "active (running)"
- Run command : "pwd" you will see local path like  "/mnt/c/Windows/system32" .
 Download project zip file from github, unzip it ,rename it as "Holmes" and move it to  /mnt/c/Windows/system32 (Path of the previous step)
- Run command "cd Holmes && sudo ./Install.sh " wait installing

## Ubuntu
- sudo apt-get clean
- sudo apt update
- sudo apt-get install ca-certificates curl gnupg lsb-release
- sudo curl -fsSL http://mirrors.aliyun.com/docker-ce/linux/ubuntu/gpg | sudo apt-key add -
- sudo add-apt-repository "deb [arch=amd64] http://mirrors.aliyun.com/docker-ce/linux/ubuntu $(lsb_release -cs) stable"
- sudo apt-get update
- apt-get install docker-ce docker-ce-cli containerd.io
- sudo usermod -aG docker $USER
- Need user exit and relogin
- systemctl start docker
- sud apt-get -y install apt-transport-https ca-certificates curl software-properties-common
- sudo systemctl enable docker


## Mac, Two ways to install docker


1. Download docker desktop  (Recommendation)
- from here: https://docs.docker.com/desktop/install/mac-install/
- open docker desktop and click on "settings" and then click on "resources" and then click on "advanced" and then click on "docker engine" and then click on "settings" and then click on "advanced" and then click on "daemon" and then click on "tls" and then click on "enable tls verification" and save.

2. Use system preferences to install docker
- Install command:brew install docker
- Run command:brew services start docker
- Check command:brew services list |grep docker

###
Above, the installation is complete. Run docker --version && docker-compose --version to see the Docker version, indicating that Docker has been installed correctly. If you have any additional questions, please refer to: https://docs.docker.com/desktop/
