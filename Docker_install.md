# How to install docker and docker-compose

## Windows

- You are in WSL system,you need to adjust the local motherboard virtualization options
Press the del key to enter Bios, go to the advanced options "Intel (VMX) Virtualization Technology" or "Intel (VMX) Virtualization Technology" or "AMD-V", and set "enable on", then restart the computer (or like this picture).
- ![bios.png](user_manual/cn/img/bios.png)

- You can install WSL through https://learn.microsoft.com/en-us/windows/wsl/install or through the Microsoft Store, or by running the following command in the administrator command line:
```
   WSL --install
```

- ![cmd.jpg](user_manual/cn/img/cmd.jpg)


- Open the administrator command line and log into WSL.<br>
Run the command``` wsl``` or ```wsl -u [username]```  username is the username you installed before.
- After logging in, you are now in the WSL system.
- 'docker' install command :
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
- If you encounter the error```Docker is not running```
- Slove the error by the command:
```
sudo update-alternatives --set iptables /usr/sbin/iptables-legacy
sudo update-alternatives --set ip6tables /usr/sbin/ip6tables-legacy
```
- Install docker-compose
```
sudo apt-get install docker-compose
sudo rm /usr/local/bin/docker-compose
sudo ln -s /usr/bin/docker-compose /usr/local/bin/docker-compose
```
- After installing docker, give permissions to the current logged-in user. ```sudo usermod -aG docker $USER```
- Start the docker command ```sudo service docker start```
- Check the docker running status command``` service docker status``` It should be "active (running)"
- Install network management commands```sudo apt install net-tools```
- Download the Holmes code by the command : ```git clone git@github.com:DeepThought-AI/Holmes.git```
- Run the command ```cd Holmes```  to the corresponding folder and run install file ```sudo ./install.sh```

## Ubuntu
- Run the command as follows:
```
sudo apt-get clean
sudo apt-get update
sudo apt-get install ca-certificates curl gnupg lsb-release
apt-get install docker-ce docker-ce-cli containerd.io
sudo usermod -aG docker $USER
```
- Need user exit and relogin
```
systemctl start docker
sud apt-get -y install apt-transport-https ca-certificates curl software-properties-common
sudo systemctl enable docker
```

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
