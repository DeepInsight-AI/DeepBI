# How to install docker and docker-compose

## Windows
- In widows system ,we need install WSL server, https://learn.microsoft.com/en-us/windows/wsl/install or in Microsoft Store
  search Ubuntu and install it
- Install WSL success then open command prompt and type "wsl --version",you will see the version of WSL
- login in WSL by command "wsl -d [username]", username is your username
- we can install docker and docker-compose as fellow steps in ubuntu

## Ubuntu
- sudo sudo apt-get clean
- sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"
- sudo apt-get update
- sudo apt-get install docker-ce docker-ce-cli containerd.io
- sudo systemctl start docker
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
