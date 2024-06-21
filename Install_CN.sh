#!/bin/bash
set -e
export LANG=en_US.UTF-8
# 安装文件
if [ -f .env ]; then
    rm .env
fi

if [ -f Dockerfile ]; then
    rm Dockerfile
fi
# 检测docker
if ! command -v docker &> /dev/null; then
    echo " 需要安装 docker, 参考：https://github.com/DeepInsight-AI/DeepBI/blob/main/Docker_install_CN.md "
    exit 1
fi

# 检测 docker-compose 支持
if ! command -v docker-compose &> /dev/null; then
    echo "需要安装 docker-compose, 参考：https://github.com/DeepInsight-AI/DeepBI/blob/main/Docker_install_CN.md"
    exit 1
fi
# get local ip
ip_addresses=$(ifconfig | grep -Eo 'inet (addr:)?([0-9]*\.){3}[0-9]*' | grep -vE '^inet 127(\.[0-9]{1,3}){3}')
# print ip for user to select the ip
echo "需要设置服务器的IP地址，你本地IP 如下，可以选择一个局域网IP:"
echo "$ip_addresses" | tr ' ' '\n'

# let user select ip
echo "你输入的IP，它将作为页面访问的IP地址（以后可以在.env 更改）"
while true; do
    # shellcheck disable=SC2162
    read -p "请输入你的 ip: " ip
    if [[ $ip == "" ]]; then
        echo "必须输入你的 ip，不能为空"
    else
        read -p "你输入的IP是: $ip  ,确定么？(Y/N): " confirm
        if [[ $confirm == "Y" || $confirm == "y" ]]; then
            echo "IP ：$ip"
            break
        fi
    fi
done
# shellcheck disable=SC2162
echo "如果开放外部访问，需要调整防火墙允许访问"
# shellcheck disable=SC2162
read -p "我们会使用服务端口 8338 8339 8340,确保它们没有使用？(Y/N): " confirm
if [[ $confirm == "N" || $confirm == "n" ]]; then
    exit 1
fi
# get web port
# shellcheck disable=SC2162
web_port=8338
# get socket port
# shellcheck disable=SC2162
socket_port=8339
ai_web_port=8340

# replace front file ip
echo "Rename files "
rm -rf ./client/dist
cp -R ./client/dist_source ./client/dist
echo "Replace ip port"
os_name=$(uname)
if [[ "$os_name" == "Darwin" ]]; then
    sed -i '' "s|192.168.5.165:8339|$ip:$socket_port|g" ./client/dist/vendors~app.js
    sed -i '' "s|192.168.5.165:8339|$ip:$socket_port|g" ./client/dist/app.js
else
    sed -i "s|192.168.5.165:8339|$ip:$socket_port|g" ./client/dist/vendors~app.js
    sed -i "s|192.168.5.165:8339|$ip:$socket_port|g" ./client/dist/app.js
fi
# 复制 .env file基础内容
env_content=$(cat .env.template)
# replace language
env_content=$(echo "$env_content" | sed "s/LANGTYPE/CN/g")
# replace web port
# shellcheck disable=SC2001
env_content=$(echo "$env_content" | sed "s/AI_WEB_PORT/$ai_web_port/g")
env_content=$(echo "$env_content" | sed "s/WEB_PORT/$web_port/g")
# shellcheck disable=SC2001
env_content=$(echo "$env_content" | sed "s/SOCKET_PORT/$socket_port/g")
# replace ip，替换IP
env_content=$(echo "$env_content" | sed "s/IP/$ip/g")
# replace sec_key， 替换码
sec_key=$(openssl rand -hex 16)
env_content=$(echo "$env_content" | sed "s/SEC_KEY/$sec_key/g")
# save .env file，保存文件
echo "$env_content" > .env
echo "DATA_SOURCE_FILE_DIR=/app/user_upload_files" >> .env
# 修改配置 pip 为国内清华源
sed 's/#CN#//g' Dockerfile.template > Dockerfile
# 输出说明：
echo "所有配置如下:"
echo "--------------------------------"
echo "$env_content"
echo "--------------------------------"
# begin run docker compose
echo "下面将开始 通过docker-compose 拉取创建镜像，可能需要60分钟，主要根据你的网络情况，请耐心等待。 "
echo "或者，你修改本地docker 镜像源地址，比如更改为阿里云等"

docker-compose build
echo "--------------------------------"
echo "镜像拉取创建完毕，开始初始化镜像中数据库"
docker-compose run --rm server create_db
echo "数据库初始化完毕"
echo "--------------------------------"
echo "下面开始启动DeepBI  下面是一些尝用命令"
echo "常用命令: (ubuntu need sudo)"
echo " docker-compose up  # 创建容器，并启动容器 "
echo " docker-compose up -d # 创建容器，并在后台运行容器 "
echo " docker-compose start # 启动所有已经创建的容器，并后台运行"
echo " docker-compose start [container id /container name]# 启动单个服务容器"
echo " docker-compose stop [container id /container name]# 停止单个服务容器"
echo " docker-compose ps # 查看所有运行中的容器"
echo "--------------------------------"
echo "现在，创建并启动容器......"
docker-compose up -d
echo "--------------------------------"
echo "启动成功，你可以访问 http://$ip:$web_port"
echo "--------------------------------"
echo "谢谢，如果有问题，可以给我们提issue "

