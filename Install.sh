#!/bin/bash
set -e
export LANG=en_US.UTF-8
if [ -f .env ]; then
    rm .env
fi

if [ -f Dockerfile ]; then
    rm Dockerfile
fi
# Install.sh, you mast have installed docker and docker-compose
# check docker support
if ! command -v docker &> /dev/null; then
    echo "Docker has not installed. Solve this problem : https://github.com/DeepInsight-AI/DeepBI/blob/main/Docker_install.md"
    exit 1
fi

# check Docker Compose support
if ! command -v docker-compose &> /dev/null; then
    echo "Docker Compose has not installed. Solve this problem : https://github.com/DeepInsight-AI/DeepBI/blob/main/Docker_install.md"
    exit 1
fi
# get local ip
ip_addresses=$(ifconfig | grep -Eo 'inet (addr:)?([0-9]*\.){3}[0-9]*' | grep -vE '^inet 127(\.[0-9]{1,3}){3}')
# print ip for user to select the ip
echo "You local ip as fellows:"
echo "$ip_addresses" | tr ' ' '\n'

# let user select ip
echo "Please select a ip for your server, you can change it at .env file:"
while true; do
    # shellcheck disable=SC2162
    read -p "Please input your ip: " ip
    if [[ $ip == "" ]]; then
        echo "Ip mast input"
    else
        # shellcheck disable=SC2162
        read -p "You input this: $ip  ,Are you sure？(Y/N): " confirm
        if [[ $confirm == "Y" || $confirm == "y" ]]; then
            echo "IP ：$ip"
            break
        fi
    fi
done
echo "If external access is enabled, you need to adjust the firewall permission."
# shellcheck disable=SC2162
read -p "We need server port 8338 8339 8340,is that ports not use？(Y/N): " confirm
if [[ $confirm == "N" || $confirm == "n" ]]; then
    exit 1
fi
# get web port
# shellcheck disable=SC2162
web_port=8338
ai_web_port=8340
# get socket port
# shellcheck disable=SC2162
socket_port=8339

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

rm -f ./client/app/Language.CN.js
cp ./client/app/Language.EN.js ./client/dist/Language.CN.js
# get env_template content
env_content=$(cat .env.template)
# replace language
# shellcheck disable=SC2001
env_content=$(echo "$env_content" | sed "s/LANGTYPE/EN/g")
# replace web port
# shellcheck disable=SC2001
env_content=$(echo "$env_content" | sed "s/AI_WEB_PORT/$ai_web_port/g")
# shellcheck disable=SC2001
env_content=$(echo "$env_content" | sed "s/WEB_PORT/$web_port/g")
# replace language
# shellcheck disable=SC2001
env_content=$(echo "$env_content" | sed "s/SOCKET_PORT/$socket_port/g")
# replace ip
# shellcheck disable=SC2001
env_content=$(echo "$env_content" | sed "s/IP/$ip/g")
# replace sec_key
sec_key=$(openssl rand -hex 16)
# shellcheck disable=SC2001
env_content=$(echo "$env_content" | sed "s/SEC_KEY/$sec_key/g")
# save .env file
echo "$env_content" > .env
echo "DATA_SOURCE_FILE_DIR=/app/user_upload_files" >> .env
cp Dockerfile.template Dockerfile
# save env file over
echo "You setting as fellows:"
echo "--------------------------------"
echo "$env_content"
echo "--------------------------------"
# begin run docker compose
echo "Next, we will pull the image created by docker-compose.
      Depending on your network environment, it will cost half an hours or more "

docker-compose build
echo "--------------------------------"
echo "build over. Create database container and init database"
docker-compose run --rm server create_db
echo "Initialization database completed"
echo "--------------------------------"
echo "command: (ubuntu need sudo)"
echo " docker-compose up  #Create containers and front-end command line operation "
echo " docker-compose up -d # Create containers and start containers server in the background: "
echo " docker-compose start # Start all containers server "
echo " docker-compose start [container id /container name]# Start signal container server"
echo " docker-compose stop [container id /container name]# Stop signal container server"
echo " docker-compose ps # look all containers server running info"
echo "--------------------------------"
echo "Run: docker-compose up......"
docker-compose up -d
echo "--------------------------------"
echo "You can visit http://$ip:$web_port"
echo "--------------------------------"


