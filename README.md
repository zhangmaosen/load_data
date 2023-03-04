# Install OpenMetaData using Docker
1. 创建数据存放路径, `mkdir -p $PWD/docker-volume/db-data`
1. 创建镜像,`docker compose up --build -d`
# Load data
1. 安装load data所需python包,`pip3 install -i https://pypi.org/simple --upgrade "openmetadata-ingestion"`
1. 用docker安装的openMetaData的jwt token替换 load_data.py 中的 jwt token![image](https://user-images.githubusercontent.com/1047603/222934373-884b5b08-47d8-47e3-820d-f3bc9ceff4f3.png)
2. load data, `python3 load_data.py`
