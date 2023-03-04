# Install OpenMetaData using Docker
1. 创建数据存放路径, `mkdir -p $PWD/docker-volume/db-data`
1. 创建镜像,`docker compose up --build -d`
# Load data
1. 安装load data所需python包,`pip3 install -i https://pypi.org/simple --upgrade "openmetadata-ingestion"`
1. 用docker安装的openMetaData的jwt token替换 load_data.py 中的 jwt token
2. load data, `python3 load_data.py`
