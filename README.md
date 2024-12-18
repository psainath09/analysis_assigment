README: Running the Spark Application

Prerequisites

Ensure you have the following installed on your machine:

Docker: Install Docker

Running the Application Using the Dockerfile

Steps:

Clone the Repository:

git clone <repository-url>
cd <repository-directory>

Build the Docker Image:

docker build -t klm-network-analysis .

Run the Container:
Use the following command to start the application:

docker run klm-network-analysis [parameters]

Example Passing Arguments:
To pass arguments to the Python script, use:

docker run --rm klm-network-analysis --input-path ./data --start-date 2019-03-01 --end-date 2019-03-31

View Output:
The output files will be generated in the /app/output directory within the container. To copy them to your host system, use:

docker cp <container-id>:/app/output ./output

Running the Application Using Docker Compose

Steps:

Clone the Repository:

git clone <repository-url>
cd <repository-directory>

Start the Services:
Use Docker Compose to bring up the services:

docker-compose up -d

This will start the following services:

spark-master: Spark Master node

spark-worker: Spark Worker node

hdfs-namenode: HDFS NameNode

hdfs-datanode: HDFS DataNode

Verify Services Are Running:
Check the status of the containers:

docker ps

Spark Master UI: http://localhost:8080

HDFS NameNode UI: http://localhost:9870

Run the PySpark Application:
Execute the script inside the container:

docker exec -it spark-master /bin/bash

cd /app

spark-submit /app/src/main/data_analysis.py --input_path ./data --start_date 2019-03-01 --end_date 2019-03-31

View Output:
The output files will be saved in the ./output directory on the host system.

docker cp <container-id>:/app/output ./output
