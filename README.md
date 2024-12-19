# README: Running the Spark Application

#### Prerequisites

Ensure you have the following installed on your machine:

Docker: Install Docker

Running the Application Using the Dockerfile

##### Steps:

###### Clone the Repository:

git clone <repository-url>
cd <repository-directory>

###### Build the Docker Image:

docker build -t klm-network-analysis .

###### Run the Container:

Use the following command to start the application:

docker run klm-network-analysis [parameters]

Example Passing Arguments:
To pass arguments to the Python script, use:

`docker run --rm klm-network-analysis --input-path ./data --start-date 2019-03-01 --end-date 2019-03-31
`
View Output:
The output files will be generated in the /app/output directory within the container. To copy them to your host system, use:

`docker cp <container-id>:/app/output ./output
`
Running the Application Using Docker Compose

#### Steps:

Clone the Repository:

`git clone <repository-url>
cd <repository-directory>`

Start the Services:
Use Docker Compose to bring up the services:

`docker-compose up -d`

###### This will start the following services:

spark-master: Spark Master node

spark-worker: Spark Worker node

hdfs-namenode: HDFS NameNode

hdfs-datanode: HDFS DataNode

Verify Services Are Running:
Check the status of the containers:

`docker ps`

Spark Master UI: http://localhost:8080

HDFS NameNode UI: http://localhost:9870

#### Run the PySpark Application:

##### **For local input:**


`docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
    /app/src/main/data_analysis.py \
    --input_path file:///app/data \
    --start_date 2019-03-01 \
    --end_date 2019-03-31`
 
##### **For HDFS input:**

copy the data to hdfs namenode ,

`docker cp ./data/ hdfs-namenode:/data

docker exec -it hdfs-namenode bash

`
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
    /app/src/main/data_analysis.py \
    --input_path hdfs://hdfs-namenode:9000/app/data \
    --start_date 2019-03-01 \
    --end_date 2019-03-31
`
###### View Output:

The output files will be saved in the ./output directory on the host system.

###### Stop Services:

`docker-compose down`