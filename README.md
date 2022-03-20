
#### General Docker setup
```
# create the services/containers config
  Create a docker-compose.yml file

# Launch the cluster
  docker compose up -f <file.name> -d

# Kill the cluster
  docker-compose down
```

#### Setup flink on docker
```
# create flink containers and startup
  docker compose up -f <file.name> -d
    
# Access the cluster
  [localhost](http://localhost:8081/#/overview)

# Access job manager containers
  docker exec -it $(docker ps --fliter name=jobmanager /bin/sh

# Scale up/down the taskmgr
  docker-compose scale taskmanager=<N>
  
# Run flink application jobs
   1. Build artifact from IDE
   2. Download and Install flink locally to get an handle of some scripts
       [Flink Downloads](https://flink.apache.org/downloads.html#apache-flink-1143)
   3. Submit the jar to the cluster 
       flink run -c com.example.SimpleFlinkBatchJob flink.jar
```

#### [Setup mysql on Docker](https://hub.docker.com/_/mysql)
``` 
 # pull the latest image
 docker pull mysql
 
 # start the mysql server container
 docker stop mysql1
 docker run --name mysql1 -e MYSQL_ROOT_PASSWORD=setup-your-pw -d mysql
 
 # connect to mysql 
 docker exec -it mysql1 mysql -uroot -p
 
 # start a mysql client to be access via non-container application
 docker exec -it mysql1 mysql -uroot -p
 
 # rm the mysql container
 docker rm mysql1
```

#### [Setup kafka on Docker](https://developer.confluent.io/quickstart/kafka-docker/)
````
# Start kafka on docker
  docker compose -f docker-compose-kafka.yml up -d

# create topic
  docker exec broker \
    kafka-topics --bootstrap-server broker:9092 \
                 --create \
                 --topic <topic-name>
               
# read / write from topic via console
  docker exec --interactive --tty broker \
    kafka-console-consumer \
      --bootstrap-server broker:9092 \
      --topic <topic> --from-beginning
                       
  docker exec --interactive --tty broker \
    kafka-console-producer \
     --bootstrap-server broker:9092 \
     --topic <topic>
````
