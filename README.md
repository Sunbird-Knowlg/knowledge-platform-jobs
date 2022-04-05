# sunbird-knowledge-platform-jobs
Background and pipeline jobs of Knowledge Platform

## Knowledge-platform-jobs local setup
This readme file contains the instruction to set up and run the knowledge-platform-jobs in local machine.

### System Requirements:

### Prerequisites:
* Java 11

### Prepare folders for database data and logs

```shell
mkdir -p ~/sunbird-dbs/neo4j ~/sunbird-dbs/cassandra ~/sunbird-dbs/redis ~/sunbird-dbs/es ~/sunbird-dbs/kafka
export sunbird_dbs_path=~/sunbird-dbs
```


### Elasticsearch database setup in docker:
```shell
docker run --name sunbird_es -d -p 9200:9200 -p 9300:9300 \
-v $sunbird_dbs_path/es/data:/usr/share/elasticsearch/data \
-v $sunbird_dbs_path/es/logs://usr/share/elasticsearch/logs \
-v $sunbird_dbs_path/es/backups:/opt/elasticsearch/backup \
 -e "discovery.type=single-node" docker.elastic.co/elasticsearch/elasticsearch:6.8.22

```
> --name -  Name your container (avoids generic id)
>
> -p - Specify container ports to expose
>
> Using the -p option with ports 7474 and 7687 allows us to expose and listen for traffic on both the HTTP and Bolt ports. Having the HTTP port means we can connect to our database with Neo4j Browser, and the Bolt port means efficient and type-safe communication requests between other layers and the database.
>
> -d - This detaches the container to run in the background, meaning we can access the container separately and see into all of its processes.
>
> -v - The next several lines start with the -v option. These lines define volumes we want to bind in our local directory structure so we can access certain files locally.
>
> --env - Set config as environment variables for Neo4j database
>


### Neo4j database setup in docker:
1. First, we need to get the neo4j image from docker hub using the following command.
```shell
docker pull neo4j:3.3.0 
```
2. We need to create the neo4j instance, By using the below command we can create the same and run in a container.
```shell
docker run --name sunbird_neo4j -p7474:7474 -p7687:7687 -d \
    -v $sunbird_dbs_path/neo4j/data:/var/lib/neo4j/data \
-v $sunbird_dbs_path/neo4j/logs:/var/lib/neo4j/logs \
-v $sunbird_dbs_path/neo4j/plugins:/var/lib/neo4j/plugins \
--env NEO4J_dbms_connector_https_advertised__address="localhost:7473" \
--env NEO4J_dbms_connector_http_advertised__address="localhost:7474" \
--env NEO4J_dbms_connector_bolt_advertised__address="localhost:7687" \
--env NEO4J_AUTH=none \
neo4j:3.3.0
```
> --name -  Name your container (avoids generic id)
>
> -p - Specify container ports to expose
>
> Using the -p option with ports 7474 and 7687 allows us to expose and listen for traffic on both the HTTP and Bolt ports. Having the HTTP port means we can connect to our database with Neo4j Browser, and the Bolt port means efficient and type-safe communication requests between other layers and the database.
>
> -d - This detaches the container to run in the background, meaning we can access the container separately and see into all of its processes.
>
> -v - The next several lines start with the -v option. These lines define volumes we want to bind in our local directory structure so we can access certain files locally.
>
> --env - Set config as environment variables for Neo4j database
>
> Using Docker on Windows will also need a couple of additional configurations because the default 0.0.0.0 address that is resolved with the above command does not translate to localhost in Windows. We need to add environment variables to our command above to set the advertised addresses.
>
> By default, Neo4j requires authentication and requires us to first login with neo4j/neo4j and set a new password. We will skip this password reset by initializing the authentication none when we create the Docker container using the --env NEO4J_AUTH=none.

3. Load seed data to neo4j using the instructions provided in the [link](master-data/loading-seed-data.md#loading-seed-data-to-neo4j-database)

4. Verify whether neo4j is running or not by accessing neo4j browser(http://localhost:7474/browser).

5. To SSH to neo4j docker container, run the below command.
```shell
docker exec -it sunbird_neo4j bash
```

### Redis database setup in docker:
1. we need to get the redis image from docker hub using the below command.
```shell
docker pull redis:6.0.8 
```
2. We need to create the redis instance, By using the below command we can create the same and run in a container.
```shell
docker run --name sunbird_redis -d -p 6379:6379 redis:6.0.8
```
3. To SSH to redis docker container, run the below command
```shell
docker exec -it sunbird_redis bash
```
### cassandra database setup in docker:
1. we need to get the cassandra image and can be done using the below command.
```shell
docker pull cassandra:3.11.8 
```
2. We need to create the cassandra instance, By using the below command we can create the same and run in a container.
```shell
docker run --name sunbird_cassandra -d -p 9042:9042 \
-v $sunbird_dbs_path/cassandra/data:/var/lib/cassandra \
-v $sunbird_dbs_path/cassandra/logs:/opt/cassandra/logs \
-v $sunbird_dbs_path/cassandra/backups:/mnt/backups \
--network bridge cassandra:3.11.8 
```
For network, we can use the existing network or create a new network using the following command and use it.
```shell
docker network create sunbird_db_network
```
3. To start cassandra cypher shell run the below command.
```shell
docker exec -it sunbird_cassandra cqlsh
```
4. To ssh to cassandra docker container, run the below command.
```shell
docker exec -it sunbird_cassandra /bin/bash
```
5. Load seed data to cassandra using the instructions provided in the [link](master-data/loading-seed-data.md#loading-seed-data-to-cassandra-database)

### Running kafka using docker:
1. Kafka stores information about the cluster and consumers into Zookeeper. ZooKeeper acts as a coordinator between them. we need to run two services(zookeeper & kafka), Prepare your docker-compose.yml file using the following reference.
```shell
version: '3'

services:
  zookeeper:
    image: 'wurstmeister/zookeeper:latest'
    container_name: zookeeper
    ports:
      - "2181:2181"    
    environment:
      - KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:2181     
    
  kafka:
    image: 'wurstmeister/kafka:2.11-1.0.1'
    container_name: kafka
    ports:
      - "9092:9092"
    environment:
      - KAFKA_BROKER_ID=1
      - KAFKA_LISTENERS=PLAINTEXT://:9092
      - KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:9092
      - KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181      
      - ALLOW_PLAINTEXT_LISTENER=yes
    depends_on:
      - zookeeper  
```
2. Go to the path where docker-compose.yml placed and run the below command to create and run the containers (zookeeper & kafka).
```shell
docker-compose -f docker-compose.yml up -d
```
3. To start kafka docker container shell, run the below command.
```shell
docker exec -it kafka sh
```
Go to path /opt/kafka/bin, where we will have executable files to perform operations(creating topics, running producers and consumers, etc).
Example:
```shell
kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic test_topic 
```

### Steps to start a job in debug or development mode using IntelliJ:
1. Navigate to downloaded repository folder and run below command.
```shell
mvn clean install -DskipTests
``` 
2. Open the project in IntelliJ.
3. Navigate to the target job folder (Example: ../knowledge-platform-jobs/publish-pipeline/content-publish) and edit the 'pom.xml' to add below dependency.
```shell
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-clients_${scala.version}</artifactId>
  <version>${flink.version}</version>
</dependency>
```
4. Comment "provided" scope from flink-streaming-scala_${scala.version} artifact dependency in the job's 'pom.xml'.
```shell
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-streaming-scala_${scala.version}</artifactId>
    <version>${flink.version}</version>
<!--            <scope>provided</scope>-->
</dependency>
```
5. Comment the default flink StreamExecutionEnvironment in the job's StreamTask file (Example: ContentPublishStreamTask.scala) and add code to create local StreamExecutionEnvironment.
```shell
//    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
      implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment()
```
6. Save cloud storage related environment variables in StreamTask environment variables.
7. Start all databases, zookeper and kafka containers in docker
8. Run the StreamTask (Normal or Debug)
9. Open a terminal, connect to kafka docker container and produce the target job topic.
```shell
docker exec -it kafka_container_id sh
kafka-console-producer.sh --broker-list kafka:9092 --topic sunbirddev.publish.job.request
```


## Steps for running jobs in Flink locally:-
### Running flink :
1. Download the Apache flink
```shell
wget https://dlcdn.apache.org/flink/flink-1.12.7/flink-1.12.7-bin-scala_2.12.tgz
```
2. Extract the downloaded folder
```shell
tar xzf flink-1.12.7-bin-scala_2.12.tgz
```
3. Change the directory & Start the flink cluster.
```shell
cd flink-1.12.7
./bin/start-cluster.sh
```
4. Open web view to check jobmanager and taskmanager
```shell
localhost:8081
```

### Setting up Cloud storage connection:
Setup cloud storage specific variables as environment variables.
```shell
export cloud_storage_type=  #values can be 'aws' or 'azure'

For AWS Cloud Storage connectivity: 
export aws_storage_key=
export aws_storage_secret=
export aws_storage_container=

For Azure Cloud Storage connectivity:
export azure_storage_key=
export azure_storage_secret=
export azure_storage_container=

export content_youtube_apikey= #key to fetch metadata of youtube videos

```
### Running job in Flink:
1. Navigate to the required job folder (Example: ../knowledge-platform-jobs/publish-pipeline/content-publish) and run the below maven command to build the application.
```shell
mvn clean install -DskipTests
``` 
2. Start all databases, zookeper and kafka containers in docker
3. Start flink (if not started) and submit the job to flink. Example:
```shell
cd flink-1.12.7
./bin/start-cluster.sh
./bin/flink run -m localhost:8081 /user/test/workspace/knowledge-platform-jobs/publish-pipeline/content-publish/target/content-publish-1.0.0.jar
```
4. Open a terminal, connect to kafka docker container and produce the target job topic.
```shell
docker exec -it kafka_container_id sh
kafka-console-producer.sh --broker-list kafka:9092 --topic sunbirddev.publish.job.request
```