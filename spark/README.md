# Spark docker

Docker images to:
* Setup a standalone [Apache Spark](https://spark.apache.org/) cluster running one Spark Master and multiple Spark workers as containers
* Build Spark applications using Python to run on a Spark cluster

Currently supported versions:
* Spark 2.4.4 for Hadoop 2.7+ with OpenJDK 8


## Using Docker Compose

Add the following services to your `docker-compose.yml` to integrate a Spark master and Spark worker in your data pipelines:
```yml
spark-master:
  image: mrdaggubati/spark-master:2.4.7-hadoop2.7
  container_name: spark-master
  ports:
    - "8080:8080"
    - "7077:7077"
  environment:
    - INIT_DAEMON_STEP=setup_spark
    - "constraint:node==<yourmasternode>"
spark-worker-1:
  image: mrdaggubati/spark-worker:2.4.7-hadoop2.7
  container_name: spark-worker-1
  depends_on:
    - spark-master
  ports:
    - "8081:8081"
  environment:
    - "SPARK_MASTER=spark://spark-master:7077"
    - "constraint:node==<yourworkernode>"
spark-worker-2:
  image: mrdaggubati/spark-worker:2.4.7-hadoop2.7
  container_name: spark-worker-2
  depends_on:
    - spark-master
  ports:
    - "8081:8081"
  environment:
    - "SPARK_MASTER=spark://spark-master:7077"
    - "constraint:node==<yourworkernode>"  
```
Make sure to fill in the `INIT_DAEMON_STEP` as configured in your pipeline.

## Running Docker containers without the init daemon
### Spark Master
To start a Spark master:

    docker run --name spark-master -h spark-master -e ENABLE_INIT_DAEMON=false -d mrdaggubati/spark-master:2.4.7-hadoop2.7

### Spark Worker
To start a Spark worker:

    docker run --name spark-worker-1 --link spark-master:spark-master -e ENABLE_INIT_DAEMON=false -d mrdaggubati/spark-worker:2.4.7-hadoop2.7

## Launch a Spark application
Building and running your Spark application on top of the Spark cluster is as simple as extending a template Docker image. Check the template's README for further documentation.
* [Python template](template/python)

---WIP /TBD ..
  Docker SWARM stack yml with reverse proxy using Traefik or NGINX.
  K8s 
  hive support
  
  Integate with  CI/CD buildkite, Travis, github etc..
  
  
  
  
 # CREDITS
  ------
 learned by following various influencers & geeks like Harishekon and Big Data europe.
 tweaked, trimmed or modified changed as needed to fit into my own environment and use cases.

