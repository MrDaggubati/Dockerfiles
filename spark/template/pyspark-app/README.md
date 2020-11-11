# credits : 
# Big Data Europe ;
 I am not the original developer of this, allthe credits goes to awesome people behind Big data europe team who open sourced these.


Spark Python template

The Spark Python template image serves as a base image to build your own Python application to run on a Spark cluster. See big-data-europe/docker-spark README for a description how to setup a Spark cluster.
Package your application using pip

You can build and launch your Python application on a Spark cluster by extending this image with your sources. The template uses pip to manage the dependencies of your project, so make sure you have a requirements.txt file in the root of your application specifying all the dependencies.
Extending the Spark Python template with your application
Steps to extend the Spark Python template

    Create a Dockerfile in the root folder of your project (which also contains a requirements.txt)
    Extend the Spark Python template Docker image
    Configure the following environment variables (unless the default value satisfies):

    SPARK_MASTER_NAME (default: spark-master)
    SPARK_MASTER_PORT (default: 7077)
    SPARK_APPLICATION_PYTHON_LOCATION (default: /app/app.py)
    SPARK_APPLICATION_ARGS

    Build and run the image

    ```
    docker build --rm -t mrdaggubati/spark-test-app:pyspark .

    docker run -it --network=spark_spark-network --link spark-master:spark-master -e ENABLE_INIT_DAEMON=false mrdaggubati/spark-test-app:pyspar
    ```
    
    # check subnets 
    in which the container getting initiazed, use services docker-compose for better control

docker build --rm -t mrdaggubati/spark-app .
docker run --name my-spark-app -e ENABLE_INIT_DAEMON=false --link spark-master:spark-master -d mrdaggubati/spark-app

The sources in the project folder will be automatically added to /app if you directly extend the Spark Python template image. Otherwise you will have to add the sources by yourself in your Dockerfile with the command:

COPY . /app

If you overwrite the template's CMD in your Dockerfile, make sure to execute the /template.sh script at the end.
Example Dockerfile

Example application

# check pyspark-app under template folder