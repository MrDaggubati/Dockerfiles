docker build --rm -t mrdaggubati/spark-test-app .
docker run --name pspark-test-app -e ENABLE_INIT_DAEMON=false --link spark-master:spark-master -d mrdaggubati/pyspark-test-app
