FROM mrdaggubati/spark-base:2.4.7-hadoop2.7

ENV SPARK_VERSION=2.4.7
ENV HADOOP_VERSION=2.7

COPY start-worker.sh /start-worker.sh

COPY start-worker.sh /start-worker.sh

ENV SPARK_WORKER_WEBUI_PORT 8081
ENV SPARK_WORKER_LOG /spark/logs
ENV SPARK_MASTER "spark://spark-master:7077"

EXPOSE 8081

CMD ["/bin/bash", "/start-worker.sh"]
