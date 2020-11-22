FROM mrdaggubati/spark-base:2.4.7-hadoop2.7

ENV SPARK_MASTER_NAME spark-master
ENV SPARK_MASTER_PORT 7077
ENV SPARK_APPLICATION_JAR_LOCATION /app/application.jar
ENV SPARK_APPLICATION_PYTHON_LOCATION /app/app.py
ENV SPARK_APPLICATION_MAIN_CLASS my.main.Application
ENV SPARK_APPLICATION_ARGS ""
ENV SPARK_HOME /spark
ENV SPARK_SUBMIT_ARGS ""
ENV SPARK_APPLICATION_ARGS  ""

# this is to ensure, you preload all your requirements in a base image.
# where as your app image, you predominently work on app logic than dependencies
# this reduces your build time.
COPY requirements.txt /requirements.txt
RUN  pip3 install -r requirements.txt

# this lets you copy app content that you created by using this image and also submit.sh from your local repo...
ONBUILD COPY app /


CMD ["/bin/bash", "/submit.sh"]
