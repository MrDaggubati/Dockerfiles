FROM mrdaggubati/spark-submit:2.4.7-hadoop2.7

# Copy the source code
COPY app /app
COPY template.sh /


# Copy the requirements.txt first, for separate dependency resolving and downloading
ONBUILD COPY requirements.txt /app/requirements.txt
ONBUILD RUN cd /app \
      && pip3 install -r requirements.txt


CMD ["/bin/bash", "/template.sh"]