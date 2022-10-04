from gradiant/spark:2.4.0-python

ENV PYSPARK_PYTHON=/usr/bin/python3
WORKDIR /opt/application/

COPY requirements.txt .
COPY .env .
RUN pip install -r requirements.txt

COPY etl.py .
COPY postgresql-42.4.2.jar .
COPY aws-java-sdk-1.7.4.jar .
COPY hadoop-aws-2.7.3.jar .
COPY jets3t-0.9.4.jar .
CMD ["spark-submit", "--jars", "hadoop-aws-2.7.3.jar,jets3t-0.9.4.jar,aws-java-sdk-1.7.4.jar,postgresql-42.4.2.jar", "etl.py"]
