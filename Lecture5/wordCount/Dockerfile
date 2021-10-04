FROM bde2020/spark-python-template:3.1.1-hadoop3.2

ONBUILD COPY requirements.txt /app/
ONBUILD RUN pip3 install -r /app/requirements.txt
	  
ONBUILD COPY example.py /app/

ENV SPARK_APPLICATION_PYTHON_LOCATION /app/example.py
ENV SPARK_SUBMIT_ARGS "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2"