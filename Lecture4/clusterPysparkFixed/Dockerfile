FROM bde2020/spark-python-template:3.1.1-hadoop3.2

COPY submit.sh /

ENV ENABLE_INIT_DAEMON false
ENV SPARK_SUBMIT_ARGS "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2"

CMD ["/bin/bash", "/template.sh"]