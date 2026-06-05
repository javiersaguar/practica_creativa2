FROM spark:4.1.1-scala2.13-java17-python3-ubuntu
USER root
RUN pip3 install --no-cache-dir \
    pymongo==4.17.0 \
    kafka-python==2.3.2 \
    cassandra-driver==3.30.0 \
    minio==7.2.20 \
    findspark==2.0.1 \
    iso8601==2.1.0 \
    numpy==2.2.6 \
    scipy==1.15.3 \
    scikit-learn==1.7.2 \
    mlflow==2.19.0
RUN mkdir -p /home/spark/.ivy2/cache && \
    mkdir -p /home/spark/.ivy2/jars && \
    mkdir -p /tmp/.ivy2/cache && \
    mkdir -p /tmp/.ivy2/jars && \
    mkdir -p /app/jars && \
    mkdir -p /app/models && \
    chmod -R 777 /tmp/.ivy2 && \
    chmod -R 777 /app && \
    chown -R spark:spark /home/spark
ENV HOME=/home/spark
COPY download_jars.sh /download_jars.sh
RUN chmod +x /download_jars.sh && /download_jars.sh
COPY iceberg-spark-runtime.jar /opt/spark/jars/iceberg-spark-runtime-4.0_2.13-1.10.1.jar
COPY flight_prediction_2.13-0.1.jar /app/jars/flight_prediction_2.13-0.1.jar
COPY models/ /app/models/
RUN cp /home/spark/.ivy2/jars/*.jar /opt/spark/jars/
USER spark
