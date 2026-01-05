FROM apache/spark:4.1.0-scala2.13-java21-python3-r-ubuntu

USER root

RUN mkdir -p /opt/jobs /opt/data /opt/jars

WORKDIR /opt/jobs

COPY requirements.txt .

RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y --no-install-recommends wget && \
    wget -q https://jdbc.postgresql.org/download/postgresql-42.7.6.jar \
         -O /opt/jars/postgresql-42.7.6.jar && \
    pip3 install --no-cache-dir -r requirements.txt && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY ./spark /opt/jobs/
COPY ./data /opt/data/
COPY ./jars /opt/jars/

USER spark
