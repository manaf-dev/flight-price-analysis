FROM apache/spark:4.1.0-scala2.13-java21-python3-r-ubuntu

USER root

WORKDIR /opt/jobs

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ./spark /opt/jobs/
COPY ./data /opt/data/
COPY ./jars /opt/jars/

USER spark
