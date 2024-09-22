# Use the official Airflow image as the base image
FROM apache/airflow:2.7.3-python3.10

ENV AIRFLOW_HOME=/opt/airflow

USER root

RUN apt-get update && \
    apt-get install -y openjdk-11-jre-headless procps wget git && \
    apt-get clean;

COPY scripts scripts
RUN chmod +x scripts

USER airflow

# copy the requirements file to the docker image
COPY requirements.txt requirements.txt

# run the upgrade command to have the latest pip version
RUN pip install --user --upgrade pip

# run the pip install to get all the python dependencies installed
RUN pip install --no-cache-dir --user -r requirements.txt

WORKDIR $AIRFLOW_HOME