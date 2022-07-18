ARG AIRFLOW_BASE_IMAGE="apache/airflow:2.3.3-python3.8"
FROM ${AIRFLOW_BASE_IMAGE}

COPY .env /tmp/.env
COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

USER root
RUN mkdir -p /data && chown airflow /data

USER airflow
