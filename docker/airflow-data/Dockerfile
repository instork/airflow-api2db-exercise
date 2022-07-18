ARG AIRFLOW_BASE_IMAGE="apache/airflow:2.3.3-python3.8"
FROM ${AIRFLOW_BASE_IMAGE}

USER root
RUN mkdir -p /data && chown airflow /data

USER airflow
 