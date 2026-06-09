FROM apache/airflow:3.2.2-python3.12

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        curl \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /opt/airflow/auth /opt/airflow/logs \
    && chown -R airflow:root /opt/airflow/auth /opt/airflow/logs \
    && chmod -R 775 /opt/airflow/auth /opt/airflow/logs

USER airflow

COPY --chown=airflow:root dags /opt/airflow/dags
