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

RUN pip install --no-cache-dir apache-airflow-providers-docker==4.5.7

COPY --chown=airflow:root dags /opt/airflow/dags
COPY --chown=airflow:root validate_dags.py /opt/airflow/validate_dags.py
