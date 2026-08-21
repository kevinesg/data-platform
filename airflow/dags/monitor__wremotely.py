from __future__ import annotations

from datetime import datetime, timedelta

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from _monitor import check_airflow_freshness
from _wremotely import (
    ENVIRONMENT,
    WREMOTELY_DOCKER_NETWORK_MODE,
    WREMOTELY_ETL_IMAGE,
    WREMOTELY_OUTPUT_ROOT_CONTAINER_PATH,
    WREMOTELY_WAREHOUSE_POOL,
    WREMOTELY_WAREHOUSE_ROOT_CONTAINER_PATH,
    WREMOTELY_DAG_RUN_TIMESTAMP,
    dag_schedule,
    docker_task,
    etl_command,
    onprem_wremotely_environment,
    onprem_wremotely_mounts,
    onprem_wremotely_private_environment,
    optional_env,
    create_onprem_publication_status_task,
)

MONITOR_RUN_ID = f"{WREMOTELY_DAG_RUN_TIMESTAMP}-wremotely-monitor"

with DAG(
    dag_id="monitor__wremotely",
    description=(
        "Check on-prem Wremotely Airflow freshness, ClickHouse publication freshness, "
        "and local storage headroom."
    ),
    start_date=datetime(2026, 1, 1),
    schedule=dag_schedule(ENVIRONMENT, "WREMOTELY_MONITORING_SCHEDULE"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=1),
    tags=["wremotely", "monitoring", "on-prem", "clickhouse"],
) as dag:
    check_freshness = PythonOperator(
        task_id="check_airflow_freshness",
        python_callable=check_airflow_freshness,
        execution_timeout=timedelta(minutes=10),
    )
    check_warehouse = docker_task(
        task_id="check_clickhouse_and_storage",
        image=WREMOTELY_ETL_IMAGE,
        command=etl_command(
            "--step",
            "monitor",
            "--run-id",
            MONITOR_RUN_ID,
            "--output-root",
            WREMOTELY_OUTPUT_ROOT_CONTAINER_PATH,
            "--warehouse-root",
            WREMOTELY_WAREHOUSE_ROOT_CONTAINER_PATH,
            "--monitor-maximum-publication-age-minutes",
            optional_env("WREMOTELY_MONITOR_MAX_PUBLICATION_AGE_MINUTES", "900"),
            "--monitor-minimum-free-percent",
            optional_env("WREMOTELY_MONITOR_MINIMUM_FREE_PERCENT", "10"),
            "--monitor-minimum-free-bytes",
            optional_env("WREMOTELY_MONITOR_MINIMUM_FREE_BYTES", "10737418240"),
        ),
        environment=onprem_wremotely_environment,
        private_environment=onprem_wremotely_private_environment,
        mounts=onprem_wremotely_mounts,
        execution_timeout=timedelta(minutes=15),
        network_mode=WREMOTELY_DOCKER_NETWORK_MODE,
        pool=WREMOTELY_WAREHOUSE_POOL,
    )
    check_convergence = create_onprem_publication_status_task()

    [check_freshness, check_warehouse] >> check_convergence
