from __future__ import annotations

from datetime import datetime, timedelta

from airflow.sdk import DAG

from _wremotely import create_clickhouse_dbt_build_task


with DAG(
    dag_id="build__wremotely_clickhouse",
    description="Run the isolated wremotely ClickHouse dbt graph against loaded raw relations.",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    tags=["wremotely", "clickhouse", "dbt", "on-prem"],
) as dag:
    dbt_build = create_clickhouse_dbt_build_task()
