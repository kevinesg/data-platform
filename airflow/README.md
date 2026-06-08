# airflow

The `airflow` component owns orchestration runtime configuration and DAGs.

Airflow schedules work, defines dependencies, sets retries/timeouts, and invokes
stable runtime contracts. It does not contain extract/load business logic, dbt
transformation logic, or imports from sibling component source trees.

DAGs are designed as if many teams and hundreds of DAGs will share the same
orchestration environment.

## Setup Status

Airflow setup commands are intentionally not present yet. The platform is being
rebuilt in order: scripts first, then dbt, then runtime images, then Airflow.

Use the root `README.md` for the current setup flow. This README becomes the
Airflow end-to-end setup entrypoint when the Airflow component is implemented.
