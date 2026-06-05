# airflow

The `airflow` component owns orchestration runtime configuration and DAGs.

Airflow should schedule work, define dependencies, set retries/timeouts, and
invoke stable runtime contracts. It should not contain extract/load business
logic, dbt transformation logic, or imports from sibling component source trees.

DAGs should be designed as if many teams and hundreds of DAGs will share the
same orchestration environment.
