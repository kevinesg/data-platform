# dbt

The `dbt` component owns warehouse transformations, tests, and dbt project
configuration.

Transformation logic belongs here instead of Airflow or extract/load scripts.
Targets, schemas, and model organization should be designed for production-scale
growth across many domains and thousands of models.

Pipeline-specific model behavior should live near the relevant dbt models,
tests, or domain docs instead of accumulating in this component README.
