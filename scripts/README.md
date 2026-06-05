# scripts

The `scripts` component owns extract/load commands and source-specific pipeline
code.

This component should be terminal-testable without Airflow. Airflow can
orchestrate script commands later, but extraction and loading logic belongs here.

Source-specific contracts and commands should live near the relevant pipeline
implementation instead of accumulating in this component README.

## Local Setup

Run scripts component commands from the `scripts/` directory:

```bash
cd scripts
uv sync
```

Use Python 3.12 for this component. Keep dependencies component-local so scripts,
dbt, and Airflow can evolve independently.

## Validation

Run formatting, linting, and tests from `scripts/` as those checks are added:

```bash
uv run ruff check .
uv run pytest
```

This component starts with project scaffolding only. Source-specific runtime
dependencies, commands, schemas, and tests should be added with the pipeline
features that need them.
