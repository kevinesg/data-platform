# scripts

The `scripts` component owns extract/load commands and source-specific pipeline
code.

This component should be terminal-testable without Airflow. Airflow can
orchestrate script commands later, but extraction and loading logic belongs here.

Source-specific contracts and commands should live near the relevant pipeline
implementation instead of accumulating in this component README.

## Runtime Contract

Scripts should expose explicit terminal commands for each major extract/load
step. A command should be runnable locally first, then callable by Airflow later
through the same stable interface.

Keep task boundaries retryable:

- extract from a source into a durable staging location.
- load staged data into warehouse raw tables.
- clean up completed staging files according to a documented retention policy.

For production-scale sources, do not assume source data, staged files, or
warehouse tables fit in local memory. Prefer chunked reads, durable staged files,
warehouse-side comparisons, and idempotent run identifiers.

Runtime configuration should come from environment variables or an environment
file used only for local development. Do not commit real `.env` files,
credentials, source exports, or warehouse data.

## Local Setup

Run scripts component commands from the `scripts/` directory:

```bash
cd scripts
uv sync
cp .env.example .env
```

Use Python 3.12 for this component. Keep dependencies component-local so scripts,
dbt, and Airflow can evolve independently.

The local `.env` file is only for developer convenience. Runtime environments
receive configuration through environment variables or the deployment platform's
secret manager.

## Validation

Run formatting, linting, and tests from `scripts/` as those checks are added:

```bash
uv run python validate_config.py
uv run ruff check .
uv run pytest
```

This component starts with project scaffolding only. Source-specific runtime
dependencies, commands, schemas, and tests should be added with the pipeline
features that need them.

## Design Notes

Keep extract/load logic outside Airflow. Airflow owns orchestration, scheduling,
retries, and task dependencies; this component owns source interaction, staging,
warehouse load behavior, and source-specific validation.

Add dependencies only when a pipeline needs them. Helper functions are useful
when they represent a meaningful workflow step or remove real repetition. Small
one-line wrappers around library calls are usually unnecessary.

## Pipeline Docs

Pipeline-specific contracts and commands should live near the relevant pipeline
implementation, for example under `pipelines/<source>/`. The importable runtime
source tree should not become the home for operational notes.
