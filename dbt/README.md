# dbt

The `dbt` component owns warehouse transformations, tests, and dbt project
configuration.

Transformation logic belongs here instead of Airflow or extract/load scripts.
Targets, schemas, and model organization should be designed for production-scale
growth across many domains and thousands of models.

Pipeline-specific model behavior should live near the relevant dbt models,
tests, or domain docs instead of accumulating in this component README.

## Project Layout

The first dbt setup step is the local runtime only. Install and verify the dbt
CLI before creating a dbt project directory.

The dbt project files are created with `dbt init`. Do not hand-create the
generated project skeleton before the CLI works locally.

Profiles, sources, models, seeds, tests, and dbt-specific cloud resources are
added after project initialization, in the commits that need them.

There are two setup paths:

- First-time repository initialization creates `dbt/data_warehouse/` with
  `dbt init`, then removes dbt's starter tutorial files before committing.
- Existing-checkout setup starts from the committed `dbt/data_warehouse/`
  project and installs the locked local runtime.

## Local Runtime Setup

Check for `uv` before working in this component. Install it only when the
command is missing on the workstation.

```bash
if command -v uv >/dev/null; then
  uv --version
else
  curl -LsSf https://astral.sh/uv/install.sh | sh
  uv --version
fi
```

Create or refresh the lockfile when dependencies intentionally change, then
install from the locked environment:

```bash
cd dbt
uv lock
uv sync --locked
```

Run the first local verification from the component directory:

```bash
cd dbt

uv run dbt --version
```

## First-Time Repository Initialization

This path applies only while the repository is being initialized and
`dbt/data_warehouse/` does not exist yet.

Initialize the project:

```bash
cd dbt

uv run dbt init
```

Use these prompt responses:

```text
Enter a name for your project (letters, digits, underscore): data_warehouse
The profile data_warehouse already exists in ~/.dbt/profiles.yml. Continue and overwrite it? [y/N]: n
```

Answer `n` when dbt asks to overwrite an existing global profile. This project
does not use `~/.dbt/profiles.yml` as the committed or preferred local profile
location.

Clean up the generated starter project before committing:

- Delete `data_warehouse/models/example/` and the tutorial files inside it,
  including `my_first_dbt_model.sql`, `my_second_dbt_model.sql`, and
  `schema.yml` when dbt generates them.
- Remove the generated `models.data_warehouse.example` configuration from
  `data_warehouse/dbt_project.yml`.
- Replace the generated `data_warehouse/README.md` starter text with this
  project's dbt ownership notes.
- Keep the standard dbt directories: `analyses/`, `macros/`, `models/`,
  `seeds/`, `snapshots/`, and `tests/`.
- Add `.gitkeep` files only for empty standard directories that Git must track.
- Keep `data_warehouse/.gitignore` for dbt-generated `target/`,
  `dbt_packages/`, and `logs/`.
- Leave generated local runtime files untracked, including `dbt/.venv/`,
  `dbt/logs/`, `data_warehouse/target/`, and `data_warehouse/dbt_packages/`.

The actual local `profiles.yml` belongs outside the repository under the
project secrets directory once profile setup is needed.

## Existing-Checkout Setup

When `dbt/data_warehouse/` already exists in the repository, the project has
already been initialized. Set up the local runtime from the lockfile and verify
the CLI:

```bash
cd dbt
uv sync --locked
uv run dbt --version
```
