# data-platform

`data-platform` is a loosely coupled monorepo for data platform runtime and
pipeline code.

The repository is organized by component:

```text
data-platform/
  scripts/   # extract/load commands and source-specific pipeline code
  dbt/       # warehouse transformations, tests, and dbt project configuration
  airflow/   # orchestration runtime and DAGs
  metabase/  # analytics service runtime/configuration
```

Each component is developed as an independent project with its own README,
dependencies, tests, and runtime contract. Components interact through Docker
images, APIs, warehouse tables, or documented artifacts rather than relative
imports or production runtime bind mounts.

## Production-Scale Assumption

Design decisions in this repository assume a real production data engineering
environment: hundreds of Airflow DAGs, thousands of dbt models, large warehouse
tables, multiple data sources, bounded CI/CD, and operational ownership by a
functional data engineering team.

The first pipeline can be small, but the platform architecture should not be
small-minded. MVP shortcuts are acceptable only when they keep the core workflow
moving and are documented as temporary.

## MVP Direction

The first end-to-end pipeline is the `personal_finance` ELT pipeline:

```text
personal finance source
  -> scripts extract/load
  -> dbt transformations
  -> Airflow orchestration
  -> Metabase visibility
```

The initial development order is:

1. scripts local personal finance EL
2. dbt local personal finance models
3. scripts image
4. dbt image
5. Airflow local empty stack
6. Airflow runs scripts/dbt images
7. CI for the current components
8. QA/prod deployment path
9. Metabase

## Environments

The platform promotes changes through:

```text
dev -> QA -> prod
```

Environment-specific secrets and `.env` files are not committed. Local dev uses
component `.env.example` files and local image tags. Deployed QA/prod should use
external environment files, immutable registry image tags, and no runtime
source-code bind mounts.

## Images

Runtime components are packaged as Docker images. Local development uses short
local tags:

```text
data-platform-scripts:dev
data-platform-dbt:dev
data-platform-airflow:dev
```

Registry images use GitHub Container Registry names:

```text
ghcr.io/kevinesg/data-platform-scripts
ghcr.io/kevinesg/data-platform-dbt
ghcr.io/kevinesg/data-platform-airflow
```
