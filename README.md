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
  deploy/    # shared environment bootstrap and deployment documentation
```

Each component is developed as an independent project with its own README,
dependencies, tests, and runtime contract. Components interact through Docker
images, APIs, warehouse tables, or documented artifacts rather than relative
imports or production runtime bind mounts.

## Production-Scale Assumption

Design decisions in this repository assume a production data engineering
environment: hundreds of Airflow DAGs, thousands of dbt models, large warehouse
tables, multiple data sources, bounded CI/CD, and operational ownership by a
functional data engineering team.

The first pipeline can be small, but the platform architecture is not reduced
to one-pipeline assumptions. MVP shortcuts are acceptable only when they keep
the core workflow moving and are documented as temporary.

## Initial Delivery Direction

The platform is developed vertically so each boundary is validated before the
next component depends on it:

1. scripts source extraction and raw loading
2. dbt transformations and tests
3. scripts and dbt runtime images
4. Airflow orchestration
5. CI for implemented components
6. QA and prod deployment paths
7. analytics service integration

## Environments

The platform promotes changes through:

```text
dev -> QA -> prod
```

Environment-specific secrets and `.env` files are not committed. A development
workstation defaults to `$HOME/dev/secrets/data-platform/.env`; the path remains
configurable through `DATA_PLATFORM_SECRETS_DIR` or
`DATA_PLATFORM_ENV_FILE`. QA and prod configuration belongs on the matching
deployment platform or authorized administration host, not on a development
workstation. Local dev uses component-specific service-account JSON files stored
under the external secrets directory and local image tags. Deployed QA/prod use
immutable registry image tags, environment-specific service-account JSON files
stored on the deployment platform, and no runtime source-code bind mounts.

## Setup Flow

Start here before running component commands. The root README explains the
repository boundaries and the order of setup documents; detailed commands live
in the owning README so each command sequence has one canonical home.

1. Read this README for repository boundaries, environment promotion, and
   production-scale assumptions.
2. Read `deploy/README.md` for workstation tools, CLI authentication, shared
   dev project topology, and platform bootstrap rules.
3. Run `deploy/README.md` **Platform Bootstrap** only when creating or repairing
   shared project resources. Team members joining an existing dev environment
   skip platform bootstrap after receiving their assigned component workspace
   values.
4. Run the relevant component README end to end:
   - `scripts/README.md` for extract/load service account, landing bucket, raw
     dataset, local credentials, runtime setup, and scripts verification.
   - `dbt/README.md` for dbt project setup, dbt service account, datasets,
     external profile, local credentials, and `dbt debug`.
   - `airflow/README.md` and `metabase/README.md` document boundaries for now;
     setup commands will be added when those components are implemented.
5. Run source-specific or domain-specific docs only after the owning component
   setup passes. For example, scripts pipeline commands live under
   `scripts/pipelines/`.
6. Validate changes against dev first, then QA, then prod. Do not run
   cloud-connected commands until the matching documented prerequisites are
   complete.

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
