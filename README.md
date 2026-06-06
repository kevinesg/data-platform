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

The first pipeline can be small, but the platform architecture should not be
small-minded. MVP shortcuts are acceptable only when they keep the core workflow
moving and are documented as temporary.

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
workstation. Local dev uses keyless Application Default Credentials and local
image tags. Deployed QA/prod should use immutable registry image tags, workload
credentials supplied by the runtime, and no runtime source-code bind mounts.

## Setup Path

Use this as the ordered entrypoint. Detailed commands remain in component and
deployment docs so the root README stays focused on repository-wide contracts.

1. Read this README to understand repository boundaries, environment promotion,
   and production-scale assumptions.
2. Install the required workstation tools from their official documentation:
   [Git](https://git-scm.com/downloads),
   [GitHub CLI](https://cli.github.com/),
   [Google Cloud CLI](https://cloud.google.com/sdk/docs/install),
   [uv](https://docs.astral.sh/uv/getting-started/installation/), and
   [Docker](https://docs.docker.com/engine/install/).
3. Follow `deploy/README.md` to authenticate the CLIs. Platform administrators
   create shared projects; each developer configures an isolated workspace
   inside dev.
4. Follow component READMEs for local setup and validation. Source-specific
   access and commands remain in the matching pipeline documentation.
5. Validate changes against dev first, then QA, then prod. Do not run
   cloud-connected commands until the matching documented prerequisites are
   complete.

As setup coverage grows beyond this outline, add a committed public docs
entrypoint and keep this section as the short index.

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
