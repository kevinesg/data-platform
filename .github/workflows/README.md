# GitHub Actions Workflows

This directory owns repository-level CI/CD workflow files. Keep the workflows
split by component so the platform can grow without every pull request paying
for every component check.

## Current Workflows

| Workflow | File | When it runs | What it proves |
| --- | --- | --- | --- |
| `scripts-ci` | `scripts-ci.yml` | Pull requests and pushes to `main` that change scripts runtime files, plus manual dispatch | Installs the locked scripts runtime, runs lint/tests, compiles Python files, and builds the scripts Docker image without publishing it. |
| `dbt-ci` | `dbt-ci.yml` | Pull requests and pushes to `main` that change dbt runtime files, plus manual dispatch | Installs the locked dbt runtime, parses the dbt project with non-secret placeholder values, and builds the dbt Docker image without publishing it. |
| `airflow-ci` | `airflow-ci.yml` | Pull requests and pushes to `main` that change Airflow runtime files, plus manual dispatch | Builds the Airflow Docker image and imports packaged DAG files with non-secret placeholder values. |
| `publish-images` | `publish-images.yml` | Successful component CI runs on `main`, plus manual dispatch | Publishes immutable component image tags to GHCR for deployed environments. |
| `deploy-qa` | `deploy-qa.yml` | Manual dispatch | Deploys the selected Git ref to the QA host using the latest matching immutable runtime images. |
| `deploy-prod` | `deploy-prod.yml` | Manual dispatch with `prod` environment approval | Promotes the QA image manifest to prod, validates prod dbt compile, and recreates the prod Airflow stack. |

## CI/CD Boundary

CI validates code, configuration parsing, and image buildability. Pull-request CI
must not depend on live cloud credentials, developer-specific secrets, BigQuery
datasets, GCS buckets, or Google Sheets access.

CD starts by publishing immutable registry images after the matching component
CI has passed on `main`. QA deployment uses those immutable images on a
self-hosted deployment runner. Prod deployment promotes the QA image manifest
after manual approval through the GitHub `prod` environment.

Published runtime image tags use this form:

```text
ghcr.io/kevinesg/data-platform-<component>:sha-<commit-sha>
```

Do not use mutable `latest`, `qa`, or `prod` tags for deployed runtime
selection. Deployed environments should pin image refs through their external
`images.env` manifest.

Image publishing also depends on the repository Actions token settings. In the
GitHub repository, verify **Settings** > **Actions** > **General** >
**Workflow permissions** is set to **Read and write permissions** before running
`publish-images`. The workflow still narrows its own token scope with
`permissions: contents: read` and `packages: write`.

## Syntax Guide

GitHub Actions workflow files are YAML files with a few important top-level
sections:

- `name` is the human-readable workflow name shown in GitHub.
- `on` defines events that trigger the workflow. This repo uses
  `pull_request`, `push`, and `workflow_dispatch`.
- `paths` narrows a workflow to relevant files. Patterns beginning with `!`
  exclude files from the earlier includes.
- `concurrency` cancels older in-progress runs for the same branch/ref.
- `permissions` keeps the default GitHub token scoped to the minimum needed by
  the workflow.
- `jobs` contains one or more jobs. Jobs run on a GitHub runner such as
  `ubuntu-latest`.
- `steps` are the ordered commands inside a job.
- `uses` calls a reusable action from another repository.
- `run` executes shell commands on the runner.

## Action Versions

Use supported maintained tags, and verify that the tag exists before adding it.
Official GitHub and Docker actions generally publish major-version tags such as
`actions/checkout@v6` or `docker/build-push-action@v7`.

Some actions intentionally do not publish major-version tags. For example,
`astral-sh/setup-uv` no longer publishes `@v8` or `@v8.0`; use an immutable
release tag such as `astral-sh/setup-uv@v8.2.0`, or pin to a full commit SHA
when stronger supply-chain pinning is required.

## Local Validation

There is no perfect local clone of the GitHub-hosted Actions environment. The
local workflow should catch syntax and component failures early, while the PR
run remains the authoritative check for GitHub runner behavior.

Run workflow static checks first:

```bash
actionlint
```

Install `actionlint` once using a package manager, a prebuilt release binary, or
Go. If `actionlint` is not installed locally but Docker is available, run the
official container instead:

```bash
docker run --rm -v "$PWD:/repo" --workdir /repo rhysd/actionlint:latest -color
```

`actionlint` catches invalid workflow syntax, missing or unexpected keys,
expression mistakes, action input/output mistakes, and common shell/security
issues.

Then run the component checks documented by the changed component's README:

- `scripts/README.md` for scripts runtime validation.
- `dbt/README.md` for dbt parsing/build validation.
- `airflow/README.md` for Airflow image and DAG import validation.

For workflow emulation, `act` can run GitHub Actions locally through Docker:

```bash
act pull_request -W .github/workflows/scripts-ci.yml
act pull_request -W .github/workflows/dbt-ci.yml
act pull_request -W .github/workflows/airflow-ci.yml
```

Install `act` using one of its official package-manager or prebuilt-binary
options. `act` requires Docker Engine for containerized local workflow runs.

Treat `act` as an optional fast-feedback tool, not as a replacement for the
GitHub PR run. Runner images, Docker Buildx behavior, GitHub cache services,
event payloads, and hosted-runner details can differ locally.

Do not use local workflow emulation to push GHCR images. Local checks can prove
the image builds; publishing is intentionally limited to GitHub Actions on
validated commits.
