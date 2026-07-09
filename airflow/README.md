# airflow

The `airflow` component owns orchestration runtime configuration and DAGs.

Airflow schedules work, defines dependencies, sets retries/timeouts, and invokes
stable runtime contracts. It does not contain extract/load business logic, dbt
transformation logic, or imports from sibling component source trees.

DAGs are designed as if many teams and hundreds of DAGs will share the same
orchestration environment.

Use this README after reading the root [README.md](../README.md) and completing
the applicable shared setup in [deploy/README.md](../deploy/README.md).

## Outline

- [Local Setup](#local-setup)
- [Local Commands](#local-commands)
- [DAGs](#dags)
- [Failure Alerts](#failure-alerts)
- [Design Notes](#design-notes)

## Local Setup

Run commands from the repository root unless a block changes directories.

```bash
export DATA_PLATFORM_SECRETS_DIR="${DATA_PLATFORM_SECRETS_DIR:-$HOME/dev/secrets/data-platform}"
export DATA_PLATFORM_ENV_FILE="${DATA_PLATFORM_ENV_FILE:-$DATA_PLATFORM_SECRETS_DIR/.env}"
export DATA_PLATFORM_ENV_PARENT="$(dirname "$DATA_PLATFORM_ENV_FILE")"

mkdir -p "$DATA_PLATFORM_ENV_PARENT"
chmod 700 "$DATA_PLATFORM_ENV_PARENT"

if test -f "$DATA_PLATFORM_ENV_FILE"; then
  echo "Environment file already exists: $DATA_PLATFORM_ENV_FILE"
else
  cp airflow/.env.example "$DATA_PLATFORM_ENV_FILE"
  chmod 600 "$DATA_PLATFORM_ENV_FILE"
fi
```

Add or verify the complete Airflow values in the external dev environment file.
If another component already created the file, keep the existing values and
merge every Airflow value from `airflow/.env.example`. Do not create
`airflow/.env`.

```dotenv
AIRFLOW_UID=50000
DOCKER_GID=0
AIRFLOW_COMPOSE_PROJECT=data-platform-airflow-dev
AIRFLOW_API_PORT=8080
DATA_PLATFORM_AIRFLOW_IMAGE=data-platform-airflow:dev
DATA_PLATFORM_SCRIPTS_IMAGE=data-platform-scripts:dev
DATA_PLATFORM_DBT_IMAGE=data-platform-dbt:dev

POSTGRES_USER=airflow
POSTGRES_PASSWORD=<local-password>
POSTGRES_DB=airflow

AIRFLOW_ADMIN_USERNAME=admin
AIRFLOW_SECRET_KEY=<generated-secret>
AIRFLOW_JWT_SECRET=<generated-secret>
AIRFLOW_FERNET_KEY=<generated-fernet-key>

# Optional, external only:
# AIRFLOW__API__BASE_URL=http://localhost:8080
# DATA_PLATFORM_AIRFLOW_FAILURE_ALERT_WEBHOOK_URL=<Slack incoming webhook URL>
```

Set `AIRFLOW_UID` to the host user ID:

```bash
id -u
```

Set `DOCKER_GID` to the host Docker socket group ID:

```bash
stat -c '%g' /var/run/docker.sock
```

Generate API/JWT secret values with Python:

```bash
python -c "import secrets; print(secrets.token_urlsafe(48))"
```

Generate the Fernet key separately because Airflow requires a Fernet-formatted
key for encrypting connection passwords and variables in the metadata database:

```bash
python -c "import base64, os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())"
```

Keep `AIRFLOW_FERNET_KEY` stable after creating real Airflow connections or
variables. Changing it later without key rotation can make existing encrypted
values unreadable.

Validate the external environment file before running Docker:

```bash
cd airflow
python validate_config.py --env-file "$DATA_PLATFORM_ENV_FILE"
```

Do not continue to Docker Compose until validation prints:

```text
airflow config OK
```

## Local Commands

Validate and start the local dev stack:

```bash
cd airflow

docker compose --env-file "$DATA_PLATFORM_ENV_FILE" -f docker-compose.yml -f docker-compose.dev.yml config --quiet
docker compose --env-file "$DATA_PLATFORM_ENV_FILE" -f docker-compose.yml -f docker-compose.dev.yml build
docker compose --env-file "$DATA_PLATFORM_ENV_FILE" -f docker-compose.yml -f docker-compose.dev.yml up -d
docker compose --env-file "$DATA_PLATFORM_ENV_FILE" -f docker-compose.yml -f docker-compose.dev.yml ps
```

If `airflow-init` exits with status `1`, inspect the initialization logs before
changing the Compose files:

```bash
docker compose --env-file "$DATA_PLATFORM_ENV_FILE" -f docker-compose.yml -f docker-compose.dev.yml logs airflow-init
```

If the logs show `FATAL: password authentication failed for user "airflow"`,
another Postgres authentication, database, user, or password error, or if
`POSTGRES_USER`, `POSTGRES_PASSWORD`, or `POSTGRES_DB` changed after a previous
local start, do not rerun `build` or `up` yet. Remove only this local Airflow
Compose stack and its metadata volumes, then rerun the validation and start
commands:

```bash
docker compose --env-file "$DATA_PLATFORM_ENV_FILE" -f docker-compose.yml -f docker-compose.dev.yml down -v --remove-orphans
```

Postgres applies `POSTGRES_USER`, `POSTGRES_PASSWORD`, and `POSTGRES_DB` only
when initializing an empty data directory. A later env-file edit does not change
the credentials stored in the existing `postgres-data` volume. Rebuilding the
Airflow image also does not change credentials stored in that Postgres volume.

The Airflow UI/API is exposed on `http://localhost:8080` by default. Runtime
logs and local Simple Auth Manager passwords are stored in named Docker volumes
so host directory ownership does not block Airflow from writing logs.

Airflow 3's Simple Auth Manager generates the local user's password and stores
it in a named Docker volume. Read it after the API server starts:

```bash
docker compose --env-file "$DATA_PLATFORM_ENV_FILE" -f docker-compose.yml -f docker-compose.dev.yml exec api-server cat /opt/airflow/auth/simple_auth_manager_passwords.json.generated
```

That password persists across normal `docker compose up -d` rebuilds because
the password file is stored in the `airflow-auth` named volume. It resets if the
volume is removed.

Check DAG parsing after local startup:

```bash
docker compose --env-file "$DATA_PLATFORM_ENV_FILE" -f docker-compose.yml -f docker-compose.dev.yml exec scheduler airflow dags list
docker compose --env-file "$DATA_PLATFORM_ENV_FILE" -f docker-compose.yml -f docker-compose.dev.yml exec scheduler airflow dags list-import-errors
```

Stop the stack without deleting metadata/log volumes:

```bash
docker compose --env-file "$DATA_PLATFORM_ENV_FILE" -f docker-compose.yml -f docker-compose.dev.yml down
```

Completely remove only this local Airflow Compose stack, including its metadata
and log volumes:

```bash
docker compose --env-file "$DATA_PLATFORM_ENV_FILE" -f docker-compose.yml -f docker-compose.dev.yml down -v --remove-orphans
```

## DAGs

The Airflow image copies DAG files from `airflow/dags/` so CI and registry
images are self-contained. Local development uses `docker-compose.dev.yml` to
bind-mount `./dags` for faster iteration.

Private DAG helper modules use a leading underscore, such as
`airflow/dags/_alerting.py`. The `.airflowignore` file excludes `_*.py` from
Airflow DAG discovery while keeping those modules importable by DAG files.
Use the same pattern for future helper modules such as `_dag_factory.py`.

DAGs that launch component images use DockerOperator through the mounted host
Docker socket. This is local runtime support for image-contract validation; DAGs
must still keep extract/load logic in `scripts` and transform logic in `dbt`.

For DAGs that pass the Airflow DAG run ID into extract/load commands, retries
and task clears are expected to replay the same staged source snapshot. Trigger
a new DAG run when source data changed and a fresh extract is required.

Do not add a README for every DAG. Add a DAG-specific runbook only when the DAG
has operational behavior that is not already covered by the component README or
the owning source/dbt documentation.
When such a runbook exists, keep it beside the DAG under `airflow/dags/`.

## Failure Alerts

Airflow task failure alerts are implemented as a reusable callback in
[dags/_alerting.py](dags/_alerting.py). The callback sends only failure alerts,
never success alerts, and delivery is best effort. If the webhook is missing or
temporarily unavailable, Airflow prints the alert failure and keeps the original
task failure as the source of truth.

Create the Slack incoming webhook URL:

1. Open [Slack API Apps](https://api.slack.com/apps) and create an app for the
   workspace, or open an existing platform alerts app.
2. Open **Incoming Webhooks** for the app and turn on **Activate Incoming
   Webhooks**.
3. Select **Add New Webhook to Workspace**, choose the channel that should
   receive Airflow alerts, and authorize the app.
4. Copy the generated webhook URL from **Webhook URLs for Your Workspace**.
   Slack treats this URL as a secret; keep it out of Git.

Configure alert delivery through the external Airflow environment file:

```dotenv
DATA_PLATFORM_AIRFLOW_FAILURE_ALERT_WEBHOOK_URL=<Slack incoming webhook URL>
```

Use a dedicated test Slack channel and webhook for dev or QA alert testing.
Leave `DATA_PLATFORM_AIRFLOW_FAILURE_ALERT_WEBHOOK_URL` unset for day-to-day
local development or manual-only QA if alerts would create noise. Do not commit
webhook URLs. If a webhook URL is exposed in chat, logs, screenshots, or Git,
revoke it in Slack and create a replacement.

Alert payloads include DAG id, task id, run id, failure timestamp, a clickable
task-log link when available, exception summary, and recent container log lines
when Airflow exposes them on the exception.

Set `AIRFLOW__API__BASE_URL` in the external environment file when alert links
should open a specific Airflow UI hostname. For local development, use
`http://localhost:8080` unless `AIRFLOW_API_PORT` is different.

Dev alert testing is useful before QA because it proves the callback code,
external env value, Airflow import path, and Slack webhook delivery. QA testing
is still required after deploy because it proves the packaged image and deployed
environment files.

Airflow callbacks run only when a DAG or task state changes because a worker
executed it. Manually marking a task as failed from the UI or CLI updates
metadata state but does not execute the failure callback. Use the manual
callback test below to verify Slack delivery, or trigger a real task execution
that fails.

Test alert delivery from the local Airflow stack after setting the webhook URL
and `AIRFLOW__API__BASE_URL=http://localhost:8080` in the external environment
file. Recreate the stack so the containers read the updated env file:

```bash
cd airflow

docker compose --env-file "$DATA_PLATFORM_ENV_FILE" -f docker-compose.yml -f docker-compose.dev.yml up -d --force-recreate --remove-orphans

docker compose --env-file "$DATA_PLATFORM_ENV_FILE" -f docker-compose.yml -f docker-compose.dev.yml exec -T scheduler python - <<'PY'
import sys
from types import SimpleNamespace

sys.path.insert(0, "/opt/airflow/dags")

from _alerting import send_failure_alert

send_failure_alert(
    {
        "task_instance": SimpleNamespace(
            dag_id="alert_test",
            task_id="manual_test",
            log_url="http://localhost:8080",
        ),
        "dag_run": SimpleNamespace(run_id="manual_alert_test"),
        "ts": "manual test",
        "exception": RuntimeError("manual Slack alert test"),
    }
)
PY
```

The manual `python` process adds `/opt/airflow/dags` to `sys.path` because it
does not run through Airflow's DAG importer.

## Design Notes

Airflow owns orchestration concerns: schedules, dependencies, retries, and task
commands. Extract/load logic lives in `scripts`, and transformation logic lives
in `dbt`.

The base `docker-compose.yml` is the deployed shape and does not bind-mount DAG
source. The local override builds the image from the local Dockerfile and mounts
`./dags` only for development.
