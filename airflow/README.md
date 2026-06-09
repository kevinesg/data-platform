# airflow

The `airflow` component owns orchestration runtime configuration and DAGs.

Airflow schedules work, defines dependencies, sets retries/timeouts, and invokes
stable runtime contracts. It does not contain extract/load business logic, dbt
transformation logic, or imports from sibling component source trees.

DAGs are designed as if many teams and hundreds of DAGs will share the same
orchestration environment.

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

Check the empty DAG environment:

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

DAGs that launch component images use DockerOperator through the mounted host
Docker socket. This is local runtime support for image-contract validation; DAGs
must still keep extract/load logic in `scripts` and transform logic in `dbt`.

## Design Notes

Airflow owns orchestration concerns: schedules, dependencies, retries, and task
commands. Extract/load logic lives in `scripts`, and transformation logic lives
in `dbt`.

The base `docker-compose.yml` is the deployed shape and does not bind-mount DAG
source. The local override builds the image from the local Dockerfile and mounts
`./dags` only for development.
