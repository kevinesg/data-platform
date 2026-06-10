# metabase

The `metabase` component owns the analytics service runtime and operator
workflow.

Metabase connects to published warehouse tables as an analytics client. It stays
independent from Airflow, dbt, and scripts internals: no source-code bind
mounts, no imports from other components, and no dependency on dbt artifacts at
runtime.

The default deployment strategy is one prod Metabase instance. Metabase carries
durable application state such as users, dashboards, questions, collections,
permissions, and database connections, so the platform does not create matching
dev and QA Metabase instances unless there is a concrete review or permission
need. Local Compose is still useful for smoke testing runtime configuration.

## Outline

- [Command Flow](#command-flow)
- [Local Smoke Test](#local-smoke-test)
- [Local Commands](#local-commands)
- [Runtime Contract](#runtime-contract)
- [Runtime Hardening](#runtime-hardening)
- [Application Database](#application-database)
- [Warehouse Connections](#warehouse-connections)
- [CI/CD Scope](#cicd-scope)

## Command Flow

Use this README after reading the root [README.md](../README.md) and completing
the applicable shared setup in [deploy/README.md](../deploy/README.md). For an
existing dev environment, that usually means workstation tools and an external
environment file only.

Run commands from the `metabase/` component directory unless a block changes
directories:

```bash
cd metabase
```

## Local Smoke Test

Local Metabase is optional and mainly validates Compose configuration. Do not
create or commit a local `.env` file in this component directory.

```bash
export DATA_PLATFORM_SECRETS_DIR="${DATA_PLATFORM_SECRETS_DIR:-$HOME/dev/secrets/data-platform}"
export METABASE_ENV_FILE="${METABASE_ENV_FILE:-$DATA_PLATFORM_SECRETS_DIR/metabase.env}"

mkdir -p "$DATA_PLATFORM_SECRETS_DIR"
chmod 700 "$DATA_PLATFORM_SECRETS_DIR"

if test -f "$METABASE_ENV_FILE"; then
  echo "Metabase env file already exists: $METABASE_ENV_FILE"
else
  cp .env.example "$METABASE_ENV_FILE"
  chmod 600 "$METABASE_ENV_FILE"
fi
```

Edit the external `METABASE_ENV_FILE` and replace every `change-me` value. At
minimum:

```text
METABASE_COMPOSE_PROJECT=data-platform-metabase
METABASE_IMAGE=metabase/metabase:v0.62.1.2
METABASE_BIND_ADDRESS=127.0.0.1
METABASE_PORT=3000
MB_SITE_URL=http://localhost:3000
MB_DB_PASS=<local-metabase-app-db-password>
MB_ENCRYPTION_SECRET_KEY=<openssl-rand-base64-32-output>
```

Generate an encryption key before adding warehouse connections:

```bash
openssl rand -base64 32
```

Use a different `METABASE_PORT` if port `3000` is already in use. Keep
`METABASE_BIND_ADDRESS=127.0.0.1` for local-only access unless direct LAN access
is intentional.

## Local Commands

Validate and start Metabase:

```bash
DATA_PLATFORM_ENV_FILE="$METABASE_ENV_FILE" \
  docker compose --env-file "$METABASE_ENV_FILE" -f docker-compose.yml config --quiet

DATA_PLATFORM_ENV_FILE="$METABASE_ENV_FILE" \
  docker compose --env-file "$METABASE_ENV_FILE" -f docker-compose.yml pull

DATA_PLATFORM_ENV_FILE="$METABASE_ENV_FILE" \
  docker compose --env-file "$METABASE_ENV_FILE" -f docker-compose.yml up -d

DATA_PLATFORM_ENV_FILE="$METABASE_ENV_FILE" \
  docker compose --env-file "$METABASE_ENV_FILE" -f docker-compose.yml ps
```

The Metabase UI is exposed on `http://localhost:3000` by default. On first
startup, complete the setup wizard in the browser.

Stop the stack without deleting the application database volume:

```bash
DATA_PLATFORM_ENV_FILE="$METABASE_ENV_FILE" \
  docker compose --env-file "$METABASE_ENV_FILE" -f docker-compose.yml down
```

Do not use `docker compose down -v` unless you intentionally want to delete the
Metabase application database.

## Runtime Contract

The Compose runtime has two services:

```text
metabase
|-- postgres: Metabase application database
`-- metabase: Metabase web application
```

Metabase application database settings use `MB_DB_*` variables only. Do not use
shared `POSTGRES_USER`, `POSTGRES_PASSWORD`, or `POSTGRES_DB` names here because
those belong to the Airflow metadata database in the deployed runtime contract.
Keep Metabase settings in a separate external `metabase.env` file instead of
mixing them into the Airflow/dbt deploy `.env`.

The single deployed Metabase instance uses the same Compose file with an
external environment file:

```bash
METABASE_ENV_FILE="$HOME/secrets/data-platform/prod/metabase.env"

DATA_PLATFORM_ENV_FILE="$METABASE_ENV_FILE" \
  docker compose --env-file "$METABASE_ENV_FILE" -f docker-compose.yml config --quiet
```

Metabase uses the official pinned `METABASE_IMAGE`; it does not use the
repository `images.env` manifest because no repo-built Metabase image exists.

## Runtime Hardening

The service binds to `127.0.0.1` by default through `METABASE_BIND_ADDRESS`.
Expose deployed Metabase through a reverse proxy or Cloudflare Tunnel instead of
binding the container directly to every network interface. Set
`METABASE_BIND_ADDRESS=0.0.0.0` only when direct LAN access is intentional.

`MB_ENCRYPTION_SECRET_KEY` is required so Metabase can encrypt stored warehouse
connection details in its application database. Generate it before adding any
warehouse connections and keep it stable. If this key is lost or changed without
following a rotation procedure, encrypted connection details may need to be reset
in Metabase.

`MB_ANON_TRACKING_ENABLED=false` is the default in the example env file. Keep it
explicit so local and deployed behavior are easy to review.

## Application Database

Metabase stores users, saved questions, dashboards, collections, permissions,
and connection metadata in its application database. This stack uses Postgres
instead of Metabase's embedded H2 database so local and deployed runtimes have
the same persistence model.

The `postgres-data` named volume is persistent platform state. Back up this
volume before upgrading Metabase or treating dashboards as production assets.

Create a logical backup of the application database:

```bash
set -a
. "$METABASE_ENV_FILE"
set +a

mkdir -p backups

DATA_PLATFORM_ENV_FILE="$METABASE_ENV_FILE" \
  docker compose --env-file "$METABASE_ENV_FILE" -f docker-compose.yml exec -T postgres \
  pg_dump -U "$MB_DB_USER" "$MB_DB_DBNAME" > "backups/metabase-$(date +%Y%m%dT%H%M%S).sql"
```

Restore into a fresh or intentionally reset application database only:

```bash
set -a
. "$METABASE_ENV_FILE"
set +a

DATA_PLATFORM_ENV_FILE="$METABASE_ENV_FILE" \
  docker compose --env-file "$METABASE_ENV_FILE" -f docker-compose.yml exec -T postgres \
  psql -U "$MB_DB_USER" "$MB_DB_DBNAME" < backups/metabase-backup.sql
```

Local backup files under `metabase/backups/` are ignored by git. Store deployed
backups outside the repo and test restore before relying on them.

## Warehouse Connections

Create warehouse/database connections from the Metabase UI after the service is
running. For BigQuery, use a least-privilege Metabase service account for the
deployed analytics instance.

Minimum read-only permissions:

- `roles/bigquery.jobUser` on the project where Metabase runs query jobs.
- `roles/bigquery.dataViewer` on only the published mart dataset or datasets
  Metabase should read.
- `roles/bigquery.metadataViewer` on the same published mart dataset or
  datasets so Metabase can inspect schemas and table metadata.

Do not grant `BigQuery Admin`, `BigQuery Data Editor`, or project-wide
`BigQuery Data Viewer` unless there is a specific documented reason.

Run this from an authenticated terminal with permission to create service
accounts, grant IAM, and create service-account keys:

```bash
export ENVIRONMENT=prod
export PROJECT_ID=kevinesg-prod
export DATASET_IDS="mart_personal_finance"
export SERVICE_ACCOUNT_ID="metabase-$ENVIRONMENT"
export KEY_FILE="$HOME/secrets/data-platform/$ENVIRONMENT/metabase-bigquery-service-account.json"

mkdir -p "$(dirname "$KEY_FILE")"
chmod 700 "$(dirname "$KEY_FILE")"

if gcloud iam service-accounts describe \
  "$SERVICE_ACCOUNT_ID@$PROJECT_ID.iam.gserviceaccount.com" \
  --project="$PROJECT_ID" >/dev/null 2>&1; then
  echo "Service account already exists: $SERVICE_ACCOUNT_ID@$PROJECT_ID.iam.gserviceaccount.com"
else
  gcloud iam service-accounts create "$SERVICE_ACCOUNT_ID" \
    --project="$PROJECT_ID" \
    --display-name="Metabase BigQuery read-only ($PROJECT_ID)"
fi

export SERVICE_ACCOUNT_EMAIL="$SERVICE_ACCOUNT_ID@$PROJECT_ID.iam.gserviceaccount.com"

wait_for_service_account() {
  local service_account_email="$1"
  local attempt

  for attempt in {1..12}; do
    if gcloud iam service-accounts describe \
      "$service_account_email" \
      --project="$PROJECT_ID" >/dev/null 2>&1; then
      echo "Service account is visible: $service_account_email"
      return 0
    fi

    echo "Waiting for service account to propagate: $service_account_email"
    sleep 10
  done

  gcloud iam service-accounts describe \
    "$service_account_email" \
    --project="$PROJECT_ID"
}

add_project_iam_binding_with_retry() {
  local member="$1"
  local role="$2"
  local attempt

  for attempt in {1..6}; do
    if gcloud projects add-iam-policy-binding "$PROJECT_ID" \
      --member="$member" \
      --role="$role"; then
      return 0
    fi

    echo "Retrying IAM binding after propagation delay: $member $role"
    sleep 10
  done

  gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="$member" \
    --role="$role"
}

wait_for_service_account "$SERVICE_ACCOUNT_EMAIL"

add_project_iam_binding_with_retry \
  "serviceAccount:$SERVICE_ACCOUNT_EMAIL" \
  "roles/bigquery.jobUser"

for DATASET_ID in $DATASET_IDS; do
  bq query \
    --project_id="$PROJECT_ID" \
    --use_legacy_sql=false \
    "GRANT \`roles/bigquery.dataViewer\`, \`roles/bigquery.metadataViewer\`
     ON SCHEMA \`$PROJECT_ID\`.$DATASET_ID
     TO \"serviceAccount:$SERVICE_ACCOUNT_EMAIL\""
done

if test -f "$KEY_FILE"; then
  echo "Metabase BigQuery key already exists."
else
  gcloud iam service-accounts keys create "$KEY_FILE" \
    --project="$PROJECT_ID" \
    --iam-account="$SERVICE_ACCOUNT_EMAIL"
fi

chmod 600 "$KEY_FILE"
```

After the key exists:

1. Open Metabase and complete the first-run admin setup if needed.
2. Go to Admin settings > Databases > Add a database.
3. Choose Google BigQuery.
4. Enter the GCP project ID without any legacy `project_name:` prefix.
5. Upload or paste the service-account JSON key.
6. In dataset sync settings, choose only published mart datasets, such as
   `mart_personal_finance`.
7. Save the connection and let Metabase sync database metadata.
8. Confirm Metabase can browse analytics tables and run a small row-limited
   query.

## CI/CD Scope

Metabase does not need a dedicated CI/CD workflow yet. The current Metabase
change is runtime infrastructure only: Compose config, environment contract, and
operator documentation.

Add Metabase CI later when there is something deterministic to validate, such as
Compose config rendering in GitHub Actions or a governed dashboard/export
format. Add Metabase CD later only if the deployed Metabase runtime becomes
managed by GitHub Actions.
