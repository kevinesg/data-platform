# wremotely ClickHouse service

This component owns the persistent ClickHouse service used by the private
wremotely ETL loader and the isolated ClickHouse dbt project. It is separate
from the Airflow and dbt images: those components connect to ClickHouse, but
neither component owns its data directory or credentials.

The service uses the pinned `clickhouse/clickhouse-server:26.3.12.3` image,
stores data on the general-purpose warehouse disk, and binds the HTTP port to
loopback by default. Do not publish ClickHouse directly to the Internet. The
private ETL loader, Airflow DockerOperator, and dbt runtime must reach it over
the host's local network boundary.

## Prerequisites and secret placement

Install Docker Engine and the Compose plugin, and ensure the operator account
can run Docker. The host must already have the warehouse filesystem mounted.
For homeserver this is expected to be `/srv/data`, with ClickHouse data below
`/srv/data/warehouse/workmichi/`.

Keep environment files and password files outside Git with user-only
permissions. A production example layout is:

```text
$HOME/secrets/data-platform/prod/clickhouse.env
$HOME/secrets/data-platform/prod/clickhouse-password
```

Create a password file without printing it or placing it in a Compose file:

```bash
umask 077
install -d -m 0700 "$HOME/secrets/data-platform/prod"
read -rsp 'wremotely ClickHouse password: ' CLICKHOUSE_PASSWORD
printf '\n'
printf '%s' "$CLICKHOUSE_PASSWORD" > "$HOME/secrets/data-platform/prod/clickhouse-password"
unset CLICKHOUSE_PASSWORD
chmod 600 "$HOME/secrets/data-platform/prod/clickhouse-password"
```

Create `clickhouse.env` with values appropriate for the environment. Do not
copy a production file into dev or QA:

```dotenv
WREMOTELY_CLICKHOUSE_COMPOSE_PROJECT=wremotely-clickhouse-prod
WREMOTELY_CLICKHOUSE_IMAGE=clickhouse/clickhouse-server:26.3.12.3
WREMOTELY_CLICKHOUSE_DATABASE=wremotely_prod
WREMOTELY_CLICKHOUSE_USER=wremotely_prod
WREMOTELY_CLICKHOUSE_DATA_DIR=/srv/data/warehouse/workmichi/clickhouse/prod/data
WREMOTELY_CLICKHOUSE_LOG_DIR=/srv/data/warehouse/workmichi/clickhouse/prod/logs
WREMOTELY_CLICKHOUSE_PASSWORD_FILE=/home/kevinesg/secrets/data-platform/prod/clickhouse-password
WREMOTELY_CLICKHOUSE_BIND_ADDRESS=127.0.0.1
```

Use separate database, user, password, data directory, Compose project, and
write permissions for dev, QA, and prod. The ETL and dbt connection variables
(`WREMOTELY_CLICKHOUSE_DATABASE`, `WREMOTELY_CLICKHOUSE_USER`, and
`WREMOTELY_CLICKHOUSE_PASSWORD`) must refer to the same environment-specific
database and user.

## Start or verify the service

Run the following from this directory after loading the external environment
file. The `mkdir` step is intentionally explicit so Docker cannot create a
root-owned host path because a variable was misspelled.

```bash
set -a
. "$HOME/secrets/data-platform/prod/clickhouse.env"
set +a

test -r "$WREMOTELY_CLICKHOUSE_PASSWORD_FILE"
test "$WREMOTELY_CLICKHOUSE_IMAGE" = "clickhouse/clickhouse-server:26.3.12.3"

install -d -m 0750 \
  "$WREMOTELY_CLICKHOUSE_DATA_DIR" \
  "$WREMOTELY_CLICKHOUSE_LOG_DIR"

docker compose --env-file "$HOME/secrets/data-platform/prod/clickhouse.env" \
  -f docker-compose.yml config --quiet

docker compose --env-file "$HOME/secrets/data-platform/prod/clickhouse.env" \
  -f docker-compose.yml up -d

docker compose --env-file "$HOME/secrets/data-platform/prod/clickhouse.env" \
  -f docker-compose.yml ps

curl --fail --silent http://127.0.0.1:8123/ping
```

The expected ping response is `Ok.`. Validate authenticated access without
printing data:

```bash
docker compose --env-file "$HOME/secrets/data-platform/prod/clickhouse.env" \
  -f docker-compose.yml exec -T server \
  clickhouse-client \
    --user "$WREMOTELY_CLICKHOUSE_USER" \
    --password "$(<"$WREMOTELY_CLICKHOUSE_PASSWORD_FILE")" \
    --query 'SELECT version(), currentDatabase()'
```

The dbt project and ETL loader must pass their own relation/count checks before
this service is treated as an on-prem warehouse authority. Starting the
container alone does not load raw relations or run transformations.

## Stop, upgrade, and recovery

Stop the service without deleting its data:

```bash
docker compose --env-file "$HOME/secrets/data-platform/prod/clickhouse.env" \
  -f docker-compose.yml stop
```

Recreate it after an image or configuration change without removing volumes:

```bash
docker compose --env-file "$HOME/secrets/data-platform/prod/clickhouse.env" \
  -f docker-compose.yml up -d --force-recreate
```

Do not use `down -v` for this service. The bind-mounted warehouse data is the
analytical copy, while the immutable filesystem landing under
`/srv/data/warehouse/workmichi/storage/` remains the recovery source. Until a
separate backup/restore PR adds a verified ClickHouse backup procedure, recover
by restoring the warehouse filesystem and replaying `load-clickhouse-raw`.
Validate row counts and dbt tests after recovery before resuming publication.

Password rotation requires an environment-specific operator procedure: stop
the service, change the password through ClickHouse's authenticated user
management, update the external password file and all consumers, then restart
and repeat the authenticated validation above. Never rotate by committing a
secret or rebuilding the image.

## Explicit deferrals

This component does not yet grant public access, configure VPS publication
reads, schedule the on-prem ETL DAG, or replace the paused GCP wremotely DAG.
Those changes require reconciled production data, backup/restore evidence,
failure recovery, publication comparison, and a bounded rollback window.
