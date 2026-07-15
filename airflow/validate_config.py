from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path

ENV_EXAMPLE_FILE = Path(__file__).resolve().with_name(".env.example")
PREFERRED_DEV_ENV_FILE = Path.home() / "dev/secrets/data-platform/.env"
FERNET_KEY_PATTERN = re.compile(r"^[A-Za-z0-9_-]{43}=$")
BIGQUERY_DATASET_ID_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,1023}$")
PUBSUB_TOPIC_ID_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9._~+%-]{2,254}$")


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate Airflow component configuration.")
    parser.add_argument(
        "--env-file",
        type=Path,
        default=default_env_file(),
        help=(
            "external dotenv file; discovers DATA_PLATFORM_ENV_FILE, "
            "DATA_PLATFORM_SECRETS_DIR, or the preferred dev path when present"
        ),
    )
    args = parser.parse_args()

    if args.env_file is None:
        print("error: DATA_PLATFORM_ENV_FILE is required", file=sys.stderr)
        return 2

    env_file = args.env_file.expanduser()
    if not env_file.is_file():
        print(f"error: env file does not exist: {env_file}", file=sys.stderr)
        return 2
    env_file_values = read_env_file(env_file)

    required_env_vars = required_env_var_names(ENV_EXAMPLE_FILE)
    values = {
        name: os.getenv(name, env_file_values.get(name, "")).strip()
        for name in required_env_vars
    }
    missing = [name for name, value in values.items() if not value]
    if missing:
        print(
            "error: missing required environment variables: " + ", ".join(missing),
            file=sys.stderr,
        )
        return 2

    error = validate_values(values)
    if error:
        print(f"error: {error}", file=sys.stderr)
        return 2

    print("airflow config OK")
    print(f"compose_project={values['AIRFLOW_COMPOSE_PROJECT']}")
    print(f"api_port={values['AIRFLOW_API_PORT']}")
    print(f"airflow_image={values['DATA_PLATFORM_AIRFLOW_IMAGE']}")
    print(f"postgres_db={values['POSTGRES_DB']}")
    return 0


def required_env_var_names(env_example_file: Path) -> list[str]:
    required_env_vars = []
    for line in env_example_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            required_env_vars.append(line.split("=", 1)[0].strip())
    return required_env_vars


def read_env_file(env_file: Path) -> dict[str, str]:
    values = {}
    for line in env_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        name, value = line.split("=", 1)
        value = value.strip().strip("'\"")
        values[name.strip()] = value
    return values


def validate_values(values: dict[str, str]) -> str | None:
    for name in ("AIRFLOW_UID", "AIRFLOW_API_PORT"):
        if not is_positive_integer(values[name]):
            return f"{name} must be a positive integer"

    if not is_non_negative_integer(values["DOCKER_GID"]):
        return "DOCKER_GID must be a non-negative integer"

    for name, value in values.items():
        if value.startswith("change-me") or "<" in value or ">" in value:
            return f"{name} must be replaced"

    for name in ("POSTGRES_PASSWORD", "AIRFLOW_SECRET_KEY", "AIRFLOW_JWT_SECRET"):
        if values[name].startswith("change-me"):
            return f"{name} must be replaced"

    if values["AIRFLOW_FERNET_KEY"].startswith("change-me"):
        return "AIRFLOW_FERNET_KEY must be replaced"
    if not FERNET_KEY_PATTERN.fullmatch(values["AIRFLOW_FERNET_KEY"]):
        return "AIRFLOW_FERNET_KEY must be a Fernet-formatted 32-byte urlsafe base64 key"

    for name in ("RAW_DATASET", "WREMOTELY_HANDOFF_DATASET", "DBT_DATASET"):
        if not BIGQUERY_DATASET_ID_PATTERN.fullmatch(values[name]):
            return f"{name} must be a valid BigQuery dataset ID"

    publication_topic = values["WREMOTELY_PUBLICATION_TOPIC"]
    if (
        not PUBSUB_TOPIC_ID_PATTERN.fullmatch(publication_topic)
        or publication_topic.lower().startswith("goog")
    ):
        return "WREMOTELY_PUBLICATION_TOPIC must be a valid Pub/Sub topic ID"

    for name in (
        "DBT_GOOGLE_APPLICATION_CREDENTIALS",
        "WREMOTELY_ETL_GOOGLE_APPLICATION_CREDENTIALS",
        "WREMOTELY_PUBLICATION_HOLD_POLICY",
    ):
        error = validate_existing_file_path(name, values[name])
        if error:
            return error

    error = validate_directory_path(
        "WREMOTELY_ETL_ARTIFACTS_DIR",
        values["WREMOTELY_ETL_ARTIFACTS_DIR"],
    )
    if error:
        return error

    for name in (
        "WREMOTELY_SOURCE_CRAWL_WORKER_COUNT",
        "WREMOTELY_EXTRACT_WORKER_COUNT",
        "WREMOTELY_PLATFORM_WORKER_COUNT",
        "WREMOTELY_RECHECK_WORKER_COUNT",
        "WREMOTELY_PAGE_MAX_BYTES",
        "WREMOTELY_DOMAIN_FAILURE_LIMIT",
        "WREMOTELY_CRAWL4AI_MIN_TEXT_CHARS",
        "WREMOTELY_STAGE_CHUNK_ROW_COUNT",
        "WREMOTELY_KNOWN_URL_LOOKBACK_DAYS",
    ):
        if not is_positive_integer(values[name]):
            return f"{name} must be a positive integer"

    if int(values["WREMOTELY_SOURCE_CRAWL_WORKER_COUNT"]) > 32:
        return "WREMOTELY_SOURCE_CRAWL_WORKER_COUNT must be no greater than 32"
    if int(values["WREMOTELY_PLATFORM_WORKER_COUNT"]) > 8:
        return "WREMOTELY_PLATFORM_WORKER_COUNT must be no greater than 8"
    if int(values["WREMOTELY_RECHECK_WORKER_COUNT"]) > 32:
        return "WREMOTELY_RECHECK_WORKER_COUNT must be no greater than 32"

    for name in ("WREMOTELY_DOMAIN_DELAY_SECONDS", "WREMOTELY_LOCAL_LLM_TIMEOUT_SECONDS"):
        if not is_non_negative_number(values[name]):
            return f"{name} must be a non-negative number"

    if values["WREMOTELY_CRAWL4AI_FALLBACK"] not in {"auto", "disabled"}:
        return "WREMOTELY_CRAWL4AI_FALLBACK must be auto or disabled"
    if values["WREMOTELY_LOCAL_LLM_RUNTIME"] not in {"disabled", "ollama"}:
        return "WREMOTELY_LOCAL_LLM_RUNTIME must be disabled or ollama"
    if values["WREMOTELY_LOCAL_LLM_RUNTIME"] == "ollama" and not values[
        "WREMOTELY_LOCAL_LLM_ENDPOINT"
    ].startswith(("http://", "https://")):
        return "WREMOTELY_LOCAL_LLM_ENDPOINT must be an HTTP URL"
    return None


def default_env_file() -> Path | None:
    value = os.getenv("DATA_PLATFORM_ENV_FILE", "").strip()
    if value:
        return Path(value).expanduser()
    secrets_dir = os.getenv("DATA_PLATFORM_SECRETS_DIR", "").strip()
    if secrets_dir:
        return Path(secrets_dir).expanduser() / ".env"
    if PREFERRED_DEV_ENV_FILE.is_file():
        return PREFERRED_DEV_ENV_FILE
    return None


def is_positive_integer(value: str) -> bool:
    try:
        return int(value) > 0
    except ValueError:
        return False


def is_non_negative_integer(value: str) -> bool:
    try:
        return int(value) >= 0
    except ValueError:
        return False


def is_non_negative_number(value: str) -> bool:
    try:
        return float(value) >= 0
    except ValueError:
        return False


def validate_existing_file_path(name: str, value: str) -> str | None:
    path = Path(value).expanduser()
    if not path.is_absolute():
        return f"{name} must be an absolute path"
    if not path.is_file():
        return f"{name} must point to an existing file"
    return None


def validate_directory_path(name: str, value: str) -> str | None:
    path = Path(value).expanduser()
    if not path.is_absolute():
        return f"{name} must be an absolute path"
    if not path.is_dir():
        return f"{name} must point to an existing directory"
    return None


if __name__ == "__main__":
    raise SystemExit(main())
