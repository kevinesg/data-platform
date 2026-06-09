from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path

ENV_EXAMPLE_FILE = Path(__file__).resolve().with_name(".env.example")
PREFERRED_DEV_ENV_FILE = Path.home() / "dev/secrets/data-platform/.env"
FERNET_KEY_PATTERN = re.compile(r"^[A-Za-z0-9_-]{43}=$")


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
    for name in ("AIRFLOW_UID", "DOCKER_GID", "AIRFLOW_API_PORT"):
        if not is_positive_integer(values[name]):
            return f"{name} must be a positive integer"

    for name in ("POSTGRES_PASSWORD", "AIRFLOW_SECRET_KEY", "AIRFLOW_JWT_SECRET"):
        if values[name].startswith("change-me"):
            return f"{name} must be replaced"

    if values["AIRFLOW_FERNET_KEY"].startswith("change-me"):
        return "AIRFLOW_FERNET_KEY must be replaced"
    if not FERNET_KEY_PATTERN.fullmatch(values["AIRFLOW_FERNET_KEY"]):
        return "AIRFLOW_FERNET_KEY must be a Fernet-formatted 32-byte urlsafe base64 key"

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


if __name__ == "__main__":
    raise SystemExit(main())
