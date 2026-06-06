from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path
from urllib.parse import urlparse

from dotenv import load_dotenv

VALID_ENVIRONMENTS = {"dev", "qa", "prod"}
ENV_EXAMPLE_FILE = Path(__file__).resolve().with_name(".env.example")
PREFERRED_DEV_ENV_FILE = Path.home() / "dev/secrets/data-platform/.env"
DEVELOPER_ID_PATTERN = re.compile(r"^[a-z][a-z0-9]{2,7}$")
OPTIONAL_ENV_VARS = {"DEVELOPER_ID"}


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate scripts component configuration.")
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

    if args.env_file is not None:
        env_file = args.env_file.expanduser()
        if not env_file.is_file():
            print(f"error: env file does not exist: {env_file}", file=sys.stderr)
            return 2
        load_dotenv(env_file, override=False)

    required_env_vars = required_env_var_names(ENV_EXAMPLE_FILE)
    values = {name: os.getenv(name, "").strip() for name in required_env_vars}

    missing = [
        name for name, value in values.items() if not value and name not in OPTIONAL_ENV_VARS
    ]
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

    print("scripts config OK")
    print(f"environment={values['ENVIRONMENT'].lower()}")
    if values["DEVELOPER_ID"]:
        print(f"developer_id={values['DEVELOPER_ID']}")
    print(f"project_id={values['PROJECT_ID']}")
    print(f"raw_dataset={values['RAW_DATASET']}")
    return 0


def required_env_var_names(env_example_file: Path) -> list[str]:
    required_env_vars = []
    for line in env_example_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            required_env_vars.append(line.split("=", 1)[0].strip())
    return required_env_vars


def validate_values(values: dict[str, str]) -> str | None:
    environment = values["ENVIRONMENT"].lower()
    if environment not in VALID_ENVIRONMENTS:
        valid_values = ", ".join(sorted(VALID_ENVIRONMENTS))
        return f"ENVIRONMENT must be one of: {valid_values}"

    developer_id = values["DEVELOPER_ID"]
    if environment == "dev":
        if not DEVELOPER_ID_PATTERN.fullmatch(developer_id):
            return (
                "DEVELOPER_ID must start with a lowercase letter and contain 3-8 "
                "lowercase letters or digits in dev"
            )

        expected_dataset = f"raw_{developer_id}"
        if values["RAW_DATASET"] != expected_dataset:
            return f"RAW_DATASET must be {expected_dataset} for dev"

        expected_bucket = f"{values['PROJECT_ID']}-data-platform-landing-{developer_id}"
        if values["PERSONAL_FINANCE_GCS_BUCKET"] != expected_bucket:
            return f"PERSONAL_FINANCE_GCS_BUCKET must be {expected_bucket} for dev"

    sheet_url = urlparse(values["PERSONAL_FINANCE_GSHEET_URL"])
    if sheet_url.scheme not in {"http", "https"} or not sheet_url.netloc:
        return "PERSONAL_FINANCE_GSHEET_URL must be a valid URL"

    prefix = values["PERSONAL_FINANCE_GCS_PREFIX"].strip("/")
    if not prefix:
        return "PERSONAL_FINANCE_GCS_PREFIX must not be empty"

    for name in ("PERSONAL_FINANCE_CHUNK_SIZE", "PERSONAL_FINANCE_JSONL_RETENTION_DAYS"):
        if not is_positive_integer(values[name]):
            return f"{name} must be a positive integer"

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
