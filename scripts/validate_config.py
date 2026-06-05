from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from urllib.parse import urlparse

from dotenv import load_dotenv

VALID_ENVIRONMENTS = {"dev", "qa", "prod"}
DEFAULT_ENV_FILE = Path(__file__).resolve().with_name(".env")
ENV_EXAMPLE_FILE = Path(__file__).resolve().with_name(".env.example")


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate scripts component configuration.")
    parser.add_argument(
        "--env-file",
        type=Path,
        default=DEFAULT_ENV_FILE,
        help="dotenv file for local development",
    )
    args = parser.parse_args()

    if args.env_file.exists():
        load_dotenv(args.env_file, override=False)
    elif args.env_file != DEFAULT_ENV_FILE:
        print(f"error: env file does not exist: {args.env_file}", file=sys.stderr)
        return 2

    required_env_vars = required_env_var_names(ENV_EXAMPLE_FILE)
    values = {name: os.getenv(name, "").strip() for name in required_env_vars}

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

    print("scripts config OK")
    print(f"environment={values['ENVIRONMENT'].lower()}")
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

    credentials_path = Path(values["GOOGLE_APPLICATION_CREDENTIALS"]).expanduser()
    if not credentials_path.exists():
        return f"GOOGLE_APPLICATION_CREDENTIALS does not exist: {credentials_path}"

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


def is_positive_integer(value: str) -> bool:
    try:
        return int(value) > 0
    except ValueError:
        return False


if __name__ == "__main__":
    raise SystemExit(main())
