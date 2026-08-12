"""Run one dbt build and atomically retain its successful run results."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import tempfile
from pathlib import Path

SUCCESSFUL_DBT_STATUSES = frozenset({"no-op", "pass", "reused", "success", "warn"})


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run dbt and retain only the latest successful build result."
    )
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("dbt_args", nargs=argparse.REMAINDER)
    args = parser.parse_args()

    dbt_args = args.dbt_args
    if dbt_args[:1] == ["--"]:
        dbt_args = dbt_args[1:]
    if not dbt_args or dbt_args[0] != "build":
        parser.error("the retained invocation must start with dbt build")

    return run_and_retain_results(args.output, dbt_args)


def run_and_retain_results(output_path: Path, dbt_args: list[str]) -> int:
    if "--target-path" in dbt_args:
        raise ValueError("the retained dbt invocation must not set --target-path")

    with tempfile.TemporaryDirectory(prefix="wremotely-dbt-build-") as target_dir:
        completed = subprocess.run(
            ["dbt", *dbt_args, "--target-path", target_dir],
            check=False,
        )
        if completed.returncode != 0:
            return completed.returncode

        run_results_path = Path(target_dir) / "run_results.json"
        payload = validate_successful_build_results(run_results_path)
        write_json_atomically(output_path, payload)
    return 0


def validate_successful_build_results(path: Path) -> dict[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"dbt build did not produce valid run results: {path}") from exc

    if not isinstance(payload, dict):
        raise RuntimeError("dbt run results must be a JSON object")
    metadata = payload.get("metadata")
    results = payload.get("results")
    if not isinstance(metadata, dict) or not isinstance(results, list) or not results:
        raise RuntimeError("dbt run results has an invalid or empty structure")
    metadata_args = metadata.get("args")
    if not isinstance(metadata_args, dict) or metadata_args.get("which") != "build":
        raise RuntimeError("dbt run results is not from dbt build")

    for result in results:
        if not isinstance(result, dict):
            raise RuntimeError("dbt run results contains an invalid result")
        status = result.get("status")
        if status not in SUCCESSFUL_DBT_STATUSES:
            raise RuntimeError(f"dbt build contains a non-successful result status: {status}")
    return payload


def write_json_atomically(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, sort_keys=True, separators=(",", ":"))
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary_path, 0o644)
        os.replace(temporary_path, path)
        directory_descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
    finally:
        temporary_path.unlink(missing_ok=True)


if __name__ == "__main__":
    raise SystemExit(main())
