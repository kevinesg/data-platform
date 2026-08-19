"""Run ClickHouse dbt builds with atomic results and native retry artifacts."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

SUCCESSFUL_DBT_STATUSES = frozenset({"no-op", "pass", "reused", "success", "warn"})
RETAINED_ARTIFACT_METADATA_KEY = "wremotely_retention"
RETAINED_ARTIFACT_CONTRACT_VERSION = 1
MAX_RETAINED_FAILED_TARGETS = 5


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Run ClickHouse dbt and retain the latest successful result while "
            "preserving failed targets for native dbt retry."
        )
    )
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--failed-target-root", type=Path)
    parser.add_argument("--retry-target-path", type=Path)
    parser.add_argument("dbt_args", nargs=argparse.REMAINDER)
    args = parser.parse_args()

    if args.retry_target_path is not None:
        if args.dbt_args:
            parser.error("dbt retry mode does not accept dbt arguments")
        return retry_and_retain_results(args.output, args.retry_target_path)

    dbt_args = args.dbt_args[1:] if args.dbt_args[:1] == ["--"] else args.dbt_args
    if not dbt_args or dbt_args[0] != "build":
        parser.error("the retained invocation must start with dbt build")
    return run_and_retain_results(
        args.output,
        dbt_args,
        failed_target_root=args.failed_target_root,
    )


def run_and_retain_results(
    output_path: Path,
    dbt_args: list[str],
    *,
    failed_target_root: Path | None = None,
) -> int:
    if dbt_args[:1] != ["build"]:
        raise ValueError("the retained invocation must start with dbt build")
    if "--target-path" in dbt_args:
        raise ValueError("the retained dbt invocation must not set --target-path")

    with tempfile.TemporaryDirectory(prefix="wremotely-clickhouse-dbt-") as target_dir:
        completed = subprocess.run(
            ["dbt", *dbt_args, "--target-path", target_dir],
            check=False,
        )
        if completed.returncode != 0:
            retain_failed_target(target_dir, failed_target_root)
            return completed.returncode

        try:
            payload = add_retention_provenance(
                validate_successful_build_results(Path(target_dir) / "run_results.json"),
                invocation="dbt build",
            )
        except RuntimeError:
            retain_failed_target(target_dir, failed_target_root)
            raise
        write_json_atomically(output_path, payload)
    return 0


def retry_and_retain_results(output_path: Path, target_path: Path) -> int:
    if not target_path.is_dir():
        raise ValueError(f"dbt retry target directory does not exist: {target_path}")
    completed = subprocess.run(
        ["dbt", "retry", "--target-path", str(target_path)],
        check=False,
    )
    if completed.returncode != 0:
        return completed.returncode
    payload = add_retention_provenance(
        validate_successful_build_results(target_path / "run_results.json"),
        invocation="dbt build",
    )
    write_json_atomically(output_path, payload)
    return 0


def retain_failed_target(
    target_dir: str | Path,
    failed_target_root: Path | None,
) -> Path | None:
    if failed_target_root is None:
        return None
    source = Path(target_dir)
    failed_target_root.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    destination = failed_target_root / f"{timestamp}-{uuid4().hex[:12]}"
    temporary_destination = failed_target_root / f".{destination.name}.tmp"
    shutil.copytree(source, temporary_destination)
    os.replace(temporary_destination, destination)
    prune_failed_targets(failed_target_root)
    print(f"dbt_failed_target={destination}", flush=True)
    return destination


def prune_failed_targets(failed_target_root: Path) -> None:
    retained_targets = sorted(
        (
            path
            for path in failed_target_root.iterdir()
            if path.is_dir() and not path.name.startswith(".")
        ),
        key=lambda path: path.stat().st_mtime_ns,
        reverse=True,
    )
    for stale_target in retained_targets[MAX_RETAINED_FAILED_TARGETS:]:
        shutil.rmtree(stale_target)


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
    if metadata_args is not None and (
        not isinstance(metadata_args, dict)
        or (
            "which" in metadata_args
            and metadata_args.get("which") not in {"build", "retry"}
        )
    ):
        raise RuntimeError("dbt run results contradicts the dbt build invocation")
    for result in results:
        if not isinstance(result, dict):
            raise RuntimeError("dbt run results contains an invalid result")
        if result.get("status") not in SUCCESSFUL_DBT_STATUSES:
            raise RuntimeError(
                f"dbt build contains a non-successful result status: {result.get('status')}"
            )
    return payload


def add_retention_provenance(
    payload: dict[str, object], *, invocation: str
) -> dict[str, object]:
    metadata = payload.get("metadata")
    if not isinstance(metadata, dict):
        raise RuntimeError("dbt run results has invalid metadata")
    retained_payload = dict(payload)
    retained_metadata = dict(metadata)
    retained_metadata[RETAINED_ARTIFACT_METADATA_KEY] = {
        "contract_version": RETAINED_ARTIFACT_CONTRACT_VERSION,
        "invocation": invocation,
    }
    retained_payload["metadata"] = retained_metadata
    return retained_payload


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
