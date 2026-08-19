from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from run_and_retain_results import retry_and_retain_results, run_and_retain_results


def payload(status: str = "success") -> dict[str, object]:
    return {
        "metadata": {"dbt_version": "1.10.23"},
        "results": [{"unique_id": "model.wremotely.example", "status": status}],
    }


class RetainedClickHouseBuildResultsTest(unittest.TestCase):
    def test_failure_retains_target_and_preserves_previous_output(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / "run_results.json"
            output.write_text('{"previous":true}\n', encoding="utf-8")
            failed_root = root / "failed"

            def fail(command: list[str], *, check: bool) -> subprocess.CompletedProcess:
                target = Path(command[command.index("--target-path") + 1])
                (target / "run_results.json").write_text(
                    json.dumps(payload("error")), encoding="utf-8"
                )
                return subprocess.CompletedProcess(command, 1)

            with patch("run_and_retain_results.subprocess.run", side_effect=fail):
                self.assertEqual(
                    run_and_retain_results(
                        output,
                        ["build"],
                        failed_target_root=failed_root,
                    ),
                    1,
                )

            self.assertEqual(
                json.loads(output.read_text(encoding="utf-8")),
                {"previous": True},
            )
            targets = [path for path in failed_root.iterdir() if path.is_dir()]
            self.assertEqual(len(targets), 1)
            self.assertTrue((targets[0] / "run_results.json").is_file())

    def test_retry_atomically_publishes_successful_results(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            target = root / "failed-target"
            target.mkdir()
            (target / "run_results.json").write_text(
                json.dumps(payload("error")),
                encoding="utf-8",
            )
            output = root / "run_results.json"

            def retry(command: list[str], *, check: bool) -> subprocess.CompletedProcess:
                self.assertEqual(command[:3], ["dbt", "retry", "--target-path"])
                (target / "run_results.json").write_text(
                    json.dumps(payload()),
                    encoding="utf-8",
                )
                return subprocess.CompletedProcess(command, 0)

            with patch("run_and_retain_results.subprocess.run", side_effect=retry):
                self.assertEqual(retry_and_retain_results(output, target), 0)

            result = json.loads(output.read_text(encoding="utf-8"))
            self.assertEqual(result["metadata"]["wremotely_retention"]["invocation"], "dbt build")

    def test_target_path_is_owned_by_wrapper(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError, "must not set --target-path"):
                run_and_retain_results(
                    Path(directory) / "result.json",
                    ["build", "--target-path", "/tmp/x"],
                )
