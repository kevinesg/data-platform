from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from run_and_retain_results import run_and_retain_results


def build_payload(
    *, invocation: str | None = None, status: str = "success"
) -> dict[str, object]:
    metadata: dict[str, object] = {"dbt_version": "1.12.0"}
    if invocation is not None:
        metadata["args"] = {"which": invocation}
    return {
        "metadata": metadata,
        "elapsed_time": 12.5,
        "results": [{"unique_id": "model.wremotely.example", "status": status}],
    }


class RetainedBuildResultsTest(unittest.TestCase):
    def test_non_build_invocation_is_rejected_before_starting_dbt(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with patch("run_and_retain_results.subprocess.run") as run:
                with self.assertRaisesRegex(ValueError, "must start with dbt build"):
                    run_and_retain_results(Path(directory) / "run_results.json", ["generate"])
            run.assert_not_called()

    def test_successful_build_atomically_replaces_previous_result(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "retained" / "run_results.json"
            output.parent.mkdir()
            output.write_text('{"previous":true}\n', encoding="utf-8")

            def complete_build(command: list[str], *, check: bool) -> subprocess.CompletedProcess:
                self.assertFalse(check)
                target = Path(command[command.index("--target-path") + 1])
                (target / "run_results.json").write_text(
                    json.dumps(build_payload()), encoding="utf-8"
                )
                return subprocess.CompletedProcess(command, 0)

            with patch("run_and_retain_results.subprocess.run", side_effect=complete_build):
                result = run_and_retain_results(output, ["build", "--project-dir", "wremotely"])

            self.assertEqual(result, 0)
            retained_payload = json.loads(output.read_text(encoding="utf-8"))
            self.assertEqual(
                retained_payload["metadata"]["wremotely_retention"],
                {"contract_version": 1, "invocation": "dbt build"},
            )
            retained_payload["metadata"].pop("wremotely_retention")
            self.assertEqual(retained_payload, build_payload())

    def test_failed_build_preserves_previous_result(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "run_results.json"
            output.write_text('{"previous":true}\n', encoding="utf-8")
            with patch(
                "run_and_retain_results.subprocess.run",
                return_value=subprocess.CompletedProcess(["dbt", "build"], 1),
            ):
                result = run_and_retain_results(output, ["build"])

            self.assertEqual(result, 1)
            self.assertEqual(json.loads(output.read_text(encoding="utf-8")), {"previous": True})

    def test_non_build_or_unsuccessful_artifact_preserves_previous_result(self) -> None:
        for payload in (
            build_payload(invocation="generate"),
            build_payload(status="error"),
        ):
            with self.subTest(payload=payload), tempfile.TemporaryDirectory() as directory:
                output = Path(directory) / "run_results.json"
                output.write_text('{"previous":true}\n', encoding="utf-8")

                def complete_invalid_build(
                    command: list[str], *, check: bool, result_payload: object = payload
                ) -> subprocess.CompletedProcess:
                    target = Path(command[command.index("--target-path") + 1])
                    (target / "run_results.json").write_text(
                        json.dumps(result_payload), encoding="utf-8"
                    )
                    return subprocess.CompletedProcess(command, 0)

                with patch(
                    "run_and_retain_results.subprocess.run",
                    side_effect=complete_invalid_build,
                ):
                    with self.assertRaises(RuntimeError):
                        run_and_retain_results(output, ["build"])

                self.assertEqual(json.loads(output.read_text(encoding="utf-8")), {"previous": True})


if __name__ == "__main__":
    unittest.main()
