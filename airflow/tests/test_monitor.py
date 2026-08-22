"""Regression tests for the Airflow-3 monitor API boundary."""

from __future__ import annotations

import json
import sys
import unittest
from io import BytesIO
from pathlib import Path
from unittest.mock import patch


sys.path.insert(0, str(Path(__file__).parents[1] / "dags"))

import _monitor  # noqa: E402


class _Response:
    def __init__(self, payload: object) -> None:
        self._body = BytesIO(json.dumps(payload).encode())

    def __enter__(self) -> _Response:
        return self

    def __exit__(self, *_args: object) -> None:
        self._body.close()

    def read(self, *args: object, **kwargs: object) -> bytes:
        return self._body.read(*args, **kwargs)


class MonitorApiTests(unittest.TestCase):
    def test_lists_successful_runs_through_authenticated_api(self) -> None:
        response = _Response({"dag_runs": [{"dag_run_id": "run-1", "state": "success"}]})

        with (
            patch.object(_monitor, "init_auth_manager") as init_auth_manager,
            patch.object(_monitor, "get_auth_manager") as get_auth_manager,
            patch.object(_monitor, "urlopen", return_value=response) as urlopen,
        ):
            get_auth_manager.return_value.generate_jwt.return_value = "test-token"
            runs = _monitor._list_successful_runs("etl__wremotely")

        self.assertEqual(runs, [{"dag_run_id": "run-1", "state": "success"}])
        init_auth_manager.assert_called_once_with()
        get_auth_manager.return_value.generate_jwt.assert_called_once()
        request = urlopen.call_args.args[0]
        self.assertIn("/dags/etl__wremotely/dagRuns?", request.full_url)
        self.assertIn("state=success", request.full_url)
        self.assertIn("limit=100", request.full_url)
        self.assertIn("order_by=-end_date", request.full_url)
        self.assertEqual(request.get_header("Authorization"), "Bearer test-token")

    def test_rejects_invalid_api_run_list(self) -> None:
        response = _Response({"dag_runs": {"run-1": "success"}})

        with (
            patch.object(_monitor, "init_auth_manager"),
            patch.object(_monitor, "get_auth_manager") as get_auth_manager,
            patch.object(_monitor, "urlopen", return_value=response),
        ):
            get_auth_manager.return_value.generate_jwt.return_value = "test-token"
            with self.assertRaisesRegex(RuntimeError, "invalid run list"):
                _monitor._list_successful_runs("etl__wremotely")

    def test_latest_successful_run_uses_latest_available_timestamp(self) -> None:
        with patch.object(
            _monitor,
            "_list_successful_runs",
            return_value=[
                {
                    "dag_run_id": "older",
                    "end_date": "2026-08-22T00:00:00+00:00",
                    "start_date": "2026-08-21T23:00:00+00:00",
                },
                {
                    "dag_run_id": "newer",
                    "end_date": "2026-08-22T01:00:00+00:00",
                    "start_date": "2026-08-22T00:30:00+00:00",
                },
            ],
        ):
            latest = _monitor._latest_successful_run("etl__wremotely")

        self.assertEqual(latest["run_id"], "newer")
        self.assertEqual(latest["end_date"], "2026-08-22T01:00:00+00:00")

    def test_latest_successful_run_fails_when_api_returns_no_runs(self) -> None:
        with patch.object(_monitor, "_list_successful_runs", return_value=[]):
            with self.assertRaisesRegex(RuntimeError, "has no successful runs"):
                _monitor._latest_successful_run("etl__wremotely")


if __name__ == "__main__":
    unittest.main()
