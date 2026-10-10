#!/usr/bin/env python3
"""Safety and accounting regressions for the bounded staging probe."""
import copy
from argparse import Namespace
import json
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

from abastevo_guard import violations
import guarded_http
import guard_job
import fetch_official_registry
import gzip
from unittest.mock import MagicMock


def healthy():
    return {"containers": {"pod/api": {"ready": True, "restarts": 0, "oom": False,
            "memory_mib": 100, "memory_limit_mib": 1024}},
            "available_mib": 24000, "host_cpus": 8, "load1": 1, "disk_free_mib": 200000}


class SafetyTests(unittest.TestCase):
    def test_https_transport_reuses_success_and_never_retries_timeout(self):
        guarded_http.CONNECTIONS.http = None
        conn = MagicMock()
        response = conn.getresponse.return_value.__enter__.return_value
        response.status = 200
        response.read.return_value = b'{"items":[]}'
        with patch.object(guarded_http.http.client, "HTTPSConnection", return_value=conn) as factory:
            self.assertEqual(guarded_http.fetch("/v1/stations", "items", 3)[0], "200")
            self.assertEqual(guarded_http.fetch("/v1/stations", "items", 3)[0], "200")
            factory.assert_called_once()
            self.assertEqual(conn.request.call_count, 2)
            conn.getresponse.side_effect = TimeoutError()
            self.assertEqual(guarded_http.fetch("/v1/stations", "items", 3)[0], "timeout")
            self.assertEqual(conn.request.call_count, 3, "no implicit retry")
            conn.close.assert_called_once()
            self.assertIsNone(guarded_http.CONNECTIONS.http)

    def test_official_freeze_rejects_oversize_gzip_and_ambiguous_identity(self):
        with self.assertRaises(ValueError):
            fetch_official_registry.decode_bounded(gzip.compress(b"X" * 1000), 100)
        row = {"id":3550308,"nome":"TEST CITY", "microrregiao":{"mesorregiao":{"UF":{"sigla":"SP"}}}}
        encoded = fetch_official_registry.aliases_from_ibge(json.dumps([row]).encode())
        self.assertEqual(json.loads(encoded)["entries"][0]["aliases"], ["TEST CITY"])
        with self.assertRaises(ValueError):
            fetch_official_registry.aliases_from_ibge(json.dumps([row,row]).encode())
        row["microrregiao"] = None
        with self.assertRaises(ValueError):
            fetch_official_registry.aliases_from_ibge(json.dumps([row]).encode())

    def test_each_pressure_and_identity_failure_stops(self):
        baseline = healthy()
        self.assertFalse(violations(baseline, baseline))
        for field, value in (("available_mib", 10000), ("load1", 6), ("disk_free_mib", 100)):
            row = copy.deepcopy(baseline)
            row[field] = value
            self.assertTrue(violations(row, baseline))
        for field, value in (("ready", False), ("oom", True), ("restarts", 1), ("memory_mib", 800)):
            row = copy.deepcopy(baseline)
            row["containers"]["pod/api"][field] = value
            self.assertTrue(violations(row, baseline))
        row = healthy()
        row["containers"]["different/api"] = row["containers"].pop("pod/api")
        self.assertTrue(violations(row, baseline))

    def test_http_failure_and_slot_drops_reconcile(self):
        for failure in (True, False):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                routes = root / "routes.json"
                routes.write_text(json.dumps([{"name": "city", "path": "/v1/stations?limit=20", "required": "items"}]))
                args = Namespace(rate=20, clients=1, seconds=1, warmup=0, trials=1,
                                 routes=str(routes), out=str(root / "out"), p95_ms=500)
                def fetch(*_):
                    time.sleep(.2)
                    return ("error" if failure else "200"), 200
                with patch.object(guarded_http, "snapshot", healthy), patch.object(guarded_http, "fetch", fetch):
                    self.assertEqual(guarded_http.run(args), 1)
                manifest = json.loads((root / "out/manifest.json").read_text())
                measured = manifest["summaries"][0]
                self.assertTrue(measured["reconciled"])
                self.assertEqual(measured["offered"], measured["completed"] + measured["drops"] + measured["missed"])
                self.assertTrue(manifest["stopped"])
                self.assertIsNone(measured["routes"]["city"]["p99_ms"])

    def test_job_guard_can_only_stop_its_exact_owned_job(self):
        with tempfile.TemporaryDirectory() as temp:
            pressure = healthy()
            pressure["available_mib"] = 100
            with patch.object(guard_job, "snapshot", return_value=pressure), patch.object(guard_job, "command", return_value='{"metadata":{"labels":{"app":"abastevo-rst-hardening"}},"status":{"succeeded":1}}') as command:
                self.assertEqual(guard_job.run("abastevo-rst-owned-test", str(Path(temp)/"guard.jsonl"), 5), 1)
                command.assert_any_call("delete", "job", "abastevo-rst-owned-test", "--wait=false")
            for name in ("abastevo-temp-api", "other-system", "abastevo-rst-x;anything", "../abastevo-rst-x"):
                with patch.object(guard_job, "command") as command, self.assertRaises(ValueError):
                    guard_job.run(name, str(Path(temp)/"unused"), 5)
                command.assert_not_called()

    def test_job_without_ownership_label_is_never_deleted(self):
        with patch.object(guard_job, "command", return_value='{"metadata":{"labels":{}},"status":{}}') as command:
            with self.assertRaises(ValueError):
                guard_job.stop_owned("abastevo-rst-other-effort")
            self.assertEqual(command.call_count, 1)
            self.assertEqual(command.call_args.args[:2], ("get", "job"))

    def test_unsafe_limits_refused_before_ssh(self):
        with patch.object(guarded_http, "snapshot") as probe:
            with self.assertRaises(ValueError):
                guarded_http.run(Namespace(rate=100, clients=8, seconds=300, warmup=60, trials=5))
            probe.assert_not_called()


if __name__ == "__main__":
    unittest.main()
