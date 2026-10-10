#!/usr/bin/env python3
"""Guard one explicitly named, bounded Abastevo test/import Job. Never stop a service."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import subprocess
import time

from abastevo_guard import violations
from guarded_http import snapshot


def command(*args):
    result = subprocess.run(["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=5", "sorrimobi-vps",
                             "kubectl", "-n", "abastevo-temp", *args], capture_output=True, text=True, timeout=12)
    if result.returncode:
        raise RuntimeError("owned Abastevo Job operation failed")
    return result.stdout


def validate(job, seconds):
    if not re.fullmatch(r"abastevo-rst-[a-z0-9-]{1,48}", job) or not 1 <= seconds <= 1200:
        raise ValueError("refused: only an explicitly owned Abastevo RST Job, at most 20 minutes")


def owned_status(job):
    resource = json.loads(command("get", "job", job, "-o", "json"))
    if resource.get("metadata", {}).get("labels", {}).get("app") != "abastevo-rst-hardening":
        raise ValueError("refused: Job lacks this campaign's ownership label")
    return resource.get("status", {})


def stop_owned(job):
    owned_status(job)  # Recheck ownership immediately before the exact deletion.
    command("delete", "job", job, "--wait=false")


def run(job, out, seconds):
    validate(job, seconds)
    owned_status(job)
    baseline = snapshot()
    stop_reasons = violations(baseline, baseline)
    deadline = time.monotonic() + seconds
    with open(out, "x") as output:
        while True:
            current = snapshot()
            stop_reasons += violations(current, baseline)
            status = owned_status(job)
            output.write(json.dumps({"wall": datetime.now(timezone.utc).isoformat(), **current,
                                     "job_status": status, "violations": stop_reasons}) + "\n")
            output.flush()
            if time.monotonic() > deadline:
                stop_reasons.append("bounded Job guard deadline")
            if stop_reasons:
                # Only this exact Job is disposable. No deployment/statefulset,
                # other namespace, image, PVC or database is deleted.
                stop_owned(job)
                print("stopped only owned Job:", job, stop_reasons, flush=True)
                return 1
            if status.get("succeeded"):
                print("owned Abastevo Job completed; telemetry:", out, flush=True)
                return 0
            if status.get("failed"):
                print("owned Abastevo Job failed; telemetry:", out, flush=True)
                return 1
            time.sleep(5)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--job", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--seconds", type=int, default=900)
    args = parser.parse_args()
    validate(args.job, args.seconds)  # Invalid arguments never authorize deletion.
    try:
        raise SystemExit(run(args.job, args.out, args.seconds))
    except Exception as exc:
        # Fail closed on missing telemetry while preserving application services.
        if re.fullmatch(r"abastevo-rst-[a-z0-9-]{1,48}", args.job):
            try:
                stop_owned(args.job)
            except Exception:
                pass  # Job activeDeadlineSeconds remains the final hard fence.
        raise SystemExit("owned Job guard unavailable: " + str(exc))
