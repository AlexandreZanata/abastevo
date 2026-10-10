#!/usr/bin/env python3
"""Bounded actual HTTPS pilot against the owned Abastevo staging origin.

Client runs off-VPS; resource sampling is read-only through the existing SSH
alias. Any missing telemetry, safety violation, unhealthy response or request
error stops scheduling immediately. No deployment/quota/rate-limit mutation.
Outputs are unique raw CSV, guard JSONL and a manifest/summary; no response bodies,
secrets, names or precise contributor coordinates are recorded.
"""
import argparse
import csv
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import queue
import signal
import subprocess
import threading
import time
import urllib.request
import urllib.error
import socket
import http.client
import ssl

from abastevo_guard import violations

ORIGIN = "https://teste.abastevo.com.br"
class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None
HTTP = urllib.request.build_opener(NoRedirect())
GUARD = Path(__file__).with_name("abastevo_guard.py")
CONNECTIONS = threading.local()


def snapshot():
    result = subprocess.run(["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=5",
                             "sorrimobi-vps", "python3 -"], input=GUARD.read_bytes(),
                            capture_output=True, timeout=12)
    if result.returncode:
        raise RuntimeError("Abastevo resource telemetry unavailable")
    return json.loads(result.stdout)


def fetch(path, required, deadline):
    started = time.monotonic()
    success = False
    try:
        if not getattr(CONNECTIONS, "http", None):
            CONNECTIONS.http = http.client.HTTPSConnection("teste.abastevo.com.br", timeout=deadline,
                                                          context=ssl.create_default_context())
        connection = CONNECTIONS.http
        connection.request("GET", path, headers={"User-Agent": "abastevo-rst-qualified/1", "Cache-Control": "no-cache"})
        with connection.getresponse() as response:
            body = response.read(2 * 1024 * 1024 + 1)
            if len(body) > 2 * 1024 * 1024:
                connection.close()
                CONNECTIONS.http = None
                return "oversize", (time.monotonic() - started) * 1000
            if response.status != 200:
                return str(response.status), (time.monotonic() - started) * 1000
            decoded = json.loads(body)
            good = response.status == 200 and isinstance(decoded, dict) and required in decoded
            if required == "items":
                good = good and isinstance(decoded["items"], list)
            success = good
            return "200" if good else "semantic", (time.monotonic() - started) * 1000
    except urllib.error.HTTPError as exc:
        return str(exc.code), (time.monotonic() - started) * 1000
    except (socket.timeout, TimeoutError):
        return "timeout", (time.monotonic() - started) * 1000
    except Exception:
        return "error", (time.monotonic() - started) * 1000
    finally:
        # No hidden retry: a broken transport remains a counted failure.
        # Fully read successful responses can reuse the worker's connection.
        if not success and getattr(CONNECTIONS, "http", None):
            CONNECTIONS.http.close()
            CONNECTIONS.http = None


def percentile(values, fraction):
    ordered = sorted(values)
    if not ordered:
        return None
    return ordered[min(len(ordered) - 1, max(0, __import__("math").ceil(fraction * len(ordered)) - 1))]


def run(args):
    if not (0 < args.rate <= 20 and 1 <= args.clients <= 8 and 1 <= args.seconds <= 300
            and 0 <= args.warmup <= 60 and 1 <= args.trials <= 5):
        raise ValueError("refused: rate<=20, clients<=8, steady<=300s, warmup<=60s, trials<=5")
    routes = json.loads(Path(args.routes).read_text())
    if not routes or len(routes) > 32:
        raise ValueError("require 1..32 frozen public routes")
    for route in routes:
        path = route["path"]
        if not path.startswith("/v1/stations") or "://" in path or "#" in path or route["required"] not in ("items", "station_id", "id"):
            raise ValueError("only owned public directory/profile routes allowed")
    dataset = None
    if getattr(args, "dataset_manifest", None):
        with open(args.dataset_manifest, "rb") as file:
            raw_manifest = file.read((1 << 20) + 1)
        if len(raw_manifest) > 1 << 20:
            raise ValueError("dataset manifest exceeds 1 MiB")
        dataset = {"sha256": hashlib.sha256(raw_manifest).hexdigest(), "manifest": json.loads(raw_manifest)}
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=False)
    stop = threading.Event()
    reasons = []
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    baseline = snapshot()
    reasons.extend(violations(baseline, baseline))
    if reasons:
        raise ValueError("initial safety gate refused: " + "; ".join(reasons))
    guard_file = open(out / "guard.jsonl", "w")
    guard_file.write(json.dumps({"wall": datetime.now(timezone.utc).isoformat(), **baseline}) + "\n")
    guard_file.flush()

    def guard():
        while not stop.wait(5):
            try:
                current = snapshot()
                bad = violations(current, baseline)
                # Readiness is checked over the actual domain, outside timed requests.
                with HTTP.open(urllib.request.Request(ORIGIN + "/health/ready", headers={"User-Agent":"abastevo-rst-qualified/1"}), timeout=3) as response:
                    if response.status != 200:
                        bad.append("HTTPS readiness failed")
                guard_file.write(json.dumps({"wall": datetime.now(timezone.utc).isoformat(), **current, "violations": bad}) + "\n")
                guard_file.flush()
                if bad:
                    reasons.extend(bad)
                    stop.set()
            except Exception as exc:
                reasons.append(str(exc))
                stop.set()
    watcher = threading.Thread(target=guard, daemon=True)
    watcher.start()
    summaries = []
    results = queue.Queue(maxsize=args.clients * 2)
    slots = threading.BoundedSemaphore(args.clients)
    try:
        with ThreadPoolExecutor(max_workers=args.clients) as executor:
            for trial in range(args.trials):
                for phase, seconds in (("warmup", args.warmup), ("steady", args.seconds)):
                    if stop.is_set() or not seconds:
                        continue
                    origin = time.monotonic()
                    end = origin + seconds
                    offered = done = drops = missed = errors = 0
                    latencies = {r["name"]: [] for r in routes}
                    path = out / ("trial-%d-%s.csv" % (trial + 1, phase))
                    with open(path, "w", newline="") as raw:
                        writer = csv.writer(raw)
                        writer.writerow(("route", "scheduled_ms", "started_ms", "completed_ms", "status"))

                        def task(route, scheduled):
                            begun = time.monotonic()
                            status, elapsed = fetch(route["path"], route["required"], 3)
                            results.put((route["name"], scheduled, begun, begun + elapsed / 1000, status))
                            slots.release()

                        def drain():
                            nonlocal done, errors
                            while True:
                                try:
                                    name, scheduled, begun, ended, status = results.get_nowait()
                                except queue.Empty:
                                    return
                                writer.writerow((name, (scheduled - origin) * 1000, (begun - origin) * 1000, (ended - origin) * 1000, status))
                                done += 1
                                if status == "200":
                                    latencies[name].append((ended - begun) * 1000)
                                else:
                                    errors += 1
                                    reasons.append("HTTP request failed: " + status)
                                    stop.set()
                        next_at = origin
                        index = 0
                        while time.monotonic() < end and not stop.is_set():
                            drain()
                            now = time.monotonic()
                            if now < next_at:
                                stop.wait(min(next_at - now, 0.02))
                                continue
                            if now - next_at > 1 / args.rate:
                                skipped = int((now - next_at) * args.rate)
                                for _ in range(skipped):
                                    writer.writerow(("scheduler", (next_at - origin) * 1000, "", "", "missed"))
                                    next_at += 1 / args.rate
                                missed += skipped
                                offered += skipped
                            offered += 1
                            route = routes[index % len(routes)]
                            index += 1
                            if slots.acquire(blocking=False):
                                executor.submit(task, route, next_at)
                            else:
                                drops += 1
                                writer.writerow((route["name"], (next_at - origin) * 1000, "", "", "drop"))
                                reasons.append("bounded client slots exhausted")
                                stop.set()
                            next_at += 1 / args.rate
                        # Scheduling stops, then drain bounded in-flight work without
                        # marking ordinary end-of-window completions cancelled.
                        drain_deadline = time.monotonic() + 5
                        while done + drops + missed < offered and time.monotonic() < drain_deadline:
                            drain()
                            time.sleep(0.01)
                        drain()
                    elapsed = time.monotonic() - origin
                    reconciled = done + drops + missed == offered
                    successful = sum(map(len, latencies.values()))
                    summary = {"trial": trial + 1, "phase": phase, "requested_seconds": seconds,
                               "observation_seconds": elapsed, "offered": offered, "completed": done,
                               "success": successful, "errors": errors, "drops": drops, "missed": missed,
                               "reconciled": reconciled, "successful_rps": successful / elapsed,
                               "routes": {name: {"n": len(v), "p50_ms": percentile(v, .50),
                                                  "p95_ms": percentile(v, .95),
                                                  "p99_ms": percentile(v, .99) if len(v) >= 10000 else None,
                                                  "p99_qualified": len(v) >= 10000}
                                          for name, v in latencies.items()}}
                    summary["p95_qualified"] = all(len(v) >= 100 for v in latencies.values())
                    worst = max((v["p95_ms"] or 0 for v in summary["routes"].values()), default=0)
                    summary["p95_budget_ms"] = args.p95_ms
                    summary["p95_budget_pass"] = worst <= args.p95_ms if successful else None
                    if phase == "steady" and worst > args.p95_ms:
                        reasons.append("HTTPS p95 budget breached")
                        stop.set()
                    summaries.append(summary)
                    if errors or drops or missed or not reconciled:
                        reasons.append("arrival/error/reconciliation budget breached")
                        stop.set()
                    print(json.dumps(summary), flush=True)
    finally:
        stop.set()
        watcher.join(timeout=15)
        guard_file.close()
    hashes = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in out.glob("*.csv")}
    hashes["guard.jsonl"] = hashlib.sha256((out / "guard.jsonl").read_bytes()).hexdigest()
    manifest = {"format": "abastevo-https-pilot-v2", "wall_end": datetime.now(timezone.utc).isoformat(),
                "origin": ORIGIN, "layer": "client HTTPS including TLS/edge/network", "cache": "client asks no-cache; edge behavior unchanged",
                "transport": "Python http.client, one reusable verified TLS/HTTP1.1 connection per worker; max8; no automatic retry",
                "guard_sha256": hashlib.sha256(GUARD.read_bytes()).hexdigest(),
                "driver_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                "parameters": vars(args), "routes": routes, "hashes": hashes, "summaries": summaries,
                "dataset": dataset,
                "stopped": bool(reasons), "reasons": reasons,
                "qualification": "bounded real HTTPS campaign; dataset coverage and 24/48h stability require independent evidence"}
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return int(bool(reasons))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--routes", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--dataset-manifest", help="frozen prepared manifest, at most 1 MiB")
    parser.add_argument("--rate", type=float, default=5)
    parser.add_argument("--p95-ms", type=float, default=500)
    parser.add_argument("--clients", type=int, default=8)
    parser.add_argument("--seconds", type=int, default=300)
    parser.add_argument("--warmup", type=int, default=60)
    parser.add_argument("--trials", type=int, default=5)
    args = parser.parse_args()
    raise SystemExit(run(args))
