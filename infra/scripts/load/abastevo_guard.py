#!/usr/bin/env python3
"""Read-only, Abastevo-only resource snapshot. No cluster/host mutation."""
import json
import os
from pathlib import Path
import shutil
import subprocess

NAMESPACE = "abastevo-temp"
LIMITS_MIB = {"api": 1024, "worker": 512, "private-storage": 1024, "postgres": 1024}


def quantity_mib(value):
    for suffix, factor in (("Ki", 1 / 1024), ("Mi", 1), ("Gi", 1024)):
        if value.endswith(suffix):
            return float(value[:-len(suffix)]) * factor
    return float(value) / 1048576


def collect():
    def kube(*args):
        return subprocess.check_output(["kubectl", "-n", NAMESPACE, *args], text=True, timeout=8)
    all_pods = json.loads(kube("get", "pods", "-o", "json"))["items"]
    pods = [p for p in all_pods if p["metadata"]["name"].startswith(("abastevo-temp-api-", "abastevo-temp-db-"))]
    if len(pods) != 2:
        raise ValueError("expected one Abastevo API and one DB pod")
    statuses = {}
    for pod in pods:
        name = pod["metadata"]["name"]
        for status in pod["status"].get("containerStatuses", []):
            statuses[name + "/" + status["name"]] = {
                "ready": status["ready"], "restarts": status["restartCount"],
                "oom": status.get("lastState", {}).get("terminated", {}).get("reason") == "OOMKilled",
            }
    for line in kube("top", "pods", "--containers", "--no-headers").splitlines():
        name, container, cpu, memory = line.split()[:4]
        key = name + "/" + container
        if key in statuses:
            if container not in LIMITS_MIB:
                raise ValueError("unrecognized Abastevo resource budget")
            statuses[key].update(memory_mib=quantity_mib(memory),
                                 memory_limit_mib=LIMITS_MIB[container], cpu=cpu)
    if len(statuses) != 4 or any("memory_mib" not in c for c in statuses.values()):
        raise ValueError("missing Abastevo container telemetry")
    sql = """SET statement_timeout='2s'; SELECT json_build_object(
      'wal_bytes',(SELECT wal_bytes::text FROM pg_stat_wal),
      'wal_stats_reset',(SELECT stats_reset::text FROM pg_stat_wal),
      'checkpoints_completed',(SELECT num_done FROM pg_stat_checkpointer),
      'checkpoint_stats_reset',(SELECT stats_reset::text FROM pg_stat_checkpointer),
      'table_bytes',(SELECT COALESCE(sum(pg_table_size(c.oid)),0) FROM pg_class c
        JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relkind IN ('r','p')),
      'index_bytes',(SELECT COALESCE(sum(pg_indexes_size(c.oid)),0) FROM pg_class c
        JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relkind IN ('r','p')),
      'dead_tuples',(SELECT COALESCE(sum(n_dead_tup),0) FROM pg_stat_user_tables),
      'run_backlog',(SELECT count(*) FROM registry_source_runs WHERE state='running'),
      'waiting',(SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock'),
      'active',(SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND state='active'),
      'stations',(SELECT count(*) FROM directory_stations),
      'assertions',(SELECT count(*) FROM registry_assertions));"""
    db = json.loads(kube("exec", "abastevo-temp-db-0", "--", "sh", "-c",
        'exec psql -qAt -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "$1"', "guard", sql))
    memory = dict(line.split(":", 1) for line in Path("/proc/meminfo").read_text().splitlines())
    return {"containers": statuses, "db": db, "available_mib": int(memory["MemAvailable"].split()[0]) / 1024,
            "load1": os.getloadavg()[0], "host_cpus": os.cpu_count(),
            "disk_free_mib": shutil.disk_usage("/var/lib/rancher/k3s").free / 1048576}


def violations(snapshot, baseline):
    reasons = []
    if snapshot["available_mib"] < 12288:
        reasons.append("host available memory below 12 GiB reserve")
    if snapshot["load1"] > snapshot["host_cpus"] * 0.65:
        # Load includes runnable and uninterruptible work, not CPU utilization.
        reasons.append("host load1 above conservative 0.65 x CPU-count threshold")
    if snapshot["disk_free_mib"] < 51200:
        reasons.append("host free disk below 50 GiB reserve")
    if set(snapshot["containers"]) != set(baseline["containers"]):
        reasons.append("Abastevo pod/container identity changed")
    if "db" in snapshot:
        for key in ("wal_stats_reset", "checkpoint_stats_reset"):
            if snapshot["db"][key] != baseline["db"][key]:
                reasons.append("Abastevo DB statistics reset: " + key)
        if snapshot["db"]["run_backlog"] > baseline["db"]["run_backlog"] + 4:
            reasons.append("Abastevo import backlog grew by more than four runs")
        if snapshot["db"]["waiting"] > 4:
            reasons.append("Abastevo DB lock queue exceeded four sessions")
    for key, row in snapshot["containers"].items():
        if not row["ready"] or row["oom"]:
            reasons.append(key + " not ready/OOM")
        if key in baseline["containers"] and row["restarts"] != baseline["containers"][key]["restarts"]:
            reasons.append(key + " restarted")
        if row["memory_mib"] >= row["memory_limit_mib"] * 0.70:
            reasons.append(key + " reached 70% memory ceiling")
    return reasons


if __name__ == "__main__":
    try:
        print(json.dumps(collect(), sort_keys=True))
    except Exception as exc:
        print(json.dumps({"error": str(exc)}))
        raise SystemExit(1)
