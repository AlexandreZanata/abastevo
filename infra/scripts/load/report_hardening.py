#!/usr/bin/env python3
"""Validate raw owned campaign artifacts and export measured, labelled plots.

Requires the existing workspace ReportLab tool (4.4.9); no product dependency.
Never pools failed/short diagnostic trials into qualified capacity results.
"""
import argparse
import csv
import hashlib
import json
from pathlib import Path
from reportlab.graphics.charts.barcharts import VerticalBarChart
from reportlab.graphics.charts.lineplots import LinePlot
from reportlab.graphics.shapes import Drawing, String, Group, Line
from reportlab.graphics import renderSVG
from reportlab.lib import colors


def percentile(values, fraction):
    import math
    values = sorted(values)
    return values[max(0, math.ceil(len(values) * fraction) - 1)] if values else None


def verify_campaign(directory):
    manifest = json.loads((directory / "manifest.json").read_text())
    for name, expected in manifest["hashes"].items():
        if Path(name).name != name:
            raise ValueError("unsafe artifact name")
        if hashlib.sha256((directory / name).read_bytes()).hexdigest() != expected:
            raise ValueError("raw artifact checksum mismatch: " + name)
    qualified = []
    seen = set()
    for summary in manifest["summaries"]:
        raw_path = directory / ("trial-%d-%s.csv" % (summary["trial"], summary["phase"]))
        if raw_path.name not in manifest["hashes"] or raw_path.name in seen:
            raise ValueError("unhashed or repeated trial artifact")
        seen.add(raw_path.name)
        with raw_path.open() as raw_file:
            rows = list(csv.DictReader(raw_file))
        completed = [row for row in rows if row["status"] not in ("drop", "missed")]
        successes = [row for row in completed if row["status"] == "200"]
        if len(rows) != summary["offered"] or len(completed) != summary["completed"] or len(successes) != summary["success"]:
            raise ValueError("raw/summary request accounting differs")
        if (len(completed)-len(successes) != summary["errors"]
            or sum(r["status"] == "drop" for r in rows) != summary["drops"]
            or sum(r["status"] == "missed" for r in rows) != summary["missed"]):
            raise ValueError("raw error/drop accounting differs")
        if set(r["route"] for r in rows) - summary["routes"].keys():
            raise ValueError("raw route missing from summary")
        route_qualified = bool(summary["routes"])
        budget = summary.get("p95_budget_ms", 500)
        for name, stats in summary["routes"].items():
            timings = [float(r["completed_ms"]) - float(r["started_ms"]) for r in successes if r["route"] == name]
            p95 = percentile(timings, .95)
            if len(timings) != stats["n"] or (p95 is not None and abs(p95 - stats["p95_ms"]) > .001):
                raise ValueError("raw route distribution differs")
            route_qualified = route_qualified and len(timings) >= 100 and p95 <= budget
        if (summary["phase"] == "steady"
            and summary["requested_seconds"] >= 300 and summary["observation_seconds"] >= 300
            and route_qualified and summary["p95_qualified"] and summary["p95_budget_pass"] and summary["reconciled"]
            and not (summary["errors"] or summary["drops"] or summary["missed"])):
            qualified.append(summary)
    return {"directory": directory.name, "format": manifest["format"], "transport": manifest["transport"],
            "offered_rps": manifest["parameters"]["rate"], "stopped": manifest["stopped"],
            "reasons": manifest["reasons"], "completed_valid_trials": qualified,
            "campaign_qualified": not manifest["stopped"] and len(qualified) >= 5,
            "summary": manifest["summaries"], "manifest_sha256": hashlib.sha256((directory / "manifest.json").read_bytes()).hexdigest()}


def frame(title, subtitle, ylabel):
    drawing = Drawing(720, 390)
    drawing.add(String(55, 366, title, fontName="Helvetica-Bold", fontSize=15))
    drawing.add(String(55, 347, subtitle, fontName="Helvetica", fontSize=10))
    label = Group(String(0, 0, ylabel, textAnchor="middle", fontName="Helvetica", fontSize=10))
    label.transform = (0, 1, -1, 0, 16, 195)
    drawing.add(label)
    return drawing


def export_plots(root, report, out):
    prep = report["evidence"]["rust_prepare"]
    drawing = frame("Rust national preparation on VPS", "45,617 accepted / 121 quarantine; 250m CPU, 512 MiB cap; warm-up excluded; n=5", "Wall time (seconds)")
    chart = VerticalBarChart()
    chart.x, chart.y, chart.width, chart.height = 65, 65, 600, 260
    chart.data = [[r["wall_seconds"] for r in prep["trials"]]]
    chart.categoryAxis.categoryNames = [str(r["trial"]) for r in prep["trials"]]
    chart.valueAxis.valueMin, chart.valueAxis.valueMax, chart.valueAxis.valueStep = 0, 6, 1
    chart.bars[0].fillColor = colors.HexColor("#286d84")
    chart.barLabelFormat = "%.2f"
    chart.barLabels.nudge = 7
    chart.barLabels.fontName = "Helvetica"
    drawing.add(chart)
    drawing.add(String(330, 35, "Measured trial", fontSize=10))
    drawing.add(String(55, 14, "Source: rst-portable-prepare.log, 2026-10-10; full parse + sorted file emission + fsync", fontSize=9))
    renderSVG.drawToFile(drawing, str(out / "rust-preparation.svg"))

    rows = [json.loads(line) for line in (root / "rst-load-official-guard.jsonl").read_text().splitlines()]
    from datetime import datetime
    start = datetime.fromisoformat(rows[0]["wall"])
    points = [((datetime.fromisoformat(r["wall"])-start).total_seconds()/60, r["available_mib"]/1024) for r in rows]
    drawing = frame("Host RAM reserve during the Abastevo import", "Only Abastevo mutated; aggregate host telemetry; 70 samples; no guard violation", "Available host RAM (GiB)")
    chart = LinePlot()
    chart.x, chart.y, chart.width, chart.height = 65, 65, 600, 260
    chart.data = [points, [(points[0][0], 12), (points[-1][0],12)]]
    chart.lines[0].strokeColor = colors.HexColor("#286d84")
    chart.lines[1].strokeColor = colors.HexColor("#666666")
    chart.lines[1].strokeDashArray = [4,3]
    chart.yValueAxis.valueMin, chart.yValueAxis.valueMax, chart.yValueAxis.valueStep = 0, 32, 8
    drawing.add(chart)
    drawing.add(String(290, 35, "Elapsed import time (minutes)", fontSize=10))
    drawing.add(String(55, 14, "Source: owned guard JSONL, 2026-10-10; solid: available RAM; dashed: 12 GiB stop reserve", fontSize=9))
    renderSVG.drawToFile(drawing, str(out / "host-reserve.svg"))

    for campaign in report["campaigns"]:
        trials = campaign["completed_valid_trials"]
        if not trials:
            continue
        if len(trials) == 1:
            trial = trials[0]
            drawing = frame("Observed HTTPS p95 in the completed 5-minute trial", "5 req/s; one completed trial; campaign later stopped by host reserve; no replicated capacity acceptance", "Request latency p95 (ms)")
            chart = VerticalBarChart()
            chart.x, chart.y, chart.width, chart.height = 65, 65, 600, 260
            names = ["city_dense","city_sorriso","city_sparse","city_empty","text_national","detail","profile"]
            chart.data = [[trial["routes"][name]["p95_ms"] for name in names]]
            chart.categoryAxis.categoryNames = ["Dense","Sorriso","Sparse","Empty","Text","Detail","Profile"]
            chart.valueAxis.valueMin, chart.valueAxis.valueMax, chart.valueAxis.valueStep = 0, 600, 100
            chart.bars[0].fillColor = colors.HexColor("#286d84")
            chart.barLabelFormat = "%.0f"
            chart.barLabels.nudge = 7
            drawing.add(chart)
            drawing.add(Line(65,65+260*500/600,665,65+260*500/600,strokeColor=colors.grey,strokeDashArray=[4,3]))
            drawing.add(String(300, 35, "Frozen public route",fontSize=10))
            drawing.add(String(55, 14, "Source: raw HTTPS CSV, 2026-10-10; n=166-501/route; dashed: provisional500ms; no errors/drops",fontSize=9))
            renderSVG.drawToFile(drawing,str(out/(campaign["directory"]+"-p95.svg")))
            continue
        drawing = frame("Actual HTTPS p95 by qualified trial", "Offered %.0f req/s; HTTP1.1/TLS reuse; 300s observation/trial; p99 unavailable" % campaign["offered_rps"], "Request latency p95 (ms)")
        chart = LinePlot()
        chart.x, chart.y, chart.width, chart.height = 65, 65, 600, 260
        names = ["city_dense", "text_national", "profile"]
        chart.data = [[(s["trial"],s["routes"][name]["p95_ms"]) for s in trials] for name in names]
        chart.data += [[(trials[0]["trial"],500),(trials[-1]["trial"],500)]]
        for i,color in enumerate(("#286d84", "#a34a24", "#555555", "#999999")):
            chart.lines[i].strokeColor = colors.HexColor(color)
        chart.lines[3].strokeDashArray = [4,3]
        chart.yValueAxis.valueMin, chart.yValueAxis.valueMax, chart.yValueAxis.valueStep = 0, 600, 100
        drawing.add(chart)
        for x, name, color in ((65,"city_dense","#286d84"),(260,"text_national","#a34a24"),(470,"profile","#555555")):
            drawing.add(String(x, 328, name, fillColor=colors.HexColor(color),fontSize=10))
        drawing.add(String(320, 35, "Qualified trial number", fontSize=10))
        drawing.add(String(55, 14, "Source: raw HTTPS CSV, 2026-10-10; dense / national text / profile; dashed: provisional 500ms", fontSize=9))
        renderSVG.drawToFile(drawing, str(out / (campaign["directory"]+"-p95.svg")))


def run(root, out):
    out.mkdir(parents=True, exist_ok=False)
    report = {"format":"abastevo-reproducible-hardening-report-v1", "evidence":json.loads((root/"evidence.json").read_text()),
              "campaigns":[verify_campaign(p.parent) for p in sorted(root.glob("https-*/manifest.json"))],
              "qualification":"Selected bounded evidence only; RST20/21 remain partial until long mixed/maintenance trials and omitted matrix exist."}
    (out / "summary.json").write_text(json.dumps(report,indent=2)+"\n")
    export_plots(root, report, out)
    hashes={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in out.iterdir()}
    (out / "hashes.json").write_text(json.dumps(hashes,indent=2)+"\n")
    print("Validated campaigns:",len(report["campaigns"]),"valid completed 300s windows:",sum(len(c["completed_valid_trials"]) for c in report["campaigns"]),"accepted replicated campaigns:",sum(c["campaign_qualified"] for c in report["campaigns"]))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root",type=Path,required=True)
    parser.add_argument("--out",type=Path,required=True)
    args=parser.parse_args()
    run(args.root,args.out)
