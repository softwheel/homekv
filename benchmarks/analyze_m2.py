#!/usr/bin/env python3
import json
import statistics
import sys
from pathlib import Path

if len(sys.argv) != 4:
    raise SystemExit("usage: analyze_m2.py run1.json run2.json run3.json")

runs = [json.loads(Path(p).read_text()) for p in sys.argv[1:]]
for run in runs:
    if run.get("mode") != "m2-comparative":
        raise SystemExit("unexpected benchmark mode")
    if run.get("authoritative_performance_result") is not False:
        raise SystemExit("M2 evidence must remain non-authoritative")

cells = {}
for run_index, run in enumerate(runs):
    seen = set()
    for result in run["results"]:
        key = (result["protocol"], result["workload"], result["pipeline_depth"])
        if key in seen:
            raise SystemExit(f"duplicate cell in run {run_index + 1}: {key}")
        seen.add(key)
        if result["key_size"] != 16 or result["value_size"] != 64:
            raise SystemExit(f"unexpected payload profile: {key}")
        if result["dataset_cardinality"] != 50000:
            raise SystemExit(f"unexpected dataset cardinality: {key}")
        if result["measured_operations"] < 10000:
            raise SystemExit(f"insufficient samples: {key}")
        if result["failures"] != 0:
            raise SystemExit(f"benchmark failures present: {key} failures={result['failures']}")
        cells.setdefault(key, []).append(result)

required_protocols = {"compact", "grpc"}
required_workloads = {"get", "set", "delete", "read80_write20"}
required_depths = {1, 32}
expected = {(p, w, d) for p in required_protocols for w in required_workloads for d in required_depths}
if set(cells) != expected:
    missing = sorted(expected - set(cells))
    extra = sorted(set(cells) - expected)
    raise SystemExit(f"cell matrix mismatch; missing={missing} extra={extra}")

for key, results in cells.items():
    if len(results) != 3:
        raise SystemExit(f"need exactly three repetitions for {key}")
    shas = {r["environment"]["homekv_git_sha"] for r in results}
    if len(shas) != 1:
        raise SystemExit(f"repetitions do not share one exact commit for {key}: {shas}")

print("HomeKV M2 compact-vs-gRPC comparative benchmark")
print("local single-node M1 semantics only; no replicated durability/linearizability claim")
print()
for workload in ["get", "set", "delete", "read80_write20"]:
    for depth in [1, 32]:
        c = cells[("compact", workload, depth)]
        g = cells[("grpc", workload, depth)]
        cp50 = statistics.median(r["latency_ns"]["p50"] for r in c)
        cp95 = statistics.median(r["latency_ns"]["p95"] for r in c)
        cp99 = statistics.median(r["latency_ns"]["p99"] for r in c)
        ctp = statistics.median(r["throughput_ops_sec"] for r in c)
        gp50 = statistics.median(r["latency_ns"]["p50"] for r in g)
        gp95 = statistics.median(r["latency_ns"]["p95"] for r in g)
        gp99 = statistics.median(r["latency_ns"]["p99"] for r in g)
        gtp = statistics.median(r["throughput_ops_sec"] for r in g)
        latency_ratio = gp50 / cp50 if cp50 else float("inf")
        throughput_ratio = ctp / gtp if gtp else float("inf")
        print(
            f"{workload:16s} depth={depth:2d} | "
            f"compact p50/p95/p99={cp50/1e3:.1f}/{cp95/1e3:.1f}/{cp99/1e3:.1f} us "
            f"tput={ctp:.1f} ops/s | "
            f"grpc p50/p95/p99={gp50/1e3:.1f}/{gp95/1e3:.1f}/{gp99/1e3:.1f} us "
            f"tput={gtp:.1f} ops/s | "
            f"grpc/compact p50={latency_ratio:.2f}x compact/grpc tput={throughput_ratio:.2f}x"
        )

print()
print("M2 comparative evidence gate: PASS (matrix, repetitions, >=10k/cell, zero failures)")
print("No fixed speedup is required by Accepted Spec 0004; ratios are observations, not acceptance thresholds.")
