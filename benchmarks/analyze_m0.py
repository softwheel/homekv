#!/usr/bin/env python3
"""Summarize immutable HomeKV M0 benchmark bundles for Spec 0002 verification.

The analyzer intentionally reports observations only. It does not infer causes and it does
not turn M0 into an authoritative distributed-performance result.
"""

from __future__ import annotations

import argparse
import json
import math
from collections import defaultdict
from pathlib import Path
from statistics import median
from typing import Any, Iterable

PRIMARY_KEY_SIZE = 16
PRIMARY_VALUE_SIZE = 64
PRIMARY_DATASETS = (1_000, 10_000, 50_000)
PRIMARY_CONCURRENCY = (1, 8, 32)
WORKLOADS = ("get", "set", "delete", "read80_write20")
PAYLOAD_CASES = ((16, 64), (16, 256), (32, 1024))
PAYLOAD_DATASET = 10_000
PAYLOAD_CONCURRENCY = 8
EXPECTED_REPETITIONS = 3
LOW_P99_TAIL_OBSERVATIONS = 5


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analyze HomeKV M0 benchmark result bundles")
    parser.add_argument("--input", type=Path, required=True, help="directory containing M0 JSON bundles")
    parser.add_argument("--output", type=Path, required=True, help="markdown report path")
    parser.add_argument("--baseline-sha", required=True, help="exact immutable pre-M1 HomeKV commit")
    return parser.parse_args()


def load_bundle(path: Path, baseline_sha: str) -> dict[str, Any]:
    data = json.loads(path.read_text())
    if data.get("authoritative_performance_result") is not False:
        raise ValueError(f"{path}: M0 bundle must be explicitly non-authoritative")
    results = data.get("results")
    if not isinstance(results, list) or not results:
        raise ValueError(f"{path}: missing non-empty results array")
    for result in results:
        environment = result.get("environment", {})
        if environment.get("homekv_git_sha") != baseline_sha:
            raise ValueError(
                f"{path}: result commit {environment.get('homekv_git_sha')!r} != {baseline_sha}"
            )
    return data


def load_repetitions(root: Path, prefix: str, baseline_sha: str) -> list[tuple[Path, dict[str, Any]]]:
    files = sorted(root.glob(f"{prefix}-run-*.json"))
    if len(files) != EXPECTED_REPETITIONS:
        raise ValueError(
            f"expected {EXPECTED_REPETITIONS} {prefix} repetitions, found {len(files)}"
        )
    return [(path, load_bundle(path, baseline_sha)) for path in files]


def cell_key(result: dict[str, Any]) -> tuple[Any, ...]:
    return (
        result["layer"],
        result["workload"],
        result["key_size"],
        result["value_size"],
        result["dataset_cardinality"],
        result["concurrency"],
    )


def aggregate(repetitions: Iterable[tuple[Path, dict[str, Any]]]) -> dict[tuple[Any, ...], dict[str, Any]]:
    grouped: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for _, bundle in repetitions:
        seen: set[tuple[Any, ...]] = set()
        for result in bundle["results"]:
            key = cell_key(result)
            if key in seen:
                raise ValueError(f"duplicate result cell within repetition: {key}")
            seen.add(key)
            grouped[key].append(result)

    aggregated: dict[tuple[Any, ...], dict[str, Any]] = {}
    for key, rows in grouped.items():
        if len(rows) != EXPECTED_REPETITIONS:
            raise ValueError(f"cell {key} has {len(rows)} repetitions, expected {EXPECTED_REPETITIONS}")
        throughputs = [float(row["throughput_ops_sec"]) for row in rows]
        measured = [int(row["measured_operations"]) for row in rows]
        attempted = [int(row["attempted_operations"]) for row in rows]
        failures = [int(row.get("failures", 0)) for row in rows]
        p50 = [int(row["latency_ns"]["p50"]) for row in rows]
        p95 = [int(row["latency_ns"]["p95"]) for row in rows]
        p99 = [int(row["latency_ns"]["p99"]) for row in rows]
        med_throughput = float(median(throughputs))
        spread = 0.0 if med_throughput == 0 else (max(throughputs) - min(throughputs)) / med_throughput
        min_measured = min(measured)
        min_tail = max(1, math.ceil(min_measured * 0.01))
        aggregated[key] = {
            "throughput": med_throughput,
            "throughput_min": min(throughputs),
            "throughput_max": max(throughputs),
            "throughput_spread": spread,
            "p50": int(median(p50)),
            "p95": int(median(p95)),
            "p99": int(median(p99)),
            "min_measured": min_measured,
            "min_attempted": min(attempted),
            "max_failures": max(failures),
            "min_p99_tail_observations": min_tail,
        }
    return aggregated


def require_cells(agg: dict[tuple[Any, ...], dict[str, Any]]) -> None:
    missing: list[tuple[Any, ...]] = []
    for dataset in PRIMARY_DATASETS:
        for workload in WORKLOADS:
            key = ("storage", workload, PRIMARY_KEY_SIZE, PRIMARY_VALUE_SIZE, dataset, 1)
            if key not in agg:
                missing.append(key)
    for dataset in PRIMARY_DATASETS:
        for concurrency in PRIMARY_CONCURRENCY:
            for workload in WORKLOADS:
                key = (
                    "server",
                    workload,
                    PRIMARY_KEY_SIZE,
                    PRIMARY_VALUE_SIZE,
                    dataset,
                    concurrency,
                )
                if key not in agg:
                    missing.append(key)
    for key_size, value_size in PAYLOAD_CASES:
        for workload in WORKLOADS:
            key = (
                "server",
                workload,
                key_size,
                value_size,
                PAYLOAD_DATASET,
                PAYLOAD_CONCURRENCY,
            )
            if key not in agg:
                missing.append(key)
    if missing:
        formatted = "\n".join(f"- {key}" for key in missing)
        raise ValueError(f"mandatory M0 cells missing:\n{formatted}")


def ns_to_us(value: int) -> float:
    return value / 1_000.0


def fmt_ops(value: float) -> str:
    return f"{value:,.0f}"


def fmt_us(value: int) -> str:
    return f"{ns_to_us(value):,.1f}"


def add_table(lines: list[str], headers: list[str], rows: Iterable[list[str]]) -> None:
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("| " + " | ".join("---" for _ in headers) + " |")
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
    lines.append("")


def ratio(numerator: float, denominator: float) -> float:
    return float("nan") if denominator == 0 else numerator / denominator


def build_report(
    root: Path,
    baseline_sha: str,
    storage: dict[tuple[Any, ...], dict[str, Any]],
    server: dict[tuple[Any, ...], dict[str, Any]],
) -> str:
    agg = {**storage, **server}
    require_cells(agg)

    lines = [
        "# HomeKV M0 Baseline Analysis",
        "",
        f"- Frozen pre-M1 commit: `{baseline_sha}`",
        f"- Repetitions: {EXPECTED_REPETITIONS} storage + {EXPECTED_REPETITIONS} server",
        "- Semantics: single-node prototype baseline; **not** distributed/strong-consistency performance",
        "- Aggregation: median of repeated runs; throughput spread is `(max-min)/median`",
        f"- p99 support flag: fewer than {LOW_P99_TAIL_OBSERVATIONS} expected observations in the top 1% of the smallest repetition is flagged as low-support",
        "",
        "## Primary storage baseline",
        "",
    ]

    storage_rows: list[list[str]] = []
    for dataset in PRIMARY_DATASETS:
        for workload in WORKLOADS:
            cell = storage[("storage", workload, 16, 64, dataset, 1)]
            storage_rows.append(
                [
                    workload,
                    f"{dataset:,}",
                    fmt_ops(cell["throughput"]),
                    fmt_us(cell["p50"]),
                    fmt_us(cell["p95"]),
                    fmt_us(cell["p99"]),
                    str(cell["min_measured"]),
                    str(cell["min_p99_tail_observations"]),
                    f"{cell['throughput_spread'] * 100:.1f}%",
                ]
            )
    add_table(
        lines,
        ["workload", "keys", "median ops/s", "p50 us", "p95 us", "p99 us", "min samples", "~top-1% samples", "run spread"],
        storage_rows,
    )

    lines.extend(["## Primary server baseline", ""])
    server_rows: list[list[str]] = []
    for dataset in PRIMARY_DATASETS:
        for concurrency in PRIMARY_CONCURRENCY:
            for workload in WORKLOADS:
                cell = server[("server", workload, 16, 64, dataset, concurrency)]
                server_rows.append(
                    [
                        workload,
                        f"{dataset:,}",
                        str(concurrency),
                        fmt_ops(cell["throughput"]),
                        fmt_us(cell["p50"]),
                        fmt_us(cell["p95"]),
                        fmt_us(cell["p99"]),
                        str(cell["min_measured"]),
                        str(cell["min_p99_tail_observations"]),
                        str(cell["max_failures"]),
                        f"{cell['throughput_spread'] * 100:.1f}%",
                    ]
                )
    add_table(
        lines,
        ["workload", "keys", "conc", "median ops/s", "p50 us", "p95 us", "p99 us", "min samples", "~top-1% samples", "max failures", "run spread"],
        server_rows,
    )

    lines.extend(["## Dataset-size write-cost observations", ""])
    write_rows: list[list[str]] = []
    for workload in ("set", "delete", "read80_write20"):
        base = storage[("storage", workload, 16, 64, 1_000, 1)]
        for dataset in PRIMARY_DATASETS:
            cell = storage[("storage", workload, 16, 64, dataset, 1)]
            write_rows.append(
                [
                    workload,
                    f"{dataset:,}",
                    fmt_ops(cell["throughput"]),
                    fmt_us(cell["p50"]),
                    fmt_us(cell["p99"]),
                    f"{ratio(cell['throughput'], base['throughput']):.3f}x",
                    f"{ratio(cell['p50'], base['p50']):.3f}x",
                ]
            )
    add_table(
        lines,
        ["workload", "keys", "median ops/s", "p50 us", "p99 us", "throughput vs 1k", "p50 vs 1k"],
        write_rows,
    )

    lines.extend(["## Payload sensitivity at 10k keys / concurrency 8", ""])
    payload_rows: list[list[str]] = []
    for workload in WORKLOADS:
        base = server[("server", workload, 16, 64, PAYLOAD_DATASET, PAYLOAD_CONCURRENCY)]
        for key_size, value_size in PAYLOAD_CASES:
            cell = server[("server", workload, key_size, value_size, PAYLOAD_DATASET, PAYLOAD_CONCURRENCY)]
            payload_rows.append(
                [
                    workload,
                    f"{key_size}B/{value_size}B",
                    fmt_ops(cell["throughput"]),
                    fmt_us(cell["p50"]),
                    fmt_us(cell["p99"]),
                    f"{ratio(cell['throughput'], base['throughput']):.3f}x",
                    f"{ratio(cell['p50'], base['p50']):.3f}x",
                ]
            )
    add_table(
        lines,
        ["workload", "key/value", "median ops/s", "p50 us", "p99 us", "throughput vs 16B/64B", "p50 vs 16B/64B"],
        payload_rows,
    )

    low_support = [
        (key, cell)
        for key, cell in sorted(agg.items())
        if cell["min_p99_tail_observations"] < LOW_P99_TAIL_OBSERVATIONS
    ]
    lines.extend(["## p99 sample-sufficiency flags", ""])
    if low_support:
        lines.append(
            f"The following cells have fewer than {LOW_P99_TAIL_OBSERVATIONS} expected observations in the top 1% in at least one repetition. Their p99 is retained but should be treated as low-support rather than a stable tail estimate."
        )
        lines.append("")
        for key, cell in low_support:
            layer, workload, key_size, value_size, dataset, concurrency = key
            lines.append(
                f"- `{layer}/{workload}` {key_size}B/{value_size}B, keys={dataset:,}, conc={concurrency}: min samples={cell['min_measured']}, ~top-1% samples={cell['min_p99_tail_observations']}"
            )
    else:
        lines.append(
            f"No measured cell fell below the mechanical low-support threshold of {LOW_P99_TAIL_OBSERVATIONS} expected top-1% observations."
        )
    lines.append("")

    lines.extend(["## Mechanical contract checks", ""])
    failed_cells = [(key, cell) for key, cell in agg.items() if cell["max_failures"] != 0]
    lines.append(f"- Mandatory primary/payload cells present: **yes**")
    lines.append(f"- Exact baseline SHA present in every result: **yes**")
    lines.append(f"- Repeated runs retained: **yes ({EXPECTED_REPETITIONS} per performance layer)**")
    lines.append(f"- Non-authoritative M0 labeling retained: **yes**")
    lines.append(f"- Cells with measured operation failures: **{len(failed_cells)}**")
    lines.append("")
    lines.append("This report contains measured observations only. Causal explanations belong in the Spec 0002 verification handoff as explicitly labeled hypotheses.")
    lines.append("")
    lines.append(f"Source directory: `{root}`")
    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    storage_reps = load_repetitions(args.input, "storage", args.baseline_sha)
    server_reps = load_repetitions(args.input, "server", args.baseline_sha)
    storage = aggregate(storage_reps)
    server = aggregate(server_reps)
    report = build_report(args.input, args.baseline_sha, storage, server)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(report)
    print(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
