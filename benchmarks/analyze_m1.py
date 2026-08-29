#!/usr/bin/env python3
import argparse, json, statistics
from pathlib import Path

M0_PUT_50K_P50_NS = 7_200_000
M0_PUT_50K_TPUT = 113.0

p=argparse.ArgumentParser()
p.add_argument('inputs', nargs='+')
a=p.parse_args()

runs=[]
for name in a.inputs:
    data=json.loads(Path(name).read_text())
    runs.append(data['results'])
if len(runs) < 3:
    raise SystemExit('need at least 3 complete M1 runs')

cells={}
for results in runs:
    seen=set()
    for r in results:
        key=(r['key_size'],r['value_size'],r['dataset_cardinality'],r['workload'])
        if key in seen: raise SystemExit(f'duplicate cell in run: {key}')
        seen.add(key)
        if r['layer']!='shard': raise SystemExit('unexpected layer')
        if r['measured_operations'] < 10000: raise SystemExit(f'insufficient samples: {key}')
        cells.setdefault(key,[]).append(r)

for key, rs in sorted(cells.items()):
    if len(rs) != len(runs): raise SystemExit(f'missing repetition: {key}')
    p50=[r['latency_ns']['p50'] for r in rs]
    p99=[r['latency_ns']['p99'] for r in rs]
    tp=[r['throughput_ops_sec'] for r in rs]
    print(f'{key}: p50 ns min/med/max={min(p50)}/{statistics.median(p50):.0f}/{max(p50)}; p99 med={statistics.median(p99):.0f}; throughput med={statistics.median(tp):.1f}')

k1=(16,64,1000,'set'); k50=(16,64,50000,'set'); kg50=(16,64,50000,'get')
p50_1=statistics.median(r['latency_ns']['p50'] for r in cells[k1])
p50_50=statistics.median(r['latency_ns']['p50'] for r in cells[k50])
tp50=statistics.median(r['throughput_ops_sec'] for r in cells[k50])
get_p99=statistics.median(r['latency_ns']['p99'] for r in cells[kg50])
ratio=p50_50/p50_1
lat_improve=M0_PUT_50K_P50_NS/p50_50
tp_improve=tp50/M0_PUT_50K_TPUT
print(f'50k/1k PUT p50 ratio: {ratio:.3f} (must <= 4.0)')
print(f'50k PUT p50 improvement vs frozen M0 ~7.2ms: {lat_improve:.2f}x (should >= 10x)')
print(f'50k PUT throughput improvement vs frozen M0 ~113 ops/s: {tp_improve:.2f}x (should >= 10x)')
print(f'50k GET p99: {get_p99/1e6:.3f} ms (should < 1 ms)')

mandatory = ratio <= 4.0 and lat_improve >= 10.0 and tp_improve >= 10.0
if not mandatory:
    raise SystemExit('M1 mandatory performance gate failed')
print('M1 mandatory performance gate: PASS')
