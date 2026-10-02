#!/usr/bin/env bash
set -euo pipefail

baseline="${1:-benchmarks/cold-warm-baseline.json}"
output="$(cargo bench -p lakeprism-index --bench cold_warm 2>&1)"
printf '%s\n' "$output"

python3 - "$baseline" "$output" <<'PY'
import json
import re
import sys

baseline_path, output = sys.argv[1:]
baseline = json.load(open(baseline_path, encoding="utf-8"))
match = re.search(r"lakeprism_benchmark cold_nanos=(\d+) warm_nanos=(\d+)", output)
if not match:
    raise SystemExit("benchmark did not emit machine-readable cold/warm timings")

cold, warm = map(int, match.groups())
expected_cold = baseline.get("cold_nanos")
expected_warm = baseline.get("warm_nanos")
if expected_cold is None or expected_warm is None:
    print("benchmark baseline is intentionally unmeasured; collected timings were not judged")
    raise SystemExit(0)

ratio = float(baseline.get("max_regression_ratio", 1.2))
for name, actual, expected in (("cold", cold, expected_cold), ("warm", warm, expected_warm)):
    if actual > expected * ratio:
        raise SystemExit(
            f"{name} benchmark regression: {actual}ns exceeds {expected}ns * {ratio}"
        )
print("cold/warm benchmark is within configured regression thresholds")
PY
