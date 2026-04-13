#!/usr/bin/env bash
# Extract WriteBlockOpAvgTime from JMX files and produce a markdown summary table.
# Usage: bash extract_write_block_avg.sh

set -euo pipefail

BASE="/Users/houzhizhen/git/hadoop-utils/bin/hdfs-lock-test/parallel-ssd"

# Directories in the required column order
DIRS=(
  "parallel-ssd-before-opt-time1-20260411-181717"
  "parallel-ssd-before-opt-time2-20260411-190411"
  "parallel-ssd-before-opt-time3-20260411-200442"
  "parallel-ssd-after-opt-time1-20260411-205159"
  "parallel-ssd-after-opt-time2-20260411-214249"
  "parallel-ssd-after-opt-time3-20260411-223053"
)

THREAD_COUNTS=(1000 2000 4000 8000 12000)
HOSTS=("rpm14usp6bo" "rpm23qygubp" "rpm723xv2lg")

# For each directory, discover sorted unique timestamps and map them 1:1 to thread counts.
# Then for each (dir, timestamp), average WriteBlockOpAvgTime across 3 hosts.

python3 - "$BASE" <<'PYEOF'
import sys, os, json, re
from collections import defaultdict

base = sys.argv[1]

dirs = [
    "parallel-ssd-before-opt-time1-20260411-181717",
    "parallel-ssd-before-opt-time2-20260411-190411",
    "parallel-ssd-before-opt-time3-20260411-200442",
    "parallel-ssd-after-opt-time1-20260411-205159",
    "parallel-ssd-after-opt-time2-20260411-214249",
    "parallel-ssd-after-opt-time3-20260411-223053",
]

thread_counts = [1000, 2000, 4000, 8000, 12000]
hosts = ["rpm14usp6bo", "rpm23qygubp", "rpm723xv2lg"]

# result[dir_index][thread_index] = averaged value
result = {}

for di, d in enumerate(dirs):
    dirpath = os.path.join(base, d)
    # List all dn-*.jmx files, extract timestamps
    files = [f for f in os.listdir(dirpath) if f.startswith("dn-") and f.endswith(".jmx")]
    # Extract unique timestamps
    timestamps = set()
    for f in files:
        # dn-{host}-{timestamp}.jmx
        parts = f.replace(".jmx", "").split("-")
        ts = parts[-1]  # last part is timestamp
        timestamps.add(ts)
    sorted_ts = sorted(timestamps)

    if len(sorted_ts) != len(thread_counts):
        print(f"WARNING: {d} has {len(sorted_ts)} timestamps, expected {len(thread_counts)}", file=sys.stderr)
        print(f"  timestamps: {sorted_ts}", file=sys.stderr)

    result[di] = {}
    for ti, ts in enumerate(sorted_ts):
        values = []
        for host in hosts:
            fpath = os.path.join(dirpath, f"dn-{host}-{ts}.jmx")
            if not os.path.exists(fpath):
                print(f"WARNING: missing {fpath}", file=sys.stderr)
                continue
            fsize = os.path.getsize(fpath)
            if fsize == 0:
                print(f"WARNING: empty file {fpath}", file=sys.stderr)
                continue
            try:
                with open(fpath) as fp:
                    data = json.load(fp)
                for bean in data.get("beans", []):
                    name = bean.get("name", "")
                    # Match DataNodeActivity bean (not per-volume)
                    if "DataNodeActivity" in name and "WriteBlockOpAvgTime" in bean:
                        values.append(float(bean["WriteBlockOpAvgTime"]))
                        break
            except Exception as e:
                print(f"WARNING: error reading {fpath}: {e}", file=sys.stderr)

        if values:
            avg = sum(values) / len(values)
        else:
            avg = float('nan')
        result[di][ti] = avg

# Print markdown table
header_cols = ["线程数", "优化前第1次", "优化前第2次", "优化前第3次", "优化后第1次", "优化后第2次", "优化后第3次"]
print("| " + " | ".join(header_cols) + " |")
print("| " + " | ".join(["---"] * len(header_cols)) + " |")

for ti, tc in enumerate(thread_counts):
    row = [str(tc)]
    for di in range(len(dirs)):
        val = result[di].get(ti, float('nan'))
        if val != val:  # NaN check
            row.append("N/A")
        else:
            row.append(f"{val:.2f}")
    print("| " + " | ".join(row) + " |")
PYEOF
