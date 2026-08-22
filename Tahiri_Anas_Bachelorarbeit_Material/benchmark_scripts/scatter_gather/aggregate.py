#!/usr/bin/env python3

import time
task_start_ns = time.time_ns()
import sys

from benchmark_timing import write_timing

input_files = sys.argv[1:-1]
output_file = sys.argv[-1]


total_count = 0
total_sum = 0
total_matrix_checksum = 0
chunks = 0

for input_file in input_files:
    chunks += 1
    values = {}

    with open(input_file, "r") as f:
        for line in f:
            if "=" in line:
                key, value = line.strip().split("=", 1)
                values[key] = value

    total_count += int(values.get("count", 0))
    total_sum += int(values.get("sum", 0))
    total_matrix_checksum += int(values.get("matrix_checksum", 0))

total_mean = total_sum / total_count if total_count > 0 else 0

with open(output_file, "w") as f:
    f.write(f"chunks={chunks}\n")
    f.write(f"total_count={total_count}\n")
    f.write(f"total_sum={total_sum}\n")
    f.write(f"global_mean={total_mean:.2f}\n")
    f.write(f"total_matrix_checksum={total_matrix_checksum}\n")

task_end_ns = time.time_ns()

write_timing("aggregate", task_start_ns, task_end_ns)
