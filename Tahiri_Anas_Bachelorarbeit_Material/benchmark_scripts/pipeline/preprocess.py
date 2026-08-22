#!/usr/bin/env python3

import time
task_start_ns = time.time_ns()
import sys
from benchmark_timing import write_timing

input_file = sys.argv[1] if len(sys.argv) > 1 else "raw_input.txt"
output_file = sys.argv[2] if len(sys.argv) > 2 else "prepared_input.txt"


with open(input_file, "r") as f:
    values = [int(line.strip()) for line in f if line.strip()]

filtered = [value for value in values if value >= 10]

with open(output_file, "w") as f:
    for value in filtered:
        f.write(f"{value}\n")

task_end_ns = time.time_ns()

write_timing("preprocess", task_start_ns, task_end_ns)
