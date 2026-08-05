#!/usr/bin/env python3

import time
task_start_ns = time.time_ns()
import sys
from benchmark_timing import write_timing

input_file = sys.argv[1] if len(sys.argv) > 1 else "result.txt"
output_file = sys.argv[2] if len(sys.argv) > 2 else "summary.txt"


with open(input_file, "r") as f:
    content = f.read()

with open(output_file, "w") as f:
    f.write("Summary of Pipeline computation\n")
    f.write("-------------------------------\n")
    f.write(content)

task_end_ns = time.time_ns()

write_timing("postprocess", task_start_ns, task_end_ns)
