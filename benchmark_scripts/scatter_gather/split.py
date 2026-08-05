#!/usr/bin/env python3

import time
task_start_ns = time.time_ns()
import sys

from benchmark_timing import write_timing

input_file = sys.argv[1] if len(sys.argv) > 1 else "prepared_input.txt"
num_chunks = int(sys.argv[2]) if len(sys.argv) > 2 else 4


with open(input_file, "r") as f:
    values = [line.strip() for line in f if line.strip()]

chunks = [[] for _ in range(num_chunks)]

for index, value in enumerate(values):
    chunks[index % num_chunks].append(value)

for index, chunk in enumerate(chunks, start=1):
    with open(f"chunk_{index}.txt", "w") as f:
        for value in chunk:
            f.write(f"{value}\n")

task_end_ns = time.time_ns()

write_timing("split", task_start_ns, task_end_ns)
