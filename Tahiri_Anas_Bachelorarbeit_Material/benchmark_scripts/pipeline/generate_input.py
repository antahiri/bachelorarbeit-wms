#!/usr/bin/env python3

import time
task_start_ns = time.time_ns()
import sys
import random
from benchmark_timing import write_timing

SEED = 20260629
NUMBER_COUNT = 100

output_file = sys.argv[1] if len(sys.argv) > 1 else "raw_input.txt"


random_generator = random.Random(SEED)
numbers = [random_generator.randint(1, 30) for _ in range(NUMBER_COUNT)]

with open(output_file, "w") as f:
    for number in numbers:
        f.write(f"{number}\n")

task_end_ns = time.time_ns()

write_timing("generate_input", task_start_ns, task_end_ns)
