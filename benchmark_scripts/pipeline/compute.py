#!/usr/bin/env python3

import time
task_start_ns = time.time_ns()
import sys
from benchmark_timing import write_timing

MATRIX_SIZE = 96

input_file = sys.argv[1]
output_file = sys.argv[2]


with open(input_file, "r") as f:
    values = [int(line.strip()) for line in f if line.strip()]

count = len(values)
total = sum(values)
mean = total / count if count > 0 else 0


def matrix_checksum(seed):
    matrix_a = [
        [(seed + row * 3 + column * 5) % 17 for column in range(MATRIX_SIZE)]
        for row in range(MATRIX_SIZE)
    ]

    matrix_b = [
        [(seed + row * 7 + column * 11) % 19 for column in range(MATRIX_SIZE)]
        for row in range(MATRIX_SIZE)
    ]

    checksum = 0

    for row in range(MATRIX_SIZE):
        for column in range(MATRIX_SIZE):
            cell_value = 0

            for index in range(MATRIX_SIZE):
                cell_value += matrix_a[row][index] * matrix_b[index][column]

            checksum += cell_value

    return checksum


matrix_result = 0

for value in values:
    matrix_result += matrix_checksum(value)

with open(output_file, "w") as f:
    f.write(f"count={count}\n")
    f.write(f"sum={total}\n")
    f.write(f"mean={mean:.2f}\n")
    f.write(f"matrix_checksum={matrix_result}\n")

task_end_ns = time.time_ns()

write_timing("compute_1", task_start_ns, task_end_ns)
