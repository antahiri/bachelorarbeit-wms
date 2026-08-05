#!/bin/bash

cp /fshpc/antahiri/slurm_modus/merlin/scatter_gather/studies/merlin_scatter_gather_20260715-203943/split/chunk_1.txt .
python3 /fshpc/antahiri/slurm_modus/merlin/scatter_gather/scripts/compute.py chunk_1.txt result_1.txt

