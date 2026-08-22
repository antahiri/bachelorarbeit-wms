#!/bin/bash
#SBATCH --no-requeue
#SBATCH --job-name="aiida-116"
#SBATCH --get-user-env
#SBATCH --output=_scheduler-stdout.txt
#SBATCH --error=_scheduler-stderr.txt
#SBATCH --partition=ki-smallcpu
#SBATCH --account=ki-mawahpc
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --time=00:10:00

'/home/antahiri/.conda/envs/aiida/bin/python3' 'preprocess.py' 'raw_input.txt' 'prepared_input.txt'  > 'aiida_stdout.txt' 2> 'aiida_stderr.txt'
