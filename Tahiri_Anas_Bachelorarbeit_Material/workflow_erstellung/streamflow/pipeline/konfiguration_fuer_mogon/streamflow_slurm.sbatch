#!/bin/bash
#SBATCH --account=ki-mawahpc
#SBATCH --job-name=sf-task
#SBATCH --partition=ki-smallcpu
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=1024M
#SBATCH --time=00:10:00
#SBATCH --output=sf_task_%j.out

echo "SCRIPT_BEGIN=$(date '+%F %T.%N')"

echo "Slurm-Job: ${SLURM_JOB_ID}"
echo "Host: $(hostname)"
echo "Start: $(date '+%F %T.%N')"

{{streamflow_command}}

exit_code=$?

echo "End: $(date '+%F %T.%N')"

echo "Exit-Code: ${exit_code}"

exit ${exit_code}