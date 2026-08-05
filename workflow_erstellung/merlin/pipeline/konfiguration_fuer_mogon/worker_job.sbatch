#!/bin/bash
#SBATCH --account=ki-mawahpc
#SBATCH --job-name=merlin-worker-pipeline
#SBATCH --partition=ki-smallcpu
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=2G
#SBATCH --time=00:15:00
#SBATCH --output=worker_%j.out

module purge
module load lang/Anaconda3/2024.06-1
source ~/.bashrc
conda activate merlin2
cd ~/slurm_modus/merlin/pipeline

export LANG=C LC_ALL=C

echo "Worker-Knoten: $(hostname), verbindet zu Broker auf mogon-nhr01"

celery -A merlin worker \
  --concurrency 1 \
  --prefetch-multiplier 1 \
  -O fair \
  -l INFO \
  -n beobachtung_worker.%h \
  -Q "[merlin]_merlin" \
  > "worker_celery_${SLURM_JOB_ID}.log" 2>&1