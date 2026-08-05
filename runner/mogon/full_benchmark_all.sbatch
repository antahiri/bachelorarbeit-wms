#!/bin/bash
#SBATCH --job-name=wms_full
#SBATCH --account=ki-mawahpc
#SBATCH --partition=ki-smallcpu
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=12
#SBATCH --exclusive
#SBATCH --time=12:00:00
#SBATCH --output=%x_%j.log

echo "########## WMS-VOLLMESSUNG ##########"
echo "Knoten: $(hostname), CPUs (nproc): $(nproc), Start: $(date '+%F %T')"
source ~/.bashrc

########## 1. STREAMFLOW ##########
echo ""
echo "########## [1/4] STREAMFLOW: $(date '+%F %T') ##########"
conda activate streamflow-new
cd ~/wms_hpc_benchmark/run_benchmarks
python3 run_streamflow_mogon.py full
echo "StreamFlow-Exit: $?"
conda deactivate

########## 2. NEXTFLOW ##########
echo ""
echo "########## [2/4] NEXTFLOW: $(date '+%F %T') ##########"
conda activate nextflow_env
module load tools/Nextflow/25.10.4
cd ~/wms_hpc_benchmark/run_benchmarks
python3 run_nextflow_mogon.py full
echo "Nextflow-Exit: $?"
module unload tools/Nextflow/25.10.4
conda deactivate

########## 3. AIIDA ##########
echo ""
echo "########## [3/4] AIIDA: $(date '+%F %T') ##########"
conda activate aiida
export PYTHONPATH=~/wms_hpc_benchmark/aiida:$PYTHONPATH
export RABBITMQ_MNESIA_BASE=/fshpc/antahiri/rabbitmq/mnesia
export RABBITMQ_LOG_BASE=/fshpc/antahiri/rabbitmq/log
export RABBITMQ_NODENAME=rabbit@localhost
rabbitmq-server -detached
for i in $(seq 1 30); do rabbitmqctl status > /dev/null 2>&1 && break; sleep 2; done
if rabbitmqctl status > /dev/null 2>&1; then
    echo "RabbitMQ bereit."
    verdi daemon stop 2>/dev/null; sleep 2
    verdi daemon start 4; sleep 5
    cd ~/wms_hpc_benchmark/run_benchmarks
    python3 run_aiida_mogon.py
    echo "AiiDA-Exit: $?"
    verdi daemon stop
    rabbitmqctl stop
else
    echo "FEHLER: RabbitMQ nicht bereit, AiiDA uebersprungen."
fi
conda deactivate

########## 4. MERLIN ##########
echo ""
echo "########## [4/4] MERLIN: $(date '+%F %T') ##########"
module load tools/Apptainer
conda activate merlin2
export LC_ALL=C
REDIS_DIR=/fshpc/antahiri/merlin_server
apptainer exec ${REDIS_DIR}/redis_latest.sif redis-server \
    --port 6379 --requirepass "$(cat ${REDIS_DIR}/redis.pass)" \
    > ~/redis_full.log 2>&1 &
REDIS_PID=$!
sleep 8
if apptainer exec ${REDIS_DIR}/redis_latest.sif redis-cli -a "$(cat ${REDIS_DIR}/redis.pass)" ping 2>/dev/null | grep -q PONG; then
    echo "Redis bereit."
    cd ~/wms_hpc_benchmark/merlin/scatter_gather
    celery -A merlin worker --concurrency 4 --prefetch-multiplier 1 -O fair \
        -l INFO -n full_worker.%h -Q "[merlin]_merlin" > ~/celery_full.log 2>&1 &
    CELERY_PID=$!
    sleep 10
    echo "Celery-Worker gestartet."
    cd ~/wms_hpc_benchmark/run_benchmarks
    python3 run_merlin_mogon.py --force
    echo "Merlin-Exit: $?"
    kill $CELERY_PID 2>/dev/null; sleep 3
    apptainer exec ${REDIS_DIR}/redis_latest.sif redis-cli -a "$(cat ${REDIS_DIR}/redis.pass)" shutdown nosave 2>/dev/null
    kill $REDIS_PID 2>/dev/null
else
    echo "FEHLER: Redis nicht bereit, Merlin uebersprungen."
    kill $REDIS_PID 2>/dev/null
fi

echo ""
echo "########## ALLE VIER SYSTEME FERTIG: $(date '+%F %T') ##########"
