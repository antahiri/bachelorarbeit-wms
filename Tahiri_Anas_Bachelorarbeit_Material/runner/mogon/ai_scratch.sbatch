#!/bin/bash
#SBATCH --job-name=ai_scratch
#SBATCH --account=ki-mawahpc
#SBATCH --partition=ki-smallcpu
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=12
#SBATCH --time=03:00:00
#SBATCH --output=%x_%j.log

echo "Knoten: $(hostname), CPUs: $(nproc), Start: $(date '+%F %T')"
source ~/.bashrc
conda activate aiida
export PYTHONPATH=~/wms_hpc_benchmark/aiida:$PYTHONPATH

JOBDIR=/localscratch/${SLURM_JOB_ID}
PROFILE=scratch_${SLURM_JOB_ID}
echo "Local Scratch: $JOBDIR | Profil: $PROFILE"
mkdir -p $JOBDIR/aiida_storage $JOBDIR/aiida_work $JOBDIR/rabbitmq/mnesia $JOBDIR/rabbitmq/log

# RabbitMQ komplett auf dem Scratch (vor Profil-Setup, da --use-rabbitmq)
export RABBITMQ_MNESIA_BASE=$JOBDIR/rabbitmq/mnesia
export RABBITMQ_LOG_BASE=$JOBDIR/rabbitmq/log
export RABBITMQ_NODENAME=rabbit@localhost
rabbitmq-server -detached
for i in $(seq 1 30); do rabbitmqctl status > /dev/null 2>&1 && break; sleep 2; done
rabbitmqctl status > /dev/null 2>&1 && echo "RabbitMQ bereit." || { echo "FEHLER: RabbitMQ"; exit 1; }

# Frisches Profil: SQLite-DB + Repository KOMPLETT auf dem Scratch
verdi profile setup core.sqlite_dos --non-interactive \
    --profile-name $PROFILE \
    --no-set-as-default \
    --use-rabbitmq \
    --email antahiri@students.uni-mainz.de \
    --first-name Anas --last-name Tahiri --institution JGU \
    --filepath $JOBDIR/aiida_storage || { echo "FEHLER: Profil-Setup"; exit 1; }

# Computer/Code wie das Original (Labels identisch), Work-Dir auf dem Scratch
verdi -p $PROFILE computer setup --non-interactive \
    --label mogon-local --hostname localhost \
    --description "MOGON node, local scratch" \
    --transport core.local --scheduler core.direct \
    --work-dir $JOBDIR/aiida_work \
    --mpirun-command "mpirun -np {tot_num_mpiprocs}" \
    --mpiprocs-per-machine 1 || { echo "FEHLER: Computer-Setup"; exit 1; }
verdi -p $PROFILE computer configure core.local mogon-local --non-interactive --safe-interval 0
verdi -p $PROFILE code create core.code.installed --non-interactive \
    --label aiida_python312 --computer mogon-local \
    --filepath-executable $(which python3) \
    --default-calc-job-plugin core.calcjob || { echo "FEHLER: Code-Setup"; exit 1; }

verdi -p $PROFILE daemon start 4
sleep 5
verdi -p $PROFILE daemon status

# Skriptvariante: Ergebnis-Pfad results_scratch, Profil explizit laden
cd ~/wms_hpc_benchmark/run_benchmarks
sed "s|\"results\"|\"results_scratch\"|g; s|load_profile()|load_profile(\"$PROFILE\")|" \
    run_aiida_mogon.py > run_aiida_scratch.py
grep -n "load_profile\|results_scratch" run_aiida_scratch.py | head -4

# Erst Smoke, bei Erfolg volle Matrix
python3 run_aiida_scratch.py --smoke
SMOKE=$?
echo "Smoke-Exit: $SMOKE"
if [ $SMOKE -eq 0 ]; then
    python3 run_aiida_scratch.py
    echo "AiiDA-Scratch-Exit: $?"
fi

# Aufraeumen: Daemon, Broker, Wegwerf-Profil samt Daten (liegt eh auf Scratch)
verdi -p $PROFILE daemon stop
rabbitmqctl stop
verdi profile delete --force --delete-data $PROFILE
echo "Fertig: $(date '+%F %T')"
