#!/bin/bash
#SBATCH --job-name=ai_psql_excl
#SBATCH --account=ki-mawahpc
#SBATCH --partition=ki-smallcpu
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=12
#SBATCH --exclusive
#SBATCH --time=01:40:00
#SBATCH --output=%x_%j.log

echo "Knoten: $(hostname), Start: $(date '+%F %T')"
source ~/.bashrc
conda activate aiida
export LANG=C LC_ALL=C
export PYTHONPATH=~/wms_hpc_benchmark/aiida:$PYTHONPATH

BASE=/fshpc/antahiri/wms_hpc_benchmark/psql_test_${SLURM_JOB_ID}
PROFILE=psql_${SLURM_JOB_ID}
mkdir -p $BASE/repo $BASE/aiida_work $BASE/rabbitmq/mnesia $BASE/rabbitmq/log
echo "Basis (auf /fshpc!): $BASE | Profil: $PROFILE"

export PGDATA=$BASE/pgdata
initdb -D $PGDATA -U aiida -A trust --locale=C -E UTF8 > $BASE/initdb.log 2>&1 || { echo "FEHLER initdb"; tail -5 $BASE/initdb.log; exit 1; }
pg_ctl -D $PGDATA -l $BASE/pg.log -o "-p 5433 -h 127.0.0.1 -k /tmp" start || { echo "FEHLER pg start"; tail -5 $BASE/pg.log; exit 1; }
for i in $(seq 1 15); do pg_isready -h 127.0.0.1 -p 5433 >/dev/null 2>&1 && break; sleep 2; done
pg_isready -h 127.0.0.1 -p 5433 || { echo "FEHLER pg nicht bereit"; tail -5 $BASE/pg.log; exit 1; }
createdb -h 127.0.0.1 -p 5433 -U aiida aiida_psql || { echo "FEHLER createdb"; exit 1; }
echo "PostgreSQL laeuft, PGDATA auf /fshpc."

export RABBITMQ_MNESIA_BASE=$BASE/rabbitmq/mnesia
export RABBITMQ_LOG_BASE=$BASE/rabbitmq/log
export RABBITMQ_NODENAME=rabbit@localhost
rabbitmq-server -detached
for i in $(seq 1 30); do rabbitmqctl status >/dev/null 2>&1 && break; sleep 2; done
rabbitmqctl status >/dev/null 2>&1 && echo "RabbitMQ bereit." || { echo "FEHLER RabbitMQ"; exit 1; }

verdi profile setup core.psql_dos --non-interactive \
    --profile-name $PROFILE --no-set-as-default --use-rabbitmq \
    --email antahiri@students.uni-mainz.de \
    --first-name Anas --last-name Tahiri --institution JGU \
    --database-hostname 127.0.0.1 --database-port 5433 \
    --database-username aiida --database-password aiida \
    --database-name aiida_psql \
    --repository-uri file://$BASE/repo || { echo "FEHLER Profil"; exit 1; }

verdi -p $PROFILE computer setup --non-interactive \
    --label mogon-local --hostname localhost \
    --description "MOGON node, psql auf fshpc" \
    --transport core.local --scheduler core.direct \
    --work-dir $BASE/aiida_work \
    --mpirun-command "mpirun -np {tot_num_mpiprocs}" \
    --mpiprocs-per-machine 1 || { echo "FEHLER Computer"; exit 1; }
verdi -p $PROFILE computer configure core.local mogon-local --non-interactive --safe-interval 0
verdi -p $PROFILE code create core.code.installed --non-interactive \
    --label aiida_python312 --computer mogon-local \
    --filepath-executable $(which python3) \
    --default-calc-job-plugin core.calcjob || { echo "FEHLER Code"; exit 1; }

verdi -p $PROFILE daemon start 4
sleep 5
verdi -p $PROFILE daemon status

cd ~/wms_hpc_benchmark/run_benchmarks
sed "s|\"results\"|\"results_psql_exclusive\"|g; s|load_profile()|load_profile(\"$PROFILE\")|" \
    run_aiida_mogon.py > run_aiida_psql.py
grep -n "results_psql_exclusive\|load_profile(\"psql\|^CHUNKS" run_aiida_psql.py

python3 run_aiida_psql.py --smoke
SMOKE=$?
echo "Smoke-Exit: $SMOKE"
if [ $SMOKE -eq 0 ]; then
    python3 run_aiida_psql.py
    echo "psql-Lauf-Exit: $?"
fi

echo "== Spread-Ernte (coordination) =="
column -s, -t ~/wms_hpc_benchmark/results_psql_exclusive/aiida/aiida_coordination_results.csv 2>/dev/null | head -30
echo "== Central =="
column -s, -t ~/wms_hpc_benchmark/results_psql_exclusive/aiida/aiida_central_results.csv 2>/dev/null | head -30

verdi -p $PROFILE daemon stop
rabbitmqctl stop
verdi profile delete --force --delete-data $PROFILE

pg_ctl -D $PGDATA stop

rm -rf $PGDATA
echo "Fertig: $(date '+%F %T'). CSVs in results_psql_exclusive/aiida/, Rest von $BASE kann weg."
