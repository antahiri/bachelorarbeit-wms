# AiiDA-Einrichtung

Bei AiiDA liegt ein wesentlicher Teil der Konfiguration nicht in Dateien, sondern
als Zustand im Profil und in der Datenbank. Dieses Dokument haelt die
Einrichtung der vier verwendeten Konfigurationen fest.


## Lokal (macOS), PostgreSQL

```
 ✔ version:     AiiDA v2.8.0
 ✔ config:      /Users/Hp/.aiida
 ✔ profile:     aiida_pipeline
 ✔ storage:     Storage for 'aiida_pipeline' [open] @ postgresql+psycopg://aiida:***@localhost:5432/aiida_db / DiskObjectStoreRepository: ce51dbad1df4465db4e147c942f66c03 | /Users/Hp/.aiida/repository/aiida_pipeline/container
Warning: RabbitMQ v4.3.1 is not supported and will cause unexpected problems!
Warning: It can cause long-running workflows to crash and jobs to be submitted multiple times.
Warning: See https://github.com/aiidateam/aiida-core/wiki/RabbitMQ-version-to-use for details.
 ✔ broker:      RabbitMQ v4.3.1 @ amqp://guest:***@127.0.0.1:5672?heartbeat=600
 ✔ daemon:      Daemon is running with PID 5108

---------------------------  ------------------------------------
Label                        localhost_aiida
PK                           1
UUID                         61fc9cef-fb6c-4dcb-92bb-93f45d3e085d
Description
Hostname                     localhost
Transport type               core.local
Scheduler type               core.direct
Work directory               /Users/Hp/.aiida/workdir/{username}
Shebang                      #!/bin/bash
Mpirun command               mpirun -np {tot_num_mpiprocs}
Default #procs/machine       1
Default memory (kB)/machine
Prepend text
Append text
---------------------------  ------------------------------------

--------------------------  ----------------------------------------------
PK                          2888
UUID                        707791d7-31b7-46e4-9ad3-b320e8091f44
Type                        core.code.installed
Label                       aiida_python312
Description
Computer                    localhost_aiida (localhost), pk: 1
Default `CalcJob` plugin
Escape using double quotes  False
Run with MPI                False
Prepend script
Append script
Filepath executable         /Users/Hp/miniforge3/envs/aiida/bin/python3.12
--------------------------  ----------------------------------------------
```


## MOGON, dedizierte Messung mit PostgreSQL (Hauptkonfiguration)

Profil `psql_<JOBID>`, temporaer je Lauf. Das Profil wird im Jobskript erzeugt
und nach Abschluss wieder entfernt, sodass jede Messung mit einer leeren
Datenbank startet. Datenbank,
Repository und Arbeitsverzeichnis liegen auf dem geteilten Dateisystem /fshpc,
wie bei den uebrigen Systemen. Die folgenden Angaben stammen aus dem Jobskript
und dem Log des exklusiven Laufs (Job 494992).

```
 ✔ version:     AiiDA v2.8.0
 ✔ config:      /home/antahiri/.aiida
 ✔ profile:     psql_494992
 ✔ storage:     Storage for 'psql_494992' [open] @ postgresql+psycopg://aiida:***@127.0.0.1:5433/aiida_psql / DiskObjectStoreRepository: /fshpc/antahiri/wms_hpc_benchmark/psql_test_494992/repo
 ✔ broker:      RabbitMQ v3.8.14 @ amqp://guest:***@127.0.0.1:5672?heartbeat=600
 ✔ daemon:      Daemon is running as PID 1526755 since 2026-07-26 05:28:54, Active workers [4]

---------------------------  -----------------------------------------------------------------
Label                        mogon-local
Description                  MOGON dediziert, psql auf fshpc
Hostname                     localhost
Transport type               core.local
Scheduler type               core.direct
Work directory               /fshpc/antahiri/wms_hpc_benchmark/psql_test_494992/aiida_work
Shebang                      #!/bin/bash
Mpirun command               mpirun -np {tot_num_mpiprocs}
Default #procs/machine       1
Default memory (kB)/machine
Prepend text
Append text
Safe interval                0
---------------------------  -----------------------------------------------------------------

--------------------------  ----------------------------------------------
Type                        core.code.installed
Label                       aiida_python312
Description
Computer                    mogon-local (localhost)
Default `CalcJob` plugin    core.calcjob
Escape using double quotes  False
Run with MPI                False
Prepend script
Append script
Filepath executable         /home/antahiri/.conda/envs/aiida/bin/python3
--------------------------  ----------------------------------------------
```


## MOGON, dedizierte Messung mit SQLite (Vergleichskonfiguration)

Profil `aiida_mogon`. Dient dem Vergleich der Datenbank-Backends bei
gleichbleibendem Speicherort sowie, in einer weiteren Variante mit
Speicher auf dem lokalen Scratch des knotens, der Untersuchung des Einflusses
des Speicherorts.

```
 ✔ version:     AiiDA v2.8.0
 ✔ config:      /home/antahiri/.aiida
 ✔ profile:     aiida_mogon
 ✔ storage:     SqliteDosStorage[/fshpc/antahiri/.aiida/repository/aiida_mogon]: open,
 ✔ broker:      RabbitMQ v3.8.14 @ amqp://guest:***@127.0.0.1:5672/?heartbeat=600
 ⏺ daemon:      The daemon is not running.

---------------------------  -------------------------------------------
Label                        mogon-local
PK                           2
UUID                         7a9d5943-0a47-446c-a925-b053d415976f
Description                  MOGON dediziert, direct scheduler
Hostname                     localhost
Transport type               core.local
Scheduler type               core.direct
Work directory               /fshpc/antahiri/aiida_work_local/{username}
Shebang                      #!/bin/bash
Mpirun command               mpirun -np {tot_num_mpiprocs}
Default #procs/machine       1
Default memory (kB)/machine
Prepend text
Append text
---------------------------  -------------------------------------------

Full label                          Pk  Entry point
--------------------------------  ----  -------------------
add_on_mogon@mogon                   5  core.code.installed
aiida_python312@mogon-local        331  core.code.installed
compute_chunk_on_mogon@mogon        65  core.code.installed
workflow_step_runner_mogon@mogon    89  core.code.installed
```


## MOGON, Slurm-Modus

Profil `slurm_modus`. Einziger Unterschied zur dedizierten Konfiguration ist der
Scheduler-Typ: statt der direkten Ausfuehrung auf dem Knoten wird jeder Task als
eigener Slurm-Job eingereicht.

```
 ✔ version:     AiiDA v2.8.0
 ✔ config:      /home/antahiri/.aiida
 ✔ profile:     slurm_modus
 ✔ storage:     SqliteDosStorage[/home/antahiri/.aiida/repository/sqlite_dos_8b5220ffbb4843e6973c144a627061f3]: open,
 ✔ broker:      RabbitMQ v3.8.14 @ amqp://guest:***@127.0.0.1:5672?heartbeat=600
 ✔ daemon:      Daemon is running with PID 1669813

---------------------------  --------------------------------------
Label                        mogon-slurm
PK                           2
UUID                         63097619-8979-4eb0-9f4d-12afe1ebee00
Description                  MOGON mit Slurm-Scheduler, Normalmodus
Hostname                     localhost
Transport type               core.local
Scheduler type               core.slurm
Work directory               /fshpc/antahiri/slurm_modus/aiida/work
Shebang                      #!/bin/bash
Mpirun command               mpirun -np {tot_num_mpiprocs}
Default #procs/machine       1
Default memory (kB)/machine
Prepend text
Append text
---------------------------  --------------------------------------

Full label                     Pk  Entry point
---------------------------  ----  -------------------
aiida_python312@mogon-slurm     1  core.code.installed
```

