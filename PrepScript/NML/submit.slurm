#!/bin/bash
#SBATCH -J cresm_run
#SBATCH -p second
#SBATCH -N 1
#SBATCH -n ncpus
#SBATCH --ntasks-per-node=80
#SBATCH -o log.%j
#SBATCH -e log.%j
#SBATCH --exclusive
#SBATCH -t 7-24:00

ulimit -s unlimited
scontrol show hostname > nd
NP=$SLURM_NPROCS
mpirun -np $NP -machinefile nd ./cresm --CASENAME=gl_latlon --CASECoLM=unstructured_cwrf_gridname.nml &> log.run