#!/usr/bin/env bash
#SBATCH --job-name=sc3km_remain
#SBATCH --partition=second
#SBATCH --nodes=1
#SBATCH --ntasks=100
#SBATCH --ntasks-per-node=100
#SBATCH --time=7-00:00:00
#SBATCH --output=/stu01/chenhj23/CPS_clean_Hongjing0506/CASES/SC_3km/Log/slurm.SC_3km_remaining.%j.out
#SBATCH --error=/stu01/chenhj23/CPS_clean_Hongjing0506/CASES/SC_3km/Log/slurm.SC_3km_remaining.%j.err

set -Ee -o pipefail

PROJECT=/stu01/chenhj23/CPS_clean_Hongjing0506
PREP=$PROJECT/PrepScript
BATCH3=$PROJECT/CASES/SC_3km/PrepCWRF/Second_ICBC/UngribMetgrid_2018-07-04_2018-10-04
GEOG=$PROJECT/CASES/SC_3km/PrepCWRF/First_StaticData/Geog_SC_3km
MODLEVS=/stu01/chenhj23/CRESMDataPrep/CRESM_ToolBox/ToolBox/CWRF-CWPS/util/src/mod_levs.exe
MODLOG=$PROJECT/CASES/SC_3km/Log/log.mod_levs.2018-07-04_2018-10-04

echo "[$(date '+%F %T')] job=${SLURM_JOB_ID:-unknown} host=$(hostname -f)"
echo "[$(date '+%F %T')] nodelist=${SLURM_JOB_NODELIST:-unknown} ntasks=${SLURM_NTASKS:-unknown}"

# The non-interactive .bashrc returns before conda initialization. Load the
# runtime explicitly before importing the preprocessing modules.
source /home/chenhj23/anaconda3/etc/profile.d/conda.sh
conda activate cresm
source /home/zhanghan/.cresm_libs
ulimit -s unlimited
set -u
export PYTHONUNBUFFERED=1

echo "[$(date '+%F %T')] python=$(command -v python)"
python --version
echo "[$(date '+%F %T')] mpirun=$(command -v mpirun || true)"

test -d "$PROJECT"
test -d "$GEOG"
test -s "$BATCH3/3D:2018-08-25_00"
test -x "$MODLEVS"

echo "[$(date '+%F %T')] resume batch3 mod_levs: 2018-08-25_00"
cd "$BATCH3"
"$MODLEVS" "3D:2018-08-25_00" "3D:2018-08-25_00.tmp" >> "$MODLOG" 2>&1
test -s "3D:2018-08-25_00.tmp"
mv -f "3D:2018-08-25_00.tmp" "3D:2018-08-25_00"

echo "[$(date '+%F %T')] resume batch3 metgrid"
cd "$PREP"
python -u - <<'PY'
from configparser import ConfigParser, ExtendedInterpolation
from datetime import datetime

from Utils import ICBC


def read_config(path):
    cfg = ConfigParser(interpolation=ExtendedInterpolation())
    cfg.read(path)
    return cfg


casecfg = read_config("case.ini")
envcfg = read_config("env.ini")
gridname = "SC_3km"
start_time = datetime.strptime("2018-07-04_00:00:00", "%Y-%m-%d_%H:%M:%S")
end_time = datetime.strptime("2018-10-04_00:00:00", "%Y-%m-%d_%H:%M:%S")

# Batch 3 already has complete 2D/3D/SST ungrib products. Link only the
# CWPS/static inputs needed by metgrid, then run metgrid/SST averaging.
ICBC.Link_CWPS_Files(casecfg, envcfg, gridname, start_time, end_time)
ICBC.Metgrid(casecfg, envcfg, gridname, start_time, end_time, cleanup_ungrib=False)
PY

echo "[$(date '+%F %T')] batch3 metgrid finished; starting remaining pipeline"
cd "$PREP"
python -u CRESM_Preprocessing_System.py -n SC_3km -g "$GEOG"

echo "[$(date '+%F %T')] SC_3km remaining ICBC and post-processing finished"
