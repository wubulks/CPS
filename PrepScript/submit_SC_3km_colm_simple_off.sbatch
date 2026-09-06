#!/usr/bin/env bash
# One-off diagnostic run: reuse existing mesh/surface/initial data and run CoLM
# plus the downstream remap/coupler/gather stages only.
# The stage switches below are applied in memory by the Python driver; the
# repository case.ini is intentionally not changed by this test.
#SBATCH --job-name=sc3km_colm_simple_off
#SBATCH --partition=second
#SBATCH --nodes=1
#SBATCH --ntasks=100
#SBATCH --ntasks-per-node=100
#SBATCH --time=7-00:00:00
#SBATCH --output=/stu01/chenhj23/CPS_clean_Hongjing0506/CASES/SC_3km/Log/slurm.SC_3km_colm_simple_off.%j.out
#SBATCH --error=/stu01/chenhj23/CPS_clean_Hongjing0506/CASES/SC_3km/Log/slurm.SC_3km_colm_simple_off.%j.err

set -Ee -o pipefail

PROJECT=/stu01/chenhj23/CPS_clean_Hongjing0506
PREP=${PROJECT}/PrepScript
CASE=${PROJECT}/CASES/SC_3km
LOGDIR=${CASE}/Log
TEST_TAG=downscaling_simple_off

echo "[test] $(date '+%F %T %z')"
echo "[test] job=${SLURM_JOB_ID:-unknown} host=$(hostname)"
echo "[test] nodelist=${SLURM_JOB_NODELIST:-unknown}"
echo "[test] mode=CoLM+downstream; ICBC/Mesh/MakeSrf/MakeIni=OFF"

source /home/chenhj23/anaconda3/etc/profile.d/conda.sh
conda activate cresm
source /home/zhanghan/.cresm_libs
ulimit -s unlimited
export PYTHONUNBUFFERED=1

cd "${PREP}"

# Preserve the fixed-name downstream logs before the test overwrites them.
for old_log in log.colm log.remap log.cpldata_step1 log.cpldata_step2; do
    if [[ -f "${LOGDIR}/${old_log}" ]]; then
        cp -p "${LOGDIR}/${old_log}" "${LOGDIR}/${old_log}.before_${TEST_TAG}.${SLURM_JOB_ID}"
    fi
done

python -u - <<'PY'
import logging
import os

import CRESM_Preprocessing_System as CPS
import PrepCRESM
import PrepCoLM


PROJECT = "/stu01/chenhj23/CPS_clean_Hongjing0506"
GRID = "SC_3km"
CASE = f"{PROJECT}/CASES/{GRID}"
LOGFILE = f"{CASE}/Log/DataPrepare.{GRID}.colm_simple_off.log"

casecfg = CPS.Read_Config("case.ini")
envcfg = CPS.Read_Config("env.ini")

# Test-only stage switches.  They are intentionally kept in memory so the
# shared case.ini is not changed while this diagnostic job is running.
stage_values = {
    "PrepCWRF": {
        "Go_Ungrib": "False",
        "Go_Metgrid": "False",
        "Go_Real": "False",
        "Go_VBS": "False",
        "Copy_CWRF_Output": "False",
    },
    "PrepCoLM": {
        "Go_MeshGrid": "False",
        "Go_MakeSrf": "False",
        "Go_MakeIni": "False",
        "Go_CoLMTempRun": "True",
        "Go_Remap": "True",
        "Copy_CoLM_Output": "True",
    },
    "PrepCRESM": {
        "Go_Coupler_Prep": "True",
    },
    "GatherData": {
        "Collect_CWRF_Output": "False",
        "Collect_CoLM_Output": "True",
        "Collect_CRESM_Output": "True",
    },
}
for section, options in stage_values.items():
    for option, value in options.items():
        casecfg.set(section, option, value)

logger_root = CPS.Setup_Logger(
    LOGFILE,
    logging.INFO,
    logger_name="CRESMPrep",
    enable_color=False,
)
logger = logging.getLogger("CRESMPrep.main")

logger.info("SC_3km diagnostic run: CoLM and downstream stages only")
logger.info("DEF_USE_Forcing_Downscaling_Simple is supplied by the case NML as .false.")
logger.info("Skipped: ICBC, Mesh, MakeSrf, MakeIni")
logger.info("Run: CoLM, Remap, CopyPrepCoLMResult, Coupler_Prep, Gather_Prepare_Data")

required = [
    f"{CASE}/NMLS/unstructured_cwrf.colm.{GRID}.icbc",
    f"{CASE}/PrepCoLM/First_GenMesh/mesh_cwrf_{GRID}.nc",
    f"{CASE}/PrepCoLM/Second_MakeSrf/CoLMSrf_{GRID}",
    f"{CASE}/PrepCoLM/Second_MakeSrf/unstructured_cwrf_{GRID}",
]
missing = [path for path in required if not os.path.exists(path)]
if missing:
    raise FileNotFoundError("Required existing input missing: " + ", ".join(missing))

try:
    PrepCoLM.Second_CoLMRun(casecfg, envcfg, GRID)
    PrepCoLM.Third_Remap(casecfg, envcfg, GRID)
    PrepCoLM.CopyPrepCoLMResult(casecfg, envcfg, GRID)
    PrepCRESM.Coupler_Prep(casecfg, envcfg, GRID)
    CPS.Gather_Prepare_Data(casecfg, envcfg, GRID)
    CPS.Clean_Temporary_Files(casecfg, envcfg, GRID)
except Exception:
    logger.exception("SC_3km diagnostic run failed")
    raise

logger.info("SC_3km diagnostic run completed successfully")
PY
