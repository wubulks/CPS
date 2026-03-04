import os
import sys
import argparse
import configparser
import re
import subprocess
from datetime import datetime
import shutil

parser = argparse.ArgumentParser(description='Create run directory for CRESM')
# 新增参数: casedir
parser.add_argument('-casedir','--casedir', help='The parent directory where the case will be created', required=True)
parser.add_argument('-casename','--casename', help='name for entire run', required=True)
parser.add_argument('-griddir','--griddir', help='Full path to Grid directory', required=True)
parser.add_argument('-icbcdir','--icbcdir', help='Full path to ICBC directory', required=True)
parser.add_argument('-st','--starttime', help='YYYYMMDDHH', required=True)
parser.add_argument('-et','--endtime', help='YYYYMMDDHH', required=True)
parser.add_argument('-ncpus','--ncpus', help='number of cpus', required=True)
parser.add_argument('-jms','--job_management_system', choices=['lsf', 'slurm'], required=True)
parser.add_argument('-ini','--ini', help='config.ini path', default='../prep.ini')
parser.add_argument('-fvcom','--fvcom', help='True/False', default='False')
parser.add_argument('-yearly', '--yearly', action='store_true', help='If true, link ICBC files without renaming and copy wrfinput for start year')
args = parser.parse_args()

# --- 路径预处理 ---
# 使用 abspath 确保在切换工作目录后，输入的路径依然有效
base_casedir = os.path.abspath(args.casedir)
grid_path = os.path.abspath(args.griddir)
icbc_path = os.path.abspath(args.icbcdir)
rundir = os.path.join(base_casedir, "RUN_" + args.casename)
styear = args.starttime[0:4]

gridname = os.path.basename(grid_path.strip('/')).replace('Grid_', '')
ncpus = str(args.ncpus)
fvcom = args.fvcom.lower() in ('true', '1', 't')

# 读取配置
config = configparser.ConfigParser()
config.read(args.ini)
CRESM_EXE_ALO = config['MAKE']['CRESM_EXE_ALO']
CRESM_EXE_AL  = config['MAKE']['CRESM_EXE_AL']
RunScripts_dir = os.path.abspath(config['CRESM']['RunScripts_dir'])
mpirun_dir = config['CRESM']['mpirun_dir']
CWRF_donot_delete_dir = config['CRESM']['CWRF_donot_delete_dir']
CoLM_basic_data_dir = config['CRESM']['CoLM_basic_data_dir']

cresm_exe = CRESM_EXE_ALO if fvcom else CRESM_EXE_AL

# --- 1. 创建并进入运行目录 ---
if not os.path.exists(rundir):
    os.makedirs(rundir)
    print(f"Created directory: {rundir}")
os.chdir(rundir)

# --- 2. 处理 ICBC (重构部分) ---
print(f"Processing ICBC files (Mode: {'Yearly' if args.yearly else 'Standard'})...")

target_files = ['wrfinput_d01', 'wrflowinp_d01', 'wrfbdy_d01', 'wrfsst_d01', 'wrfveg_d01']
icbc_all_files = os.listdir(icbc_path)

if args.yearly:
    # ✅ Yearly 模式：直接 Link 所有文件，不重命名
    for f in icbc_all_files:
        if any(base in f for base in target_files):
            os.system(f"ln -sf {os.path.join(icbc_path, f)} ./")
    
    # 特殊处理 wrfinput：复制当前起始年的作为原始输入
    target_wrfinput = f"wrfinput_d01.{styear}"
    if os.path.exists(os.path.join(icbc_path, target_wrfinput)):
        os.system(f"cp {os.path.join(icbc_path, target_wrfinput)} ./wrfinput_d01_raw")
    else:
        print(f"Error: Yearly mode requires {target_wrfinput} in ICBC dir!")
        sys.exit(1)

else:
    # ✅ 非 Yearly 模式
    # 检查是否存在带年份后缀的文件
    has_suffix = any(re.search(r'\.\d{4}$', f) for f in icbc_all_files if any(b in f for b in target_files))
    
    if has_suffix:
        print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        print(f"WARNING: ICBC files have year suffixes but --yearly is FALSE.")
        print(f"Only files for the start year ({styear}) will be linked and renamed.")
        print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        
        for base in target_files:
            src_name = f"{base}.{styear}"
            if src_name in icbc_all_files:
                os.system(f"ln -sf {os.path.join(icbc_path, src_name)} ./{base}")
    else:
        # 如果没有后缀，直接 Link
        for base in target_files:
            if base in icbc_all_files:
                os.system(f"ln -sf {os.path.join(icbc_path, base)} ./{base}")

    # 为后续脚本准备 wrfinput_d01_raw
    if os.path.exists("wrfinput_d01"):
        os.system("cp wrfinput_d01 wrfinput_d01_raw")

# --- 3. 处理 Grid (Copy 方式) ---
print(f"Copying Grid configuration from {grid_path}")
os.system(f"cp -r {grid_path}/* ./")

# 建立内部引用链接
os.system(f"ln -sf {CWRF_donot_delete_dir}/* ./")
os.system(f"ln -sf htop_rcm_{gridname}.nc htop_rcm.nc")
os.system(f"ln -sf GLMASK_{gridname}_{'FVCOM' if fvcom else 'noFVCOM'}.nc GLMASK.nc")
os.system(f"ln -sf CoLM_ref_{gridname}.nc CoLMref.nc")
os.system(f"ln -sf geo_em.d01_veg.nc geo_em.d01.nc")

# 执行 NCL 和 Python 预处理脚本
os.system(f"python {RunScripts_dir}/cwrf_add_htop.py ./wrfinput_d01_raw")
os.system(f"python {RunScripts_dir}/cwrf_add_glmask.py ./wrfinput_d01_raw")
os.system("mv ./wrfinput_d01_raw ./wrfinput_d01")
os.system("ncl alignlucc.ncl")
os.system("ncl chanlu.ncl")

# --- 4. 修改 Namelists (保留 sed 逻辑) ---
styear, stmonth, stday, sthour = args.starttime[0:4], args.starttime[4:6], args.starttime[6:8], args.starttime[8:10]
etyear, etmonth, etday, ethour = args.endtime[0:4], args.endtime[4:6], args.endtime[6:8], args.endtime[8:10]
stsec = str(int(sthour) * 3600)
etsec = str(int(ethour) * 3600)

# CWRF
if args.yearly:
    yearly_flag = ".true."
else:
    yearly_flag = ".false."
os.system("cp namelist.input_CRESM namelist.input")
for k, v in {"start_year":styear, "start_month":stmonth, "start_day":stday, "start_hour":sthour,
             "end_year":etyear, "end_month":etmonth, "end_day":etday, "end_hour":ethour, "yearly_forcing": yearly_flag}.items():
    os.system(f"sed -i '/{k}/c\ {k}={v}' ./namelist.input")

# CoLM
colm_nml = f"unstructured_cwrf_{gridname}.nml"
if os.path.exists(colm_nml):
    replacements = {
        "gridname": gridname, "styear": styear, "stmonth": stmonth, "stday": stday, "stsec": stsec,
        "etyear": etyear, "etmonth": etmonth, "etday": etday, "etsec": etsec,
        "CoLM_basic_data_dir": CoLM_basic_data_dir
    }
    for k, v in replacements.items():
        os.system(f"sed -i 's|{k}|{v}|g' {colm_nml}")
    os.system(f"sed -i '/DEF_forcing_namelist/c\ DEF_forcing_namelist = \"./noforcing.nml\"' {colm_nml}")

# CRESM namelist.cf
os.system(f"sed -i 's/stymd/{styear+stmonth+stday}/g' ./namelist.cf")
os.system(f"sed -i 's/sthour/{stsec}/g' ./namelist.cf")
os.system(f"sed -i 's/edymd/{etyear+etmonth+etday}/g' ./namelist.cf")
os.system(f"sed -i 's/ethour/{etsec}/g' ./namelist.cf")
os.system(f"sed -i 's/gridname/{gridname}/g' ./namelist.cf")

# --- 5. 设置可执行程序和提交脚本 ---
os.system(f"ln -sf {cresm_exe} ./cresm")
submit_src = "submit.lsf" if args.job_management_system == "lsf" else "submit.slurm"
os.system(f"cp {submit_src} ./submit.sh")
os.system(f"sed -i 's/ncpus/{ncpus}/g' ./submit.sh")
os.system(f"sed -i 's|mpirun|{mpirun_dir}|g' ./submit.sh")
os.system(f"sed -i 's/gridname/{gridname}/g' ./submit.sh")

# --- 6. CoLM 后处理 (重新建立目录结构) ---
colm_work_dir = f"./CoLMrun/unstructured_cwrf_{gridname}"
os.makedirs(f"{colm_work_dir}/history", exist_ok=True)
os.makedirs(f"{colm_work_dir}/landdata", exist_ok=True)
os.makedirs(f"{colm_work_dir}/restart", exist_ok=True)

# 链接土地数据
os.system(f"ln -sf {rundir}/unstructured_cwrf_{gridname}/landdata/* {colm_work_dir}/landdata/")

# 处理 Restart
new_date = datetime.strptime(args.starttime, "%Y%m%d%H")
y, d, s = new_date.strftime("%Y"), new_date.strftime("%j"), f"{new_date.hour * 3600:05d}"
target_rst = f"{y}-{d}-{s}"
src_rst_root = f"{rundir}/unstructured_cwrf_{gridname}/restart"

os.chdir(f"{colm_work_dir}/restart")
os.system(f"ln -sf {src_rst_root}/const ./")

if os.path.exists(os.path.join(src_rst_root, target_rst)):
    os.system(f"cp -r {src_rst_root}/{target_rst} ./")
else:
    folders = [f for f in os.listdir(src_rst_root) if os.path.isdir(os.path.join(src_rst_root, f)) and f != "const"]
    if folders:
        selected = folders[0]
        shutil.copytree(os.path.join(src_rst_root, selected), f"./{target_rst}")
        os.chdir(target_rst)
        old_y, old_d = selected.split("-")[0], selected.split("-")[1]
        for fname in os.listdir("."):
            os.rename(fname, fname.replace(old_y, y).replace(old_d, d))
        os.chdir("..")

# --- 7. FVCOM (如果启用) ---
if fvcom:
    os.chdir(rundir)
    print("Configuring FVCOM...")
    start_date_fv = f"\\\"{styear}-{stmonth}-{stday}\\ {sthour}:00:00\\\""
    end_date_fv = f"\\\"{etyear}-{etmonth}-{etday}\\ {ethour}:00:00\\\""
    os.makedirs("fvcom_output", exist_ok=True)
    if os.path.exists("CN_COAST_run.nml"):
        for tag in ["START_DATE", "END_DATE", "RST_FIRST_OUT", "NC_FIRST_OUT", "NCAV_FIRST_OUT"]:
            val = end_date_fv if "END" in tag else start_date_fv
            os.system(f"sed -i '/{tag}/c\\ {tag} = {val}' ./CN_COAST_run.nml")

print(f"\nDone. Run directory created at: {rundir}")