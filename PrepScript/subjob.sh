#!/bin/bash
#SBATCH --job-name=CPS           ## 作业名称
#SBATCH --nodes=1               
#SBATCH --cpus-per-task=90       ## CPU核数         
#SBATCH --output=out_%j.log       ## 输出文件
#SBATCH --error=err_%j.log        ## 错误文件
#SBATCH --exclusive
#SBATCH --partition=second




# 运行你的程序
source /home/wumej22/.cresm
cd /tera07/zhangsl/wumej22/Omarjan/CRESMDataPrep/PrepScript

python CRESM_Preprocessing_System.py -n CN_30km_PFT -y 2001  -g /hydata01/wumej22/SoftwareCopyright/CPS/Case/CN_30km/CN_30km/Geog_CN_30km -s /hydata01/wumej22/SoftwareCopyright/CPS/Case/CN_30km/CN_30km/CoLMSrf_CN_30km



########################################

# 提交任务
# sbatch subjob.sh

# 查看任务
# squeue -u wumej22

# 取消任务
# scancel 任务ID

# 查看任务状态
# scontrol show job 任务ID

# 查看任务详细信息
# sacct -j 任务ID

#######################################
