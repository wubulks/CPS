#! /stu01/wumej22/Anaconda3/bin/python
# -*- coding: utf-8 -*-

"""
===============================================================================
Module Name   : ICBC (Initial and Boundary Conditions Preparation Module)
Description   : Handles the data preparation workflow for the CWRF model's
                initial and boundary conditions (IC/BC) using various forcing datasets.
                
                Key Functions:
                - Ungrib : converts source data into WRF intermediate format.
                    · Ungrib_CFSV2          
                    · Ungrib_ERA5          
                    · Ungrib_MPI_ESM1_2_HR_hist 
                    · Ungrib_MPI_ESM1_2_HR_ssp245 
                - Metgrid
                - Real

Author        : Omarjan
Institution   : School of Atmospheric Sciences, Sun Yat-sen University (SYSU)
Created       : 2025-05-25
Last Modified : 2026-01-21
===============================================================================
"""

import os
import re
import sys
import time
import glob
import logging
import pandas as pd
from datetime import timedelta
import configparser
from pathlib import Path
from . import Tools as Tools
from . import Consts as Consts

logger = logging.getLogger("CRESMPrep." + __name__)

def Ungrib_CFSV2(casecfg, envcfg, gridname, start_time, end_time):
    old_path = os.getcwd()
    CaseOutputPath = casecfg.get(gridname, 'CaseOutputPath')
    ForcingDataName = casecfg.get(gridname, 'ForcingDataName')
    ForcingDataName = ForcingDataName.strip().lower()
    Go_Ungrib = casecfg.getboolean('PrepCWRF', 'Go_Ungrib')
    Forc_2D_Path = envcfg.get(ForcingDataName, 'Forc_2D_Path')
    Forc_3D_Path = envcfg.get(ForcingDataName, 'Forc_3D_Path')
    Forc_SST_Path = envcfg.get(ForcingDataName, 'Forc_SST_Path')
    SYS_CWRF = envcfg.get('Environment', 'SYS_CWRF')
    
    prefix = ["2D","3D","SST"]
    
    # mkdir
    time_str = f'{start_time.year}-{start_time.month:02d}-{start_time.day:02d}_{end_time.year}-{end_time.month:02d}-{end_time.day:02d}'
    cmd = f'mkdir -p {CaseOutputPath}/{gridname}/PrepCWRF/Second_ICBC/UngribMetgrid_{time_str}'
    Tools.Run_CMD(cmd, "Create directory UngribMetgrid")
    cmd = f'mkdir -p {CaseOutputPath}/{gridname}/PrepCWRF/Second_ICBC/UngribMetgrid_{time_str}/2D'
    Tools.Run_CMD(cmd, "Create directory 2D")
    cmd = f'mkdir -p {CaseOutputPath}/{gridname}/PrepCWRF/Second_ICBC/UngribMetgrid_{time_str}/3D'
    Tools.Run_CMD(cmd, "Create directory 3D")
    cmd = f'mkdir -p {CaseOutputPath}/{gridname}/PrepCWRF/Second_ICBC/UngribMetgrid_{time_str}/SST'
    Tools.Run_CMD(cmd, "Create directory SST")
    
    # cd to PrepCWRF/Second_ICBC/time_str
    os.chdir(f'{CaseOutputPath}/{gridname}/PrepCWRF/Second_ICBC/UngribMetgrid_{time_str}')
    
    #---------------------- Ungrid ----------------------
    if Go_Ungrib:
        logger.info(f'{Consts.S4}==========> Ungrid {time_str} <==========')
        
        if '<HH>' in Forc_2D_Path:
            freq = '6h'
        elif '<DD>' in Forc_2D_Path:
            freq = 'D'
        elif '<MM>' in Forc_2D_Path:
            freq = 'M'
        elif '<YYYY>' in Forc_2D_Path:
            freq = 'Y'
        else:
            raise ValueError(f"Cannot determine frequency from path: {Forc_2D_Path}")

        # link 2D, 3D, SST files into temporary directory
        for itime in pd.date_range(start=start_time, end=end_time, freq=freq):
            # link 2D files into temporary directory
            file_2D = Tools.Get_Forc_File_Path(Forc_2D_Path, itime)
            if Tools.File_Exist(file_2D, level='error'):
                Tools.Link(file_2D, './2D/')
    
            # link 3D files into temporary directory
            file_3D = Tools.Get_Forc_File_Path(Forc_3D_Path, itime)
            if Tools.File_Exist(file_3D, level='error'):
                Tools.Link(file_3D, './3D/')
                
            # link SST files into temporary directory
            file_SST = Tools.Get_Forc_File_Path(Forc_SST_Path, itime)
            if Tools.File_Exist(file_SST, level='error'):
                Tools.Link(file_SST, './SST/')
        
        # Alternating Time in namelist.wps
        start_time_str = start_time.strftime('%Y-%m-%d_%H:%M:%S')
        end_time_str = end_time.strftime('%Y-%m-%d_%H:%M:%S')
        str_use = f"start_date = '{start_time_str}',"
        cmd = f"sed -i '/start_date/c\\{str_use}' namelist.wps"
        Tools.Run_CMD(cmd, "Alternating Time in namelist.wps")
        str_use = f"end_date = '{end_time_str}', "
        cmd = f"sed -i '/end_date/c\\{str_use}' namelist.wps"
        Tools.Run_CMD(cmd, "Alternating Time in namelist.wps")
        str_use="interval_seconds = 21600,"
        cmd = f"sed -i '/interval_seconds/c\\{str_use}' namelist.wps"
        Tools.Run_CMD(cmd, "Alternating Time in namelist.wps")
        
        # ungrib 2D files
        Tools.Link('./Variable_Tables/Vtable.CFSR_sfc_flxf06', 'Vtable')
        cmd = f'rm -rf GRIBFILE.*'
        Tools.Run_CMD(cmd, "Remove old GRIBFILE")
        cmd = f'./link_grib.csh  2D/*'
        Tools.Link("./link_grib.csh", "./2D/*")
        str_use="prefix = \""+prefix[0]+"\","
        cmd = f"sed -i '/prefix/c\\{str_use}' namelist.wps"
        Tools.Run_CMD(cmd, "Alternating prefix in namelist.wps")
        log_file = f'{CaseOutputPath}/{gridname}/Log/log.ungrib_2D.{time_str}'
        cmd = f'./ungrib.exe > {log_file} 2>&1'
        Tools.Run_CMD(cmd, "Run ungrib.exe", env=SYS_CWRF)
        os.system(f'mv ungrib.log ungrib_2D.log')
        Tools.Check_Ungrib_Finish(os.getcwd(), prefix[0], 6, start_time, end_time)
        logger.info(f'{Consts.S4}✓  {time_str}: Ungrib 2D')
        
        #ungrib SST files
        Tools.Link('./Variable_Tables/Vtable.SST', 'Vtable')
        cmd = f'rm -rf GRIBFILE.*'
        Tools.Run_CMD(cmd, "Remove old GRIBFILE")
        Tools.Link("./link_grib.csh", "./SST/*")
        str_use="prefix = \""+prefix[2]+"\","
        cmd = f"sed -i '/prefix/c\\{str_use}' namelist.wps"
        Tools.Run_CMD(cmd, "Alternating prefix in namelist.wps")
        log_file = f'{CaseOutputPath}/{gridname}/Log/log.ungrib_sst.{time_str}'
        cmd = f'./ungrib.exe > {log_file} 2>&1'
        Tools.Run_CMD(cmd, "Run ungrib.exe", env=SYS_CWRF)
        os.system(f'mv ungrib.log ungrib_sst.log')
        Tools.Check_Ungrib_Finish(os.getcwd(), prefix[2], 6, start_time, end_time)
        logger.info(f'{Consts.S4}✓  {time_str}: Ungrib SST')
        
        # ungrib 3D files
        Tools.Link('./Variable_Tables/Vtable.CFSR_press_pgbh06', 'Vtable')
        cmd = f'rm -rf GRIBFILE.*'
        Tools.Run_CMD(cmd, "Remove old GRIBFILE")
        Tools.Link("./link_grib.csh", "./3D/*")
        Tools.Run_CMD(cmd, "Link 3D files")
        str_use="prefix = \""+prefix[1]+"\","
        cmd = f"sed -i '/prefix/c\\{str_use}' namelist.wps"
        Tools.Run_CMD(cmd, "Alternating prefix in namelist.wps")
        log_file = f'{CaseOutputPath}/{gridname}/Log/log.ungrib_3D.{time_str}'
        cmd = f'./ungrib.exe > {log_file} 2>&1'
        Tools.Run_CMD(cmd, "Run ungrib.exe", env=SYS_CWRF)
        Tools.Run_CMD(f'mv ungrib.log ungrib_3D.log', "Rename ungrib log file")
        Tools.Check_Ungrib_Finish(os.getcwd(), prefix[1], 6, start_time, end_time)
        logger.info(f'    ✓  {time_str}: Ungrib 3D')
        
        #run mod_levs.exe
        Tools.Run_CMD("rm -f *.tmp", "Remove temporary files")
        log_file = f'{CaseOutputPath}/{gridname}/Log/log.mod_levs.{time_str}'
        Tools.Run_CMD(f"rm -f {log_file}", "Remove old log file")
        file3d = glob.glob(f'./{prefix[1]}:*')
        # sort the file list
        file3d.sort()
        for fname in file3d:
            cmd = f'./mod_levs.exe {fname} {fname}.tmp >> {log_file} 2>&1'
            Tools.Run_CMD(cmd, "Run mod_levs.exe")
            cmd = f'mv -f {fname}.tmp {fname}'
            Tools.Run_CMD(cmd, "Move temporary file to original file")
        logger.info(f'{Consts.S4}✓  {time_str}: mod_levs')
    else:
        logger.info(f'{Consts.S4}==========> Skip Ungrib {time_str} <==========')

    # removing temporary files
    os.chdir(old_path)



def Ungrib_ERA5(casecfg, envcfg, gridname, start_time, end_time):
    old_path = os.getcwd()
    CaseOutputPath = casecfg.get(gridname, 'CaseOutputPath')
    ForcingDataName = casecfg.get(gridname, 'ForcingDataName')
    ForcingDataName = ForcingDataName.strip().lower()
    Go_Ungrib = casecfg.getboolean('PrepCWRF', 'Go_Ungrib')
    Forc_2D_Path = envcfg.get(ForcingDataName, 'Forc_2D_Path')
    Forc_3D_Path = envcfg.get(ForcingDataName, 'Forc_3D_Path')
    Forc_SST_Path = envcfg.get(ForcingDataName, 'Forc_SST_Path')
    SYS_CWRF = envcfg.get('Environment', 'SYS_CWRF')

    prefix = ["2D","3D","SST"]
    
    # mkdir
    time_str = f'{start_time.year}-{start_time.month:02d}-{start_time.day:02d}_{end_time.year}-{end_time.month:02d}-{end_time.day:02d}'
    cmd = f'mkdir -p {CaseOutputPath}/{gridname}/PrepCWRF/Second_ICBC/UngribMetgrid_{time_str}'
    Tools.Run_CMD(cmd, "Create directory UngribMetgrid")
    cmd = f'mkdir -p {CaseOutputPath}/{gridname}/PrepCWRF/Second_ICBC/UngribMetgrid_{time_str}/2D'
    Tools.Run_CMD(cmd, "Create directory 2D")
    cmd = f'mkdir -p {CaseOutputPath}/{gridname}/PrepCWRF/Second_ICBC/UngribMetgrid_{time_str}/3D'
    Tools.Run_CMD(cmd, "Create directory 3D")
    cmd = f'mkdir -p {CaseOutputPath}/{gridname}/PrepCWRF/Second_ICBC/UngribMetgrid_{time_str}/SST'
    Tools.Run_CMD(cmd, "Create directory SST")
    
    # cd to PrepCWRF/Second_ICBC/time_str
    os.chdir(f'{CaseOutputPath}/{gridname}/PrepCWRF/Second_ICBC/UngribMetgrid_{time_str}')
    
    #---------------------- Ungrid ----------------------
    if Go_Ungrib:
        logger.info(f'{Consts.S4}==========> Ungrid {time_str} <==========')

        if '<HH>' in Forc_2D_Path:
            freq = '6h'
        elif '<DD>' in Forc_2D_Path:
            freq = 'D'
        elif '<MM>' in Forc_2D_Path:
            freq = 'M'
        elif '<YYYY>' in Forc_2D_Path:
            freq = 'Y'
        else:
            raise ValueError(f"Cannot determine frequency from path: {Forc_2D_Path}")

        # link 2D, 3D, SST files into temporary directory
        for itime in pd.date_range(start=start_time, end=end_time, freq='D'):
            # link 2D files into temporary directory
            file_2D = Tools.Get_Forc_File_Path(Forc_2D_Path, itime)
            if Tools.File_Exist(file_2D, level='error'):
                Tools.Link(file_2D, './2D/')
    
            # link 3D files into temporary directory
            file_3D = Tools.Get_Forc_File_Path(Forc_3D_Path, itime)
            if Tools.File_Exist(file_3D, level='error'):
                Tools.Link(file_3D, './3D/')
                
            # link SST files into temporary directory
            file_SST = Tools.Get_Forc_File_Path(Forc_SST_Path, itime)
            if Tools.File_Exist(file_SST, level='error'):
                Tools.Link(file_SST, './SST/')
        
        # Alternating Time in namelist.wps
        start_time_str = start_time.strftime('%Y-%m-%d_%H:%M:%S')
        end_time_str = end_time.strftime('%Y-%m-%d_%H:%M:%S')
        str_use = f"start_date = '{start_time_str}',"
        cmd = f"sed -i '/start_date/c\\{str_use}' namelist.wps"
        Tools.Run_CMD(cmd, "Alternating Time in namelist.wps")
        str_use = f"end_date = '{end_time_str}', "
        cmd = f"sed -i '/end_date/c\\{str_use}' namelist.wps"
        Tools.Run_CMD(cmd, "Alternating Time in namelist.wps")
        str_use="interval_seconds = 21600,"
        cmd = f"sed -i '/interval_seconds/c\\{str_use}' namelist.wps"
        Tools.Run_CMD(cmd, "Alternating Time in namelist.wps")
        
        # ungrib 2D files
        Tools.Link("./Variable_Tables/Vtable.ECMWF", "Vtable")
        cmd = f'rm -rf GRIBFILE.*'
        Tools.Run_CMD(cmd, "Remove old GRIBFILE")
        cmd = f'./link_grib.csh  2D/*'
        Tools.Run_CMD(cmd, "Link 2D files")
        str_use="prefix = \""+prefix[0]+"\","
        cmd = f"sed -i '/prefix/c\\{str_use}' namelist.wps"
        Tools.Run_CMD(cmd, "Alternating prefix in namelist.wps")
        log_file = f'{CaseOutputPath}/{gridname}/Log/log.ungrib_2D.{time_str}'
        cmd = f'./ungrib.exe > {log_file} 2>&1'
        Tools.Run_CMD(cmd, "Run ungrib.exe", env=SYS_CWRF)
        Tools.Run_CMD(f'mv ungrib.log ungrib_2D.log', "Rename ungrib log file")
        Tools.Check_Ungrib_Finish(os.getcwd(), prefix[0], 6, start_time, end_time)
        logger.info(f'{Consts.S4}✓  {time_str}: Ungrib 2D')
        
        #ungrib SST files
        Tools.Link("./Variable_Tables/Vtable.ECMWF.SST", "Vtable")
        cmd = f'rm -rf GRIBFILE.*'
        Tools.Run_CMD(cmd, "Remove old GRIBFILE")
        cmd = f'./link_grib.csh  SST/*'
        Tools.Run_CMD(cmd, "Link SST files")
        str_use="prefix = \""+prefix[2]+"\","
        cmd = f"sed -i '/prefix/c\\{str_use}' namelist.wps"
        Tools.Run_CMD(cmd, "Alternating prefix in namelist.wps")
        log_file = f'{CaseOutputPath}/{gridname}/Log/log.ungrib_sst.{time_str}'
        cmd = f'./ungrib.exe > {log_file} 2>&1'
        Tools.Run_CMD(cmd, "Run ungrib.exe", env=SYS_CWRF)
        os.system(f'mv ungrib.log ungrib_sst.log')
        Tools.Check_Ungrib_Finish(os.getcwd(), prefix[2], 6, start_time, end_time)
        logger.info(f'{Consts.S4}✓  {time_str}: Ungrib SST')

        # ungrib 3D files
        Tools.Link("./Variable_Tables/Vtable.ECMWF", "Vtable")
        cmd = f'rm -rf GRIBFILE.*'
        Tools.Run_CMD(cmd, "Remove old GRIBFILE")
        cmd = f'./link_grib.csh  3D/*'
        Tools.Run_CMD(cmd, "Link 3D files")
        str_use="prefix = \""+prefix[1]+"\","
        cmd = f"sed -i '/prefix/c\\{str_use}' namelist.wps"
        Tools.Run_CMD(cmd, "Alternating prefix in namelist.wps")
        log_file = f'{CaseOutputPath}/{gridname}/Log/log.ungrib_3D.{time_str}'
        cmd = f'./ungrib.exe > {log_file} 2>&1'
        Tools.Run_CMD(cmd, "Run ungrib.exe", env=SYS_CWRF)
        os.system(f'mv ungrib.log ungrib_3D.log')
        Tools.Check_Ungrib_Finish(os.getcwd(), prefix[1], 6, start_time, end_time)
        logger.info(f'    ✓  {time_str}: Ungrib 3D')
        
        #run mod_levs.exe
        os.system("rm -f *.tmp")
        log_file = f'{CaseOutputPath}/{gridname}/Log/log.mod_lev.{time_str}'
        os.system(f'rm -f {log_file}')
        file3d = glob.glob(f'./{prefix[1]}:*')
        # sort the file list
        file3d.sort()
        for fname in file3d:
            cmd = f'./mod_levs.exe {fname} {fname}.tmp >> {log_file} 2>&1'
            Tools.Run_CMD(cmd, "Run mod_levs.exe", env=SYS_CWRF)
            cmd = f'mv -f {fname}.tmp {fname}'
            Tools.Run_CMD(cmd, "Move temporary file to original file")
        logger.info(f'{Consts.S4}✓  {time_str}: mod_lev')
    else:
        logger.info(f'{Consts.S4}==========> Skip Ungrib {time_str} <==========')

    # removing temporary files
    os.chdir(old_path)



def Ungrib_MPI_ESM1_2_HR_hist(casecfg, envcfg, gridname, start_time, end_time):
    old_path = os.getcwd()
    ForcingDataName = casecfg.get(gridname, 'ForcingDataName')
    ForcingDataName = ForcingDataName.strip().lower()
    CaseOutputPath = casecfg.get(gridname, 'CaseOutputPath')
    Go_Ungrib = casecfg.getboolean('PrepCWRF', 'Go_Ungrib')
    Forc_Info = envcfg.get(ForcingDataName, 'Forc_Info')
    CWRFCoreNum = casecfg.getint('PrepCWRF', 'CWRFCoreNum')
    cresmenv = envcfg.get('Environment', 'CONDA_CRESM')
    ungribenv = envcfg.get('Environment', 'CONDA_UNGRIB')

    # mkdir
    time_str = f'{start_time.year}-{start_time.month:02d}-{start_time.day:02d}_{end_time.year}-{end_time.month:02d}-{end_time.day:02d}'
    cmd = f'mkdir -p {CaseOutputPath}/{gridname}/PrepCWRF/Second_ICBC/UngribMetgrid_{time_str}'
    Tools.Run_CMD(cmd, "Create directory UngribMetgrid")
    cmd = f'mkdir -p {CaseOutputPath}/{gridname}/PrepCWRF/Second_ICBC/UngribMetgrid_{time_str}/DATA'
    Tools.Run_CMD(cmd, "Create directory DATA")
    
    # cd to PrepCWRF/Second_ICBC/time_str
    os.chdir(f'{CaseOutputPath}/{gridname}/PrepCWRF/Second_ICBC/UngribMetgrid_{time_str}')
    prefix = ["2D","3D","SST"]
    
    #---------------------- Ungrid ----------------------
    if Go_Ungrib:
        logger.info(f'{Consts.S4}==========> Ungrid {time_str} <==========')

        forcinfo = configparser.ConfigParser(inline_comment_prefixes=(';', '#'))
        forcinfo.read(Forc_Info)
        sections = forcinfo.sections()

        # 数据目录（若无 BaseInfo/DataDir，就用 Forc_Info 所在目录）
        if 'BaseInfo' not in sections:
            raise ValueError("Forc_Info file must contain a [BaseInfo] section with DataDir specified.")

        base_data_dir = forcinfo.get('BaseInfo', 'DataDir').strip()
        fileindex = forcinfo.get('BaseInfo', 'FileIndex').strip()
        datastart = pd.to_datetime(forcinfo.get('BaseInfo', 'DataStart').replace("_", " "), errors='raise')
        dataend   = pd.to_datetime(forcinfo.get('BaseInfo', 'DataEnd').replace("_", " "), errors='raise')
        # 统一把 start/end 转成 pandas Timestamp
        ts_start = pd.Timestamp(start_time)
        ts_end   = pd.Timestamp(end_time)

        if ts_start < pd.Timestamp(datastart) or ts_end > pd.Timestamp(dataend):
            raise ValueError(f"Requested time range {ts_start} to {ts_end} is out of bounds ({datastart} to {dataend}).")

        fileindex_path = os.path.join(base_data_dir, fileindex)
        Tools.File_Exist(fileindex_path, level='error')
        Tools.File_Exist(base_data_dir, level='error')

        # 读取文件索引
        fileindex_df = pd.read_csv(fileindex_path)
        
        # 获取 BaseInfo 中的变量分组
        var_groups = {}
        for key in ["atm3D", "atm2D", "land"]:
            if forcinfo.has_option("BaseInfo", key):
                vars_str = forcinfo.get("BaseInfo", key).strip()
                if vars_str:
                    var_groups[key] = [v.strip() for v in vars_str.split(",") if v.strip()]
        all_vars = sorted(set(sum(var_groups.values(), [])))
        if forcinfo.has_option("BaseInfo", "const"):
            const_vars = [v.strip() for v in forcinfo.get("BaseInfo", "const").strip().split(",") if v.strip()]
        else:
            const_vars = []
            logger.warning("No constant variables specified under 'const' in [BaseInfo].")

        used_files = []
        # === 遍历每个变量，筛选对应文件 ===
        for var in all_vars:
            varname = forcinfo.get(var, "VarNameInData", fallback=var).strip()
            timefreq = forcinfo.get(var, "TemporalRes", fallback="6H").strip().upper()

            # 从 fileindex_df 中找出包含该变量的文件
            pattern = fr'(?<![A-Za-z0-9_]){re.escape(varname)}(?![A-Za-z0-9_])'
            df_var = fileindex_df[fileindex_df["Variables"].str.contains(pattern, na=False, regex=True)].copy()
            # df_var = fileindex_df[fileindex_df["Variables"].str.contains(varname, na=False)].copy()

            if df_var.empty:
                logger.warning(f"[{var}] No files found containing variable '{varname}'.")
                continue

            # 解析时间列
            df_var["StartTime"] = pd.to_datetime(df_var["StartTime"], errors="coerce")
            df_var["EndTime"] = pd.to_datetime(df_var["EndTime"], errors="coerce")

            # 基于频率的时间范围过滤
            if timefreq.endswith("M"):  # 月度：精确到 YYYY-MM
                # 把数据与查询区间都转为“月”Period
                start_month = pd.Period(pd.to_datetime(ts_start), freq="M")
                end_month   = pd.Period(pd.to_datetime(ts_end),   freq="M")

                df_var["StartMonth"] = df_var["StartTime"].dt.to_period("M")
                df_var["EndMonth"]   = df_var["EndTime"].dt.to_period("M")

                mask = (df_var["EndMonth"] >= start_month) & (df_var["StartMonth"] <= end_month)
                df_used = df_var.loc[mask].dropna(subset=["FileName"])

            elif timefreq.endswith("D"):  # 日度：精确到天（可用 Period('D')，更直观）
                start_day = pd.Period(pd.to_datetime(ts_start), freq="D")
                end_day   = pd.Period(pd.to_datetime(ts_end),   freq="D")

                df_var["StartDay"] = df_var["StartTime"].dt.to_period("D")
                df_var["EndDay"]   = df_var["EndTime"].dt.to_period("D")

                mask = (df_var["EndDay"] >= start_day) & (df_var["StartDay"] <= end_day)
                df_used = df_var.loc[mask].dropna(subset=["FileName"])
            else:
                # 小时级（含 6H 等）：保持原来的时间戳比较最简单可靠
                df_used = df_var[
                    (df_var["EndTime"]   >= pd.to_datetime(ts_start)) &
                    (df_var["StartTime"] <= pd.to_datetime(ts_end))
                ].dropna(subset=["FileName"])

            if df_used.empty:
                logger.warning(f"Variable [{var}] No time-overlapping files found in range {ts_start}–{ts_end}")
                continue

            # 链接文件
            target_dir = Path(f"./DATA/{var}")
            target_dir.mkdir(parents=True, exist_ok=True)
            for _, row in df_used.iterrows():
                src = Path(row["FilePath"])
                dst = target_dir / src.name
                Tools.File_Exist(src, level="error")
                Tools.Link(f"{src}", f"{dst}")
                used_files.append(str(dst))
    
        # === constant 类型 ===
        target_dir = Path(f"./DATA/const")
        target_dir.mkdir(parents=True, exist_ok=True)
        for var in const_vars:
            varname = forcinfo.get(var, "VarNameInData", fallback=var).strip()

            # 从 fileindex_df 中找出包含该变量的文件
            df_var = fileindex_df[fileindex_df["Variables"].str.contains(varname, na=False)].copy()

            if df_var.empty:
                logger.warning(f"[{var}] No files found containing variable '{varname}'.")
                continue

            src = Path(df_var.iloc[0]["FilePath"])
            dst = target_dir / f'{var}.nc'
            Tools.File_Exist(src, level="error")
            Tools.Link(f"{src}", f"{dst}")
            used_files.append(str(dst))

        used_files = sorted(set(used_files))

        # Alternating Time in namelist.wps
        start_time_str = start_time.strftime('%Y-%m-%d_%H:%M:%S')
        end_time_str = end_time.strftime('%Y-%m-%d_%H:%M:%S')
        str_use = f"start_date = '{start_time_str}',"
        cmd = f"sed -i '/start_date/c\\{str_use}' namelist.wps"
        Tools.Run_CMD(cmd, "Alternating Time in namelist.wps")
        str_use = f"end_date = '{end_time_str}', "
        cmd = f"sed -i '/end_date/c\\{str_use}' namelist.wps"
        Tools.Run_CMD(cmd, "Alternating Time in namelist.wps")
        str_use="interval_seconds = 21600,"
        cmd = f"sed -i '/interval_seconds/c\\{str_use}' namelist.wps"
        Tools.Run_CMD(cmd, "Alternating Time in namelist.wps")
        
        # ungrib 2D files
        startdate = start_time.strftime('%Y-%m-%d')
        enddate = end_time.strftime('%Y-%m-%d')
        Tools.Link('./Variable_Tables/Vtable.ECMWF', 'Vtable')
        cmd = f'rm -rf GRIBFILE.*'
        Tools.Run_CMD(cmd, "Remove old GRIBFILE")
        log_file = f'{CaseOutputPath}/{gridname}/Log/log.ungrib_2D.{time_str}'
        cmd  = f'conda run -n {cresmenv} --no-capture-output python -u ungrib_nc2im.py '
        cmd += f'--forcename MPI-ESM1-2-HR_hist --startdate {startdate} --enddate {enddate} '
        cmd += f'--inputdir ./DATA --outputdir ./ --nprocs {CWRFCoreNum} -env {ungribenv} '
        cmd += f'--forcinfopath {Forc_Info} > {log_file} 2>&1'
        Tools.Run_CMD(cmd, "Run ungrib_nc2im.py")
        # os.system(f'mv ungrib.log ungrib_2D.log')
        Tools.Check_Ungrib_Finish(os.getcwd(), prefix[0], 6, start_time, end_time)
        logger.info(f'{Consts.S4}✓  {time_str}: Ungrib 2D')
        Tools.Check_Ungrib_Finish(os.getcwd(), prefix[1], 6, start_time, end_time)
        logger.info(f'    ✓  {time_str}: Ungrib 3D')
        Tools.Check_Ungrib_Finish(os.getcwd(), prefix[2], 6, start_time, end_time)
        logger.info(f'{Consts.S4}✓  {time_str}: Ungrib SST')
        logger.info(f'{Consts.S4}✓  {time_str}: mod_lev')
    else:
        logger.info(f'{Consts.S4}==========> Skip Ungrib {time_str} <==========')

    # removing temporary files
    os.chdir(old_path)



def Ungrib_MPI_ESM1_2_HR_ssp245(casecfg, envcfg, gridname, start_time, end_time):
    old_path = os.getcwd()
    ForcingDataName = casecfg.get(gridname, 'ForcingDataName')
    ForcingDataName = ForcingDataName.strip().lower()
    CaseOutputPath = casecfg.get(gridname, 'CaseOutputPath')
    Go_Ungrib = casecfg.getboolean('PrepCWRF', 'Go_Ungrib')
    Forc_Info = envcfg.get(ForcingDataName, 'Forc_Info')
    CWRFCoreNum = casecfg.getint('PrepCWRF', 'CWRFCoreNum')
    cresmenv = envcfg.get('Environment', 'CONDA_CRESM')
    ungribenv = envcfg.get('Environment', 'CONDA_UNGRIB')

    
    # mkdir
    time_str = f'{start_time.year}-{start_time.month:02d}-{start_time.day:02d}_{end_time.year}-{end_time.month:02d}-{end_time.day:02d}'
    cmd = f'mkdir -p {CaseOutputPath}/{gridname}/PrepCWRF/Second_ICBC/UngribMetgrid_{time_str}'
    Tools.Run_CMD(cmd, "Create directory UngribMetgrid")
    cmd = f'mkdir -p {CaseOutputPath}/{gridname}/PrepCWRF/Second_ICBC/UngribMetgrid_{time_str}/DATA'
    Tools.Run_CMD(cmd, "Create directory DATA")
    
    # cd to PrepCWRF/Second_ICBC/time_str
    os.chdir(f'{CaseOutputPath}/{gridname}/PrepCWRF/Second_ICBC/UngribMetgrid_{time_str}')
    prefix = ["2D","3D","SST"]
    
    #---------------------- Ungrid ----------------------
    if Go_Ungrib:
        logger.info(f'{Consts.S4}==========> Ungrid {time_str} <==========')

        forcinfo = configparser.ConfigParser(inline_comment_prefixes=(';', '#'))
        forcinfo.read(Forc_Info)
        sections = forcinfo.sections()

        # 数据目录（若无 BaseInfo/DataDir，就用 Forc_Info 所在目录）
        if 'BaseInfo' not in sections:
            raise ValueError("Forc_Info file must contain a [BaseInfo] section with DataDir specified.")

        base_data_dir = forcinfo.get('BaseInfo', 'DataDir').strip()
        fileindex = forcinfo.get('BaseInfo', 'FileIndex').strip()
        datastart = pd.to_datetime(forcinfo.get('BaseInfo', 'DataStart').replace("_", " "), errors='raise')
        dataend   = pd.to_datetime(forcinfo.get('BaseInfo', 'DataEnd').replace("_", " "), errors='raise')
        # 统一把 start/end 转成 pandas Timestamp
        ts_start = pd.Timestamp(start_time)
        ts_end   = pd.Timestamp(end_time)

        if ts_start < pd.Timestamp(datastart) or ts_end > pd.Timestamp(dataend):
            raise ValueError(f"Requested time range {ts_start} to {ts_end} is out of bounds ({datastart} to {dataend}).")

        fileindex_path = os.path.join(base_data_dir, fileindex)
        Tools.File_Exist(fileindex_path, level='error')
        Tools.File_Exist(base_data_dir, level='error')

        # 读取文件索引
        fileindex_df = pd.read_csv(fileindex_path)
        
        # 获取 BaseInfo 中的变量分组
        var_groups = {}
        for key in ["atm3D", "atm2D", "land"]:
            if forcinfo.has_option("BaseInfo", key):
                vars_str = forcinfo.get("BaseInfo", key).strip()
                if vars_str:
                    var_groups[key] = [v.strip() for v in vars_str.split(",") if v.strip()]
        all_vars = sorted(set(sum(var_groups.values(), [])))
        if forcinfo.has_option("BaseInfo", "const"):
            const_vars = [v.strip() for v in forcinfo.get("BaseInfo", "const").strip().split(",") if v.strip()]
        else:
            const_vars = []
            logger.warning("No constant variables specified under 'const' in [BaseInfo].")


        used_files = []
        # === 遍历每个变量，筛选对应文件 ===
        for var in all_vars:
            varname = forcinfo.get(var, "VarNameInData", fallback=var).strip()
            timefreq = forcinfo.get(var, "TemporalRes", fallback="6H").strip().upper()

            # 从 fileindex_df 中找出包含该变量的文件
            pattern = fr'(?<![A-Za-z0-9_]){re.escape(varname)}(?![A-Za-z0-9_])'
            df_var = fileindex_df[fileindex_df["Variables"].str.contains(pattern, na=False, regex=True)].copy()
            # df_var = fileindex_df[fileindex_df["Variables"].str.contains(varname, na=False)].copy()

            if df_var.empty:
                logger.warning(f"[{var}] No files found containing variable '{varname}'.")
                continue

            # 解析时间列
            df_var["StartTime"] = pd.to_datetime(df_var["StartTime"], errors="coerce")
            df_var["EndTime"] = pd.to_datetime(df_var["EndTime"], errors="coerce")

            # 基于频率的时间范围过滤
            if timefreq.endswith("M"):  # 月度：精确到 YYYY-MM
                # 把数据与查询区间都转为“月”Period
                start_month = pd.Period(pd.to_datetime(ts_start), freq="M")
                end_month   = pd.Period(pd.to_datetime(ts_end),   freq="M")

                df_var["StartMonth"] = df_var["StartTime"].dt.to_period("M")
                df_var["EndMonth"]   = df_var["EndTime"].dt.to_period("M")

                mask = (df_var["EndMonth"] >= start_month) & (df_var["StartMonth"] <= end_month)
                df_used = df_var.loc[mask].dropna(subset=["FileName"])

            elif timefreq.endswith("D"):  # 日度：精确到天（可用 Period('D')，更直观）
                start_day = pd.Period(pd.to_datetime(ts_start), freq="D")
                end_day   = pd.Period(pd.to_datetime(ts_end),   freq="D")

                df_var["StartDay"] = df_var["StartTime"].dt.to_period("D")
                df_var["EndDay"]   = df_var["EndTime"].dt.to_period("D")

                mask = (df_var["EndDay"] >= start_day) & (df_var["StartDay"] <= end_day)
                df_used = df_var.loc[mask].dropna(subset=["FileName"])
            else:
                # 小时级（含 6H 等）：保持原来的时间戳比较最简单可靠
                df_used = df_var[
                    (df_var["EndTime"]   >= pd.to_datetime(ts_start)) &
                    (df_var["StartTime"] <= pd.to_datetime(ts_end))
                ].dropna(subset=["FileName"])

            if df_used.empty:
                logger.warning(f"Variable [{var}] No time-overlapping files found in range {ts_start}–{ts_end}")
                continue

            # 链接文件
            target_dir = Path(f"./DATA/{var}")
            target_dir.mkdir(parents=True, exist_ok=True)
            for _, row in df_used.iterrows():
                src = Path(row["FilePath"])
                dst = target_dir / src.name
                Tools.File_Exist(src, level="error")
                Tools.Link(f"{src}", f"{dst}")
                used_files.append(str(dst))
    
        # === constant 类型 ===
        target_dir = Path(f"./DATA/const")
        target_dir.mkdir(parents=True, exist_ok=True)
        for var in const_vars:
            varname = forcinfo.get(var, "VarNameInData", fallback=var).strip()

            # 从 fileindex_df 中找出包含该变量的文件
            df_var = fileindex_df[fileindex_df["Variables"].str.contains(varname, na=False)].copy()

            if df_var.empty:
                logger.warning(f"[{var}] No files found containing variable '{varname}'.")
                continue

            src = Path(df_var.iloc[0]["FilePath"])
            dst = target_dir / f'{var}.nc'
            Tools.File_Exist(src, level="error")
            Tools.Link(f"{src}", f"{dst}")
            used_files.append(str(dst))

        used_files = sorted(set(used_files))

        # Alternating Time in namelist.wps
        start_time_str = start_time.strftime('%Y-%m-%d_%H:%M:%S')
        end_time_str = end_time.strftime('%Y-%m-%d_%H:%M:%S')
        str_use = f"start_date = '{start_time_str}',"
        cmd = f"sed -i '/start_date/c\\{str_use}' namelist.wps"
        Tools.Run_CMD(cmd, "Alternating Time in namelist.wps")
        str_use = f"end_date = '{end_time_str}', "
        cmd = f"sed -i '/end_date/c\\{str_use}' namelist.wps"
        Tools.Run_CMD(cmd, "Alternating Time in namelist.wps")
        str_use="interval_seconds = 21600,"
        cmd = f"sed -i '/interval_seconds/c\\{str_use}' namelist.wps"
        Tools.Run_CMD(cmd, "Alternating Time in namelist.wps")
        
        # ungrib 2D files
        startdate = start_time.strftime('%Y-%m-%d')
        enddate = end_time.strftime('%Y-%m-%d')
        Tools.Link('./Variable_Tables/Vtable.ECMWF', 'Vtable')
        cmd = f'rm -rf GRIBFILE.*'
        Tools.Run_CMD(cmd, "Remove old GRIBFILE")
        log_file = f'{CaseOutputPath}/{gridname}/Log/log.ungrib_2D.{time_str}'
        cmd  = f'conda run -n {cresmenv} --no-capture-output python -u ungrib_nc2im.py '
        cmd += f'--forcename MPI-ESM1-2-HR_ssp245 --startdate {startdate} --enddate {enddate} '
        cmd += f'--inputdir ./DATA --outputdir ./ --nprocs {CWRFCoreNum} -env {ungribenv} '
        cmd += f'--forcinfopath {Forc_Info} > {log_file} 2>&1'
        Tools.Run_CMD(cmd, "Run ungrib_nc2im.py")
        # os.system(f'mv ungrib.log ungrib_2D.log')
        Tools.Check_Ungrib_Finish(os.getcwd(), prefix[0], 6, start_time, end_time)
        logger.info(f'{Consts.S4}✓  {time_str}: Ungrib 2D')
        Tools.Check_Ungrib_Finish(os.getcwd(), prefix[1], 6, start_time, end_time)
        logger.info(f'    ✓  {time_str}: Ungrib 3D')
        Tools.Check_Ungrib_Finish(os.getcwd(), prefix[2], 6, start_time, end_time)
        logger.info(f'{Consts.S4}✓  {time_str}: Ungrib SST')
        
        logger.info(f'{Consts.S4}✓  {time_str}: mod_lev')
    else:
        logger.info(f'{Consts.S4}==========> Skip Ungrib {time_str} <==========')

    # removing temporary files
    os.chdir(old_path)



def Ungrib_MPI_ESM1_2_HR_ssp585(casecfg, envcfg, gridname, start_time, end_time):
    old_path = os.getcwd()
    ForcingDataName = casecfg.get(gridname, 'ForcingDataName')
    ForcingDataName = ForcingDataName.strip().lower()
    CaseOutputPath = casecfg.get(gridname, 'CaseOutputPath')
    Go_Ungrib = casecfg.getboolean('PrepCWRF', 'Go_Ungrib')
    Forc_Info = envcfg.get(ForcingDataName, 'Forc_Info')
    CWRFCoreNum = casecfg.getint('PrepCWRF', 'CWRFCoreNum')
    cresmenv = envcfg.get('Environment', 'CONDA_CRESM')
    ungribenv = envcfg.get('Environment', 'CONDA_UNGRIB')

    
    # mkdir
    time_str = f'{start_time.year}-{start_time.month:02d}-{start_time.day:02d}_{end_time.year}-{end_time.month:02d}-{end_time.day:02d}'
    cmd = f'mkdir -p {CaseOutputPath}/{gridname}/PrepCWRF/Second_ICBC/UngribMetgrid_{time_str}'
    Tools.Run_CMD(cmd, "Create directory UngribMetgrid")
    cmd = f'mkdir -p {CaseOutputPath}/{gridname}/PrepCWRF/Second_ICBC/UngribMetgrid_{time_str}/DATA'
    Tools.Run_CMD(cmd, "Create directory DATA")
    
    # cd to PrepCWRF/Second_ICBC/time_str
    os.chdir(f'{CaseOutputPath}/{gridname}/PrepCWRF/Second_ICBC/UngribMetgrid_{time_str}')
    prefix = ["2D","3D","SST"]
    
    #---------------------- Ungrid ----------------------
    if Go_Ungrib:
        logger.info(f'{Consts.S4}==========> Ungrid {time_str} <==========')

        forcinfo = configparser.ConfigParser(inline_comment_prefixes=(';', '#'))
        forcinfo.read(Forc_Info)
        sections = forcinfo.sections()

        # 数据目录（若无 BaseInfo/DataDir，就用 Forc_Info 所在目录）
        if 'BaseInfo' not in sections:
            raise ValueError("Forc_Info file must contain a [BaseInfo] section with DataDir specified.")

        base_data_dir = forcinfo.get('BaseInfo', 'DataDir').strip()
        fileindex = forcinfo.get('BaseInfo', 'FileIndex').strip()
        datastart = pd.to_datetime(forcinfo.get('BaseInfo', 'DataStart').replace("_", " "), errors='raise')
        dataend   = pd.to_datetime(forcinfo.get('BaseInfo', 'DataEnd').replace("_", " "), errors='raise')
        # 统一把 start/end 转成 pandas Timestamp
        ts_start = pd.Timestamp(start_time)
        ts_end   = pd.Timestamp(end_time)

        if ts_start < pd.Timestamp(datastart) or ts_end > pd.Timestamp(dataend):
            raise ValueError(f"Requested time range {ts_start} to {ts_end} is out of bounds ({datastart} to {dataend}).")

        fileindex_path = os.path.join(base_data_dir, fileindex)
        Tools.File_Exist(fileindex_path, level='error')
        Tools.File_Exist(base_data_dir, level='error')

        # 读取文件索引
        fileindex_df = pd.read_csv(fileindex_path)
        
        # 获取 BaseInfo 中的变量分组
        var_groups = {}
        for key in ["atm3D", "atm2D", "land"]:
            if forcinfo.has_option("BaseInfo", key):
                vars_str = forcinfo.get("BaseInfo", key).strip()
                if vars_str:
                    var_groups[key] = [v.strip() for v in vars_str.split(",") if v.strip()]
        all_vars = sorted(set(sum(var_groups.values(), [])))
        if forcinfo.has_option("BaseInfo", "const"):
            const_vars = [v.strip() for v in forcinfo.get("BaseInfo", "const").strip().split(",") if v.strip()]
        else:
            const_vars = []
            logger.warning("No constant variables specified under 'const' in [BaseInfo].")


        used_files = []
        # === 遍历每个变量，筛选对应文件 ===
        for var in all_vars:
            varname = forcinfo.get(var, "VarNameInData", fallback=var).strip()
            timefreq = forcinfo.get(var, "TemporalRes", fallback="6H").strip().upper()

            # 从 fileindex_df 中找出包含该变量的文件
            pattern = fr'(?<![A-Za-z0-9_]){re.escape(varname)}(?![A-Za-z0-9_])'
            df_var = fileindex_df[fileindex_df["Variables"].str.contains(pattern, na=False, regex=True)].copy()
            # df_var = fileindex_df[fileindex_df["Variables"].str.contains(varname, na=False)].copy()

            if df_var.empty:
                logger.warning(f"[{var}] No files found containing variable '{varname}'.")
                continue

            # 解析时间列
            df_var["StartTime"] = pd.to_datetime(df_var["StartTime"], errors="coerce")
            df_var["EndTime"] = pd.to_datetime(df_var["EndTime"], errors="coerce")

            # 基于频率的时间范围过滤
            if timefreq.endswith("M"):  # 月度：精确到 YYYY-MM
                # 把数据与查询区间都转为“月”Period
                start_month = pd.Period(pd.to_datetime(ts_start), freq="M")
                end_month   = pd.Period(pd.to_datetime(ts_end),   freq="M")

                df_var["StartMonth"] = df_var["StartTime"].dt.to_period("M")
                df_var["EndMonth"]   = df_var["EndTime"].dt.to_period("M")

                mask = (df_var["EndMonth"] >= start_month) & (df_var["StartMonth"] <= end_month)
                df_used = df_var.loc[mask].dropna(subset=["FileName"])

            elif timefreq.endswith("D"):  # 日度：精确到天（可用 Period('D')，更直观）
                start_day = pd.Period(pd.to_datetime(ts_start), freq="D")
                end_day   = pd.Period(pd.to_datetime(ts_end),   freq="D")

                df_var["StartDay"] = df_var["StartTime"].dt.to_period("D")
                df_var["EndDay"]   = df_var["EndTime"].dt.to_period("D")

                mask = (df_var["EndDay"] >= start_day) & (df_var["StartDay"] <= end_day)
                df_used = df_var.loc[mask].dropna(subset=["FileName"])
            else:
                # 小时级（含 6H 等）：保持原来的时间戳比较最简单可靠
                df_used = df_var[
                    (df_var["EndTime"]   >= pd.to_datetime(ts_start)) &
                    (df_var["StartTime"] <= pd.to_datetime(ts_end))
                ].dropna(subset=["FileName"])

            if df_used.empty:
                logger.warning(f"Variable [{var}] No time-overlapping files found in range {ts_start}–{ts_end}")
                continue

            # 链接文件
            target_dir = Path(f"./DATA/{var}")
            target_dir.mkdir(parents=True, exist_ok=True)
            for _, row in df_used.iterrows():
                src = Path(row["FilePath"])
                dst = target_dir / src.name
                Tools.File_Exist(src, level="error")
                Tools.Link(f"{src}", f"{dst}")
                used_files.append(str(dst))
    
        # === constant 类型 ===
        target_dir = Path(f"./DATA/const")
        target_dir.mkdir(parents=True, exist_ok=True)
        for var in const_vars:
            varname = forcinfo.get(var, "VarNameInData", fallback=var).strip()

            # 从 fileindex_df 中找出包含该变量的文件
            df_var = fileindex_df[fileindex_df["Variables"].str.contains(varname, na=False)].copy()

            if df_var.empty:
                logger.warning(f"[{var}] No files found containing variable '{varname}'.")
                continue

            src = Path(df_var.iloc[0]["FilePath"])
            dst = target_dir / f'{var}.nc'
            Tools.File_Exist(src, level="error")
            Tools.Link(f"{src}", f"{dst}")
            used_files.append(str(dst))

        used_files = sorted(set(used_files))

        # Alternating Time in namelist.wps
        start_time_str = start_time.strftime('%Y-%m-%d_%H:%M:%S')
        end_time_str = end_time.strftime('%Y-%m-%d_%H:%M:%S')
        str_use = f"start_date = '{start_time_str}',"
        cmd = f"sed -i '/start_date/c\\{str_use}' namelist.wps"
        Tools.Run_CMD(cmd, "Alternating Time in namelist.wps")
        str_use = f"end_date = '{end_time_str}', "
        cmd = f"sed -i '/end_date/c\\{str_use}' namelist.wps"
        Tools.Run_CMD(cmd, "Alternating Time in namelist.wps")
        str_use="interval_seconds = 21600,"
        cmd = f"sed -i '/interval_seconds/c\\{str_use}' namelist.wps"
        Tools.Run_CMD(cmd, "Alternating Time in namelist.wps")
        
        # ungrib 2D files
        startdate = start_time.strftime('%Y-%m-%d')
        enddate = end_time.strftime('%Y-%m-%d')
        Tools.Link('./Variable_Tables/Vtable.ECMWF', 'Vtable')
        cmd = f'rm -rf GRIBFILE.*'
        Tools.Run_CMD(cmd, "Remove old GRIBFILE")
        log_file = f'{CaseOutputPath}/{gridname}/Log/log.ungrib_2D.{time_str}'
        cmd  = f'conda run -n {cresmenv} --no-capture-output python -u ungrib_nc2im.py '
        cmd += f'--forcename MPI-ESM1-2-HR_ssp585 --startdate {startdate} --enddate {enddate} '
        cmd += f'--inputdir ./DATA --outputdir ./ --nprocs {CWRFCoreNum} -env {ungribenv} '
        cmd += f'--forcinfopath {Forc_Info} > {log_file} 2>&1'
        Tools.Run_CMD(cmd, "Run ungrib_nc2im.py")
        # os.system(f'mv ungrib.log ungrib_2D.log')
        Tools.Check_Ungrib_Finish(os.getcwd(), prefix[0], 6, start_time, end_time)
        logger.info(f'{Consts.S4}✓  {time_str}: Ungrib 2D')
        Tools.Check_Ungrib_Finish(os.getcwd(), prefix[1], 6, start_time, end_time)
        logger.info(f'    ✓  {time_str}: Ungrib 3D')
        Tools.Check_Ungrib_Finish(os.getcwd(), prefix[2], 6, start_time, end_time)
        logger.info(f'{Consts.S4}✓  {time_str}: Ungrib SST')
        
        logger.info(f'{Consts.S4}✓  {time_str}: mod_lev')
    else:
        logger.info(f'{Consts.S4}==========> Skip Ungrib {time_str} <==========')

    # removing temporary files
    os.chdir(old_path)



def Ungrib(casecfg, envcfg, gridname, start_time, end_time):
    """
    Ungrib CFSV2 data
    :param config: Configuration object
    :param gridname: Case name
    :param start_time: Start time of the period
    :param end_time: End time of the period
    """
    ForcingDataName = casecfg.get(gridname, 'ForcingDataName')
    ForcingDataName = ForcingDataName.strip().lower()
    if ForcingDataName == 'cfsv2':
        # Call the CFSV2 Ungrib function
        Ungrib_CFSV2(casecfg, envcfg, gridname, start_time, end_time)
    elif ForcingDataName == 'era5': 
        # Call the ERA5 Ungrib function
        Ungrib_ERA5(casecfg, envcfg, gridname, start_time, end_time)
    elif ForcingDataName == 'mpi-esm1-2-hr_hist':
        # Call the MPI-ESM1-2-HR_hist Ungrib function
        Ungrib_MPI_ESM1_2_HR_hist(casecfg, envcfg, gridname, start_time, end_time)
    elif ForcingDataName == 'mpi-esm1-2-hr_ssp245':
        # Call the MPI-ESM1-2-HR_ssp245 Ungrib function
        Ungrib_MPI_ESM1_2_HR_ssp245(casecfg, envcfg, gridname, start_time, end_time)
    elif ForcingDataName == 'mpi-esm1-2-hr_ssp585':
        # Call the MPI-ESM1-2-HR_ssp585 Ungrib function
        Ungrib_MPI_ESM1_2_HR_ssp585(casecfg, envcfg, gridname, start_time, end_time)
    else:
        logger.error(f"Unsupported ForcingDataName: {ForcingDataName}.")
        logger.error("Supported driver data is : 'CFSV2' or 'ERA5' or 'MPI-ESM1-2-HR_hist' or 'MPI-ESM1-2-HR_ssp245' or 'MPI-ESM1-2-HR_ssp585'.")
        raise ValueError(f"Unsupported ForcingDataName: {ForcingDataName}. Please use 'CFSV2' or 'ERA5' or 'MPI-ESM1-2-HR_hist' or 'MPI-ESM1-2-HR_ssp245' or 'MPI-ESM1-2-HR_ssp585'.")


def Metgrid(casecfg, envcfg, gridname, start_time, end_time):
    old_path = os.getcwd()
    CaseOutputPath = casecfg.get(gridname, 'CaseOutputPath')
    Go_Metgrid = casecfg.getboolean('PrepCWRF', 'Go_Metgrid')
    ForcingDataName = casecfg.get(gridname, 'ForcingDataName')
    ForcingDataName = ForcingDataName.strip().lower()
    Forc_Info = envcfg.get(ForcingDataName, 'Forc_Info')
    CWRFCoreNum = casecfg.getint('PrepCWRF', 'CWRFCoreNum')
    SYS_CWRF = envcfg.get('Environment', 'SYS_CWRF')

    prefix = ["2D","3D","SST"]
    
    time_str = f'{start_time.year}-{start_time.month:02d}-{start_time.day:02d}_{end_time.year}-{end_time.month:02d}-{end_time.day:02d}'
    
    # cd to PrepCWRF/Second_ICBC/time_str
    os.chdir(f'{CaseOutputPath}/{gridname}/PrepCWRF/Second_ICBC/UngribMetgrid_{time_str}')
    
    #---------------------- Metgrid ----------------------
    if Go_Metgrid:
        # metgrid
        logger.info(f'{Consts.S4}==========> Metgrid {time_str} <==========')
        st = time.time()
        
        # Alternating Time in namelist.wps
        start_time_str = start_time.strftime('%Y-%m-%d_%H:%M:%S')
        end_time_str = end_time.strftime('%Y-%m-%d_%H:%M:%S')
        str_use = f"start_date = '{start_time_str}',"
        cmd = f"sed -i '/start_date/c\\{str_use}' namelist.wps"
        Tools.Run_CMD(cmd, "Alternating Time in namelist.wps")
        str_use = f"end_date = '{end_time_str}', "
        cmd = f"sed -i '/end_date/c\\{str_use}' namelist.wps"
        Tools.Run_CMD(cmd, "Alternating Time in namelist.wps")
        str_use="interval_seconds = 21600,"
        cmd = f"sed -i '/interval_seconds/c\\{str_use}' namelist.wps"
        Tools.Run_CMD(cmd, "Alternating Time in namelist.wps")
        str_use="fg_name = \""+prefix[0]+"\", \""+prefix[1]+"\", \""+prefix[2]+"\", "
        Tools.Run_CMD(f"sed -i '/fg_name/c\\{str_use}' namelist.wps")
        str_use="constants_name= \"soilhgt\","
        Tools.Run_CMD(f"sed -i '/constants_name/c\\{str_use}' namelist.wps")
        log_file = f'{CaseOutputPath}/{gridname}/Log/log.metgrid.{time_str}'
        if CWRFCoreNum == 1:
            cmd = f"./metgrid.exe > {log_file} 2>&1"
        elif CWRFCoreNum > 1:
            cmd = f"mpirun -n {CWRFCoreNum} ./metgrid.exe > {log_file} 2>&1"
        Tools.Run_CMD(cmd, "Run metgrid.exe", env=SYS_CWRF)                           # metgrid.exe can use mpirun when handling large size data
        Tools.Check_Metgrid_Finish(os.getcwd(), 'met_em.d01', 6, start_time, end_time)
        logger.info(f'{Consts.S4}✓  {time_str}: Metgrid ')

        # SST Processing
        fs = glob.glob(f'./met_em*d01*')
        timelist = pd.date_range(start=start_time, end=end_time, freq='D')
        for iday in range(len(timelist)):
            metfiles = glob.glob(f'./met_em.d01.{timelist[iday].strftime("%Y-%m-%d")}*.nc')
            Tools.File_Exist(metfiles, level='error')
            YYYYMMDD = timelist[iday].strftime('%Y%m%d')
            cmd = f"./sst_avg_d01 {YYYYMMDD}"
            Tools.Run_CMD(cmd, "Run sst_avg_d01", env=SYS_CWRF)
            
        logger.info(f'{Consts.S4}✓  {time_str}: SST avg')
        
    else:
        logger.info(f'{Consts.S4}==========> Skip Metgrid {time_str} <==========')
    
    # removing temporary files
    os.chdir(old_path)
    
    

def Real(casecfg, envcfg, gridname, timelist):
    old_path = os.getcwd()
    CaseOutputPath = casecfg.get(gridname, 'CaseOutputPath')
    CWRFCoreNum = casecfg.getint('PrepCWRF', 'CWRFCoreNum')
    Go_Real = casecfg.getboolean('PrepCWRF', 'Go_Real')
    Go_VBS = casecfg.getboolean('PrepCWRF', 'Go_VBS')
    CWRFToolPath = envcfg.get('Paths', 'CWRFToolPath')
    SYS_CWRF = envcfg.get('Environment', 'SYS_CWRF')

    CWRFNMLPath = f'{CaseOutputPath}/{gridname}/NMLS/namelist.cwrf.{gridname}'

    # mkdir
    cmd = f'mkdir -p {CaseOutputPath}/{gridname}/PrepCWRF/Second_ICBC/Real'
    Tools.Run_CMD(cmd, "Create directory Real")
    
    # cd to PrepCWRF/Second_ICBC/Real
    os.chdir(f'{CaseOutputPath}/{gridname}/PrepCWRF/Second_ICBC/Real')
    #---------------------- Real ----------------------
    if Go_Real:
        st = time.time()
        logger.info(f'{Consts.S4}==========> Real <==========')
        
        # link needed CWPS files into temporary dictionary
        Tools.Link(f'{CWRFToolPath}/executable/*', '.')
        Tools.Link(f"{CaseOutputPath}/{gridname}/PrepCWRF/First_StaticData/Geog_{gridname}/geo_em.d01_veg.nc", "./geo_em.d01.nc")
        Tools.Copy(CWRFNMLPath, "./namelist.input")
        
        # link met_em files into Real path
        for start_time, end_time in timelist:
            time_str = f'{start_time.year}-{start_time.month:02d}-{start_time.day:02d}_{end_time.year}-{end_time.month:02d}-{end_time.day:02d}'
            Tools.Link(f'{CaseOutputPath}/{gridname}/PrepCWRF/Second_ICBC/UngribMetgrid_{time_str}/met_em.d01.*', '.')

        log_file = f'{CaseOutputPath}/{gridname}/Log/log.real'
        if CWRFCoreNum == 1:
            cmd = f"./real.exe > {log_file} 2>&1"
        elif CWRFCoreNum > 1:
            cmd = f"mpirun -n {CWRFCoreNum} ./real.exe > {log_file} 2>&1"
        Tools.Run_CMD(cmd, "Run real.exe", env=SYS_CWRF)                           # real.exe can use mpirun when handling large size data
        logger.info(f'{Consts.S4}✓  Real')
    else:
        logger.info(f'{Consts.S4}==========> Skip Real <==========')

    if Go_VBS:
        #---------------------- Vbs ----------------------
        logger.info(f'{Consts.S4}==========> Vbs <==========')
        start_time, _ = timelist[0]
        _, end_time = timelist[-1]
        end_time = end_time + timedelta(days=2)
        cmd = f'echo "{start_time.year} {start_time.month:02d} {start_time.day:02d}" > vbs.input'
        Tools.Run_CMD(cmd, "Write vbs.input")
        cmd = f'echo "{end_time.year} {end_time.month:02d} {end_time.day:02d}" >> vbs.input'
        Tools.Run_CMD(cmd, "Write vbs.input")
        cmd = "rm -f sbcs"
        Tools.Run_CMD(cmd, "Remove old sbcs")
        Tools.Link(f'{CaseOutputPath}/{gridname}/PrepCWRF/First_StaticData/Geog_{gridname}/sbcs', './sbcs')
        log_file = f'{CaseOutputPath}/{gridname}/Log/log.vbs'
        cmd = f"./vbs.re_d01.exe  < vbs.input > {log_file} 2>&1"        
        Tools.Run_CMD(cmd, "Run vbs.re_d01.exe", env=SYS_CWRF)
        logger.info(f'{Consts.S4}✓  {time_str}: Vbs')
    else:
        logger.info(f'{Consts.S4}==========> Skip Vbs <==========')
    
    os.chdir(old_path)
    
    

def Link_CWPS_Files(casecfg, envcfg, gridname, start_time, end_time):
    """
    Link ICBC files
    :param config: Configuration object
    :param gridname: Case name
    :param start_time: Start time of the period
    :param end_time: End time of the period
    :param ForcingDataName: Name of the forcing data (CFSV2 or ERA5)
    """
    old_path = os.getcwd()
    ForcingDataName = casecfg.get(gridname, 'ForcingDataName')
    CaseOutputPath = casecfg.get(gridname, 'CaseOutputPath')
    CWPSPath = envcfg.get('Paths', 'CWPSPath')
    CWRFToolPath = envcfg.get('Paths', 'CWRFToolPath')
    CWPSStaticPath = envcfg.get('Paths', 'CWPSStaticPath')
    WMEJUngrib = envcfg.get('Paths', 'WMEJUngrib')
    CWPSNMLPath = f'{CaseOutputPath}/{gridname}/NMLS/namelist.cwps.{gridname}'
    CWRFNMLPath = f'{CaseOutputPath}/{gridname}/NMLS/namelist.cwrf.{gridname}'
    ForcingDataName = ForcingDataName.strip().lower()
    time_str = f'{start_time.year}-{start_time.month:02d}-{start_time.day:02d}_{end_time.year}-{end_time.month:02d}-{end_time.day:02d}'
    # mkdir
    cmd = f'mkdir -p {CaseOutputPath}/{gridname}/PrepCWRF/Second_ICBC/UngribMetgrid_{time_str}'
    Tools.Run_CMD(cmd, f"Create directory UngribMetgrid_{time_str}")
    
    # cd to PrepCWRF/Second_ICBC/time_str
    os.chdir(f'{CaseOutputPath}/{gridname}/PrepCWRF/Second_ICBC/UngribMetgrid_{time_str}')

    # link needed CWPS files into temporary dictionary
    Tools.Link(f"{CWRFToolPath}/executable/*", ".")
    Tools.Link(f"{CWPSPath}/ungrib/Variable_Tables", ".")
    Tools.Link(f"{CWPSStaticPath}/METGRID.TBL.sq", "METGRID.TBL")
    Tools.Link(f"{CaseOutputPath}/{gridname}/PrepCWRF/First_StaticData/Geog_{gridname}/geo_em.d01_veg.nc", "./geo_em.d01.nc")
    Tools.Link(f"{CWPSPath}/link_grib.csh", ".")
    Tools.Copy(CWPSNMLPath, "./namelist.wps")
    Tools.Copy(CWRFNMLPath, "./namelist.input")

    # link soilhgt files    
    if ForcingDataName == 'cfsv2':
        Tools.Link(f"{CWPSStaticPath}/soilhgt_cfsrr.cs", "./soilhgt")
    elif ForcingDataName == 'era5':
        Tools.Link(f"{CWPSStaticPath}/SOILHGT_era5", "./soilhgt")
    elif ForcingDataName == 'mpi-esm1-2-hr_hist':
        Tools.Link(f"{CWPSStaticPath}/soilhgt.MPI-ESM1-2-HR.WMEJ", "./soilhgt")
        Tools.Link(f'{WMEJUngrib}/*', '.')
    elif ForcingDataName == 'mpi-esm1-2-hr_ssp245':
        Tools.Link(f"{CWPSStaticPath}/soilhgt.MPI-ESM1-2-HR.WMEJ", "./soilhgt")
        Tools.Link(f'{WMEJUngrib}/*', '.')
    elif ForcingDataName == 'mpi-esm1-2-hr_ssp585':
        Tools.Link(f"{CWPSStaticPath}/soilhgt.MPI-ESM1-2-HR.WMEJ", "./soilhgt")
        Tools.Link(f'{WMEJUngrib}/*', '.')
    else:
        logger.error(f"Unsupported ForcingDataName: {ForcingDataName}.")
        logger.error("Supported driver data is :")
        logger.error(f"{Consts.S8}CFSV2")
        logger.error(f"{Consts.S8}ERA5")
        logger.error(f"{Consts.S8}MPI-ESM1-2-HR_hist")
        logger.error(f"{Consts.S8}MPI-ESM1-2-HR_ssp245")
        logger.error(f"{Consts.S8}MPI-ESM1-2-HR_ssp585")
        raise ValueError(f"Unsupported ForcingDataName: {ForcingDataName}. ")

    os.chdir(old_path)
    
        
        
