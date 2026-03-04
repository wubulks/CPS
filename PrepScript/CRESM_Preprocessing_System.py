#! /stu01/wumej22/Anaconda3/bin/python
# -*- coding: utf-8 -*-

"""
===============================================================================
Script Name   : CRESM Preprocessing System (CPS)
Description   : Automation script for preparing initial and boundary conditions 
                (IC/BC) and static data for the CRESM coupled model system.
                
                Modules included:
                - PrepCWRF    : Geogrid, Ungrib, Metgrid, Real, etc.
                - PrepCoLM    : Mesh generation, Surface data, Initial data.
                - PrepCRESM   : Coupler input generation.
                - Utils.Consts: Global constants.
                - Utils.Tools : Common utility functions.
                - Utils.ICBC  : IC/BC data processing functions.
                - Utils.Logger: Adaptive logging setup.  

Author        : Omarjan
Institution   : School of Atmospheric Sciences, Sun Yat-sen University (SYSU)

Created       : 2025-05-25
Last Modified : 2026-01-21
Version       : 1.2.0

Conda Environments Required:
    - cresm     : /home/wumej22/anaconda3/envs/cresm
    - xesmf     : /home/wumej22/anaconda3/envs/xesmf
    - Chaomodis : /home/wumej22/anaconda3/envs/Chaomodis

Usage:
    python CRESM_Preprocessing_System.py -n [GridName] [Options]
    python CRESM_Preprocessing_System.py --help
===============================================================================
"""

import os
import sys
import time
import glob
import math
import shlex
import logging
import argparse
import subprocess
import configparser
import pandas as pd
import numpy as np
import multiprocessing
from datetime import datetime, timedelta
from Utils import Tools, Consts
from Utils.Logger import Setup_Logger
import PrepCWRF
import PrepCoLM
import PrepCRESM

logger = logging.getLogger("CRESMPrep." + __name__)

# =========> Functions <==========
def Read_Config(filepath):
    config = configparser.ConfigParser(
        interpolation=configparser.ExtendedInterpolation()
    )
    config.read(filepath)
    return config



def Get_Useful_Cases(casecfg):
    """ 
    Get the useful case names from the configuration file
    """
    sections = casecfg.sections()
    sections.remove('BaseInfo')
    sections.remove('PrepCoLM')
    sections.remove('PrepCWRF')
    sections.remove('PrepCRESM')
    sections.remove('GatherData')
    usefulecases = sorted(sections)
    return usefulecases



def Print_Useful_Cases(casecfg):
    """
    Print the case name in the configuration file
    """
    print("\n  Useful Case Name:\n")
    usefulecases = Get_Useful_Cases(casecfg)
    sectionslist = []
    for i, section in enumerate(usefulecases):
        print(f"{Consts.S4}[-] {section}\n")
        sectionslist.append(section)
    return sectionslist



def Modify_Config(casecfg, gridname, year=None):
    if year is None:
        return casecfg, gridname
        
    if (year is not None):
        # Modify the year in the case
        if year < 1900 or year > 2200:
            print("Please provide a valid year between 1900 and 2200")
            raise ValueError("year must be between 1900 and 2200")
        old_section = gridname
        new_section = f"{gridname}.{year}"
        print(f"")
        print(f"{Consts.S4}Modifying case section: {old_section} to {new_section}")
        casecfg.add_section(new_section)
        for key, val in casecfg.items(old_section):
            casecfg.set(new_section, key, val)
    # Check Time
    start_time_str = casecfg.get(gridname, 'StartTime')
    end_time_str = casecfg.get(gridname, 'EndTime')
    try:
        start_time = datetime.strptime(start_time_str, '%Y-%m-%d_%H:%M:%S')
        end_time = datetime.strptime(end_time_str, '%Y-%m-%d_%H:%M:%S')
    except ValueError:
        print(f"{Consts.S4}Time format error: {start_time_str}, {end_time_str}")
        print(f"{Consts.S4}Time format must be like: 2021-01-01_00:00:00")
        raise ValueError("Time format error")
    if start_time >= end_time:
        print(f"{Consts.S4}StartTime must be less than EndTime.")
        raise ValueError(f"StartTime must be earlier than EndTime: {start_time_str} >= {end_time_str}")
    
    key = 'Time'
    formatted_key = key.ljust(17)
    print(f"{Consts.S6}{formatted_key}: {start_time_str} - {end_time_str}\n")
    new_start_time = start_time.replace(year=year-1, month=12, day=1, hour=0, minute=0, second=0)
    new_end_time = end_time.replace(year=year+1, month=1, day=3, hour=0, minute=0, second=0)
    casecfg.set(new_section, 'StartTime', new_start_time.strftime('%Y-%m-%d_%H:%M:%S'))
    casecfg.set(new_section, 'EndTime', new_end_time.strftime('%Y-%m-%d_%H:%M:%S'))
    gridname = new_section
    return casecfg, gridname



def Check_AllConfig(case_cfg, env_cfg, gridname, level='INFO'):
    """
    统一配置检查函数 (Unified Configuration Check)
    """
    # =========================================================
    # 0. 初始化日志等级
    # =========================================================
    # 将字符串等级转换为 logging 常量
    if isinstance(level, str):
        level_name = level.upper()
        numeric_level = getattr(logging, level_name, logging.INFO)
    else:
        numeric_level = level

    # 设置 Root Logger
    root_logger = logging.getLogger()
    root_logger.setLevel(numeric_level)
    
    # 如果没有 Handler (防止没有任何输出)，添加一个默认的
    if not root_logger.handlers:
        fmt_normal = "%(asctime)s | %(levelname)-8s|  %(message)s"
        datefmt = "%m-%d %H:%M:%S"
        logging.basicConfig(level=numeric_level, format=fmt_normal, datefmt=datefmt)

    logging.info(f'==========> Check Configuration <==========')
    logging.debug(f"{Consts.S4}[Debug Mode] Level set to: {logging.getLevelName(numeric_level)}")

    # 用于收集错误，不立即退出
    ERROR_LIST = []

    def _error(msg):
        """记录错误"""
        logging.error(f"{Consts.S4}[X] {msg}")
        ERROR_LIST.append(msg)

    def _ok(msg, is_detail=False):
        """记录正常信息 (Detail 仅在 DEBUG 模式显示)"""
        if is_detail:
            logging.debug(f"{Consts.S4}{msg}")
        else:
            logging.info(f"{Consts.S4}{msg}")

    # =========================================================
    # 1. 定义检查清单 (在此处修改规则)
    # =========================================================

    # [A] 必须存在的 Section
    MUST_SECTIONS = ['BaseInfo', 'PrepCWRF', 'PrepCoLM', 'PrepCRESM', 'GatherData', gridname]

    # [B] 布尔值检查列表 (Section, Key)
    # 这些 Key 如果存在，必须是 True/False。如果业务要求必须存在，代码逻辑里会自动处理缺失情况。
    BOOL_CHECKS = [
        # PrepCWRF
        ('PrepCWRF', 'Go_Geogrid'), ('PrepCWRF', 'Go_Ungrib'), ('PrepCWRF', 'Go_Metgrid'),
        ('PrepCWRF', 'Go_Real'),    ('PrepCWRF', 'Go_VBS'),    ('PrepCWRF', 'Copy_CWRF_Output'),
        # PrepCoLM
        ('PrepCoLM', 'Go_MeshGrid'), ('PrepCoLM', 'Go_MakeSrf'), ('PrepCoLM', 'Go_MakeIni'),
        ('PrepCoLM', 'Go_CoLMTempRun'), ('PrepCoLM', 'Go_Remap'), ('PrepCoLM', 'Copy_CoLM_Output'),
        # BaseInfo & Others
        ('BaseInfo', 'CleanTempFiles'), ('BaseInfo', 'Enable_TimeChunk'),
        ('GatherData', 'Collect_CWRF_Output'), ('GatherData', 'Collect_CoLM_Output')
    ]

    # [C] 数值与逻辑检查列表 (Section, Key, ValidatorLambda, ErrorMsg)
    # 使用 lambda 表达式灵活定义规则
    VALUE_CHECKS = [
        (gridname, 'EdgeNum_WE', lambda x: x > 0, "Must be > 0"),
        (gridname, 'EdgeNum_SN', lambda x: x > 0, "Must be > 0"),
        (gridname, 'dx_WE',      lambda x: x > 0, "Must be > 0"),
        (gridname, 'dy_SN',      lambda x: x > 0, "Must be > 0"),
        (gridname, 'MeshSize',   lambda x: 1 <= x <= 3, "Must be between 1 and 3"),
        (gridname, 'RefLat',     lambda x: -90 <= x <= 90, "Must be -90 to 90"),
        # 你的特殊逻辑：BdyWidth 必须是奇数且 >= 13
        (gridname, 'BdyWidth',   lambda x: x >= 13 and x % 2 != 0, "Must be ODD and >= 13"),
        (gridname, 'LakeThreshold', lambda x: 0.0 <= x <= 1.0, "Must be 0.0 to 1.0"),
        (gridname, 'TimeChunkCount', lambda x: x > 0, "Must be > 0 (if used)"), 
    ]

    # [D] 必须存在的 Env 路径 Key (Env Config [Paths])
    PATH_CHECKS = [
        'ScriptPath', 'CoLMModelPath', 'CoLMRawDataPath', 'CoLMRunDataPath', 
        'CoLMForcingPath', 'RootToolBox', 'CWPSPath', 'CWRFToolPath', 
        'GeogDataPath', 'CWPSStaticPath', 'GlobalLakeDepth', 'GlobalLakeStatus',
        'NCOPath', 'CDOPath', 'NCLPath'
    ]
    ENV_CHECKS = ['SYS_CWRF', 'SYS_CoLM', 
        'CONDA_CRESM', 'CONDA_XESMF', 'CONDA_CHAO', 'CONDA_UNGRIB']

    # =========================================================
    # 2. 执行检查逻辑
    # =========================================================

    # --- 2.1 基础结构检查 ---
    for sec in MUST_SECTIONS:
        if not case_cfg.has_section(sec):
            _error(f"Missing Case Section: [{sec}]")
    
    if not env_cfg.has_section("Paths"):
        _error("Missing Env Section: [Paths]")

    # 如果连基础 Section 都缺，后续 get 会报错，先终止
    if ERROR_LIST:
        logging.critical(f"{Consts.S4}Critical structure missing. Aborting.")
        sys.exit(1)

    # --- 2.2 布尔值检查 ---
    for sec, key in BOOL_CHECKS:
        if case_cfg.has_option(sec, key):
            try:
                val = case_cfg.getboolean(sec, key)
                # 布尔值通常不需要刷屏，设为 Detail (DEBUG模式可见)
                _ok(f"{key.ljust(25)}: {val}", is_detail=True)
            except ValueError:
                _error(f"[{sec}] {key} must be boolean (True/False)")

    # --- 2.3 数值逻辑检查 ---
    for sec, key, validator, rule_desc in VALUE_CHECKS:
        if not case_cfg.has_option(sec, key):
            # 对于部分可选参数(如 TimeChunkCount)，如果不启用可能不存在，这里选择跳过
            # 如果是必填项，可以在这里加 _error
            continue

        val_str = case_cfg.get(sec, key)
        try:
            # 自动类型转换：含小数点转 float，否则转 int
            if '.' in val_str:
                val = float(val_str)
            else:
                val = int(val_str)
            
            # 执行 lambda 校验
            if not validator(val):
                _error(f"[{sec}] {key}={val} Invalid. Rule: {rule_desc}")
            else:
                # 关键数值参数建议 INFO 级别可见
                _ok(f"{key.ljust(25)}: {val}", is_detail=False)
        except ValueError:
            _error(f"[{sec}] {key} is not a valid number")

    # --- 2.4 Env 路径检查 ---
    for key in PATH_CHECKS:
        if env_cfg.has_option("Paths", key):
            path = env_cfg.get("Paths", key).strip()
            # 允许 None 或 空字符串 (视具体逻辑而定，这里假设必须有值)
            if not path or path.lower() == 'none':
                _error(f"Path is empty: [{key}]")
                continue

            if not os.path.exists(path):
                _error(f"Path not found ({key}): {path}")
            else:
                _ok(f"{key.ljust(25)}: OK", is_detail=True) # 路径检查在 DEBUG 显示
        else:
            _error(f"Env [Paths] missing key: {key}")

    # ---2.4 环境变量检查 ---
    conda_envs_text = ""
    conda_available = True
    try:
        r = subprocess.run(
            ["conda", "info", "--envs"],
            capture_output=True,
            text=True,
            check=True
        )
        conda_envs_text = r.stdout
    except Exception as e:
        conda_available = False
        _error(f"Conda not available or failed to query envs: {e}")

    for key in ENV_CHECKS:
        if env_cfg.has_option("Environment", key):
            val = env_cfg.get("Environment", key).strip()

            # 允许 None 或空
            if not val or val.lower() == "none":
                _error(f"Value is empty: [{key}]")
                continue

            # -------------------------
            # SYS_*：按路径检查
            # -------------------------
            if key.startswith("SYS_"):
                if not os.path.exists(val):
                    _error(f"SYS env path not found ({key}): {val}")
                else:
                    _ok(f"{key.ljust(25)}: OK", is_detail=True)

            # -------------------------
            # CONDA_*：按 conda 环境名检查
            # -------------------------
            elif key.startswith("CONDA_"):
                if not conda_available:
                    _error(f"Cannot check conda env ({key}): conda command unavailable")
                    continue

                found = False
                # conda info --envs 输出：每行第一个字段是 env name（可能含 *）
                for line in conda_envs_text.splitlines():
                    s = line.strip()
                    if not s or s.startswith("#"):
                        continue

                    parts = s.split()
                    if not parts:
                        continue

                    env_name = parts[0]
                    # 处理当前激活环境标记 "*"
                    if env_name == "*":
                        if len(parts) >= 2:
                            env_name = parts[1]
                        else:
                            continue

                    if env_name == val:
                        found = True
                        break

                if not found:
                    _error(f"Conda env not found ({key}): {val}")
                else:
                    _ok(f"{key.ljust(25)}: OK", is_detail=True)

            # -------------------------
            # 其他前缀：按你的策略处理（这里直接报错更安全）
            # -------------------------
            else:
                _error(f"Unknown env key prefix ({key}): {val}")

        else:
            _error(f"Env [Environment] missing key: {key}")




    # --- 2.5 时间逻辑检查 ---
    st_str = case_cfg.get(gridname, 'StartTime')
    et_str = case_cfg.get(gridname, 'EndTime')
    try:
        st = datetime.strptime(st_str, "%Y-%m-%d_%H:%M:%S")
        et = datetime.strptime(et_str, "%Y-%m-%d_%H:%M:%S")
        if st >= et:
            _error(f"StartTime ({st_str}) >= EndTime ({et_str})")
        else:
            _ok(f"TimeRange: {st_str} -> {et_str}", is_detail=False)
    except ValueError:
        _error(f"Time format error. Use YYYY-MM-DD_HH:MM:SS")

    # --- 2.6 Forcing 交叉验证 (难点) ---
    forcing_name = case_cfg.get(gridname, "ForcingDataName", fallback="Unknown").strip()
    logging.info(f"{Consts.S4}Checking Forcing Configuration: {forcing_name}")

    matched_sec = None
    # 排除 [Paths] section
    env_sections = [s for s in env_cfg.sections() if s.lower() != 'paths']
    
    for sec in env_sections:
        # 规则1: Section 名字直接匹配 (忽略大小写)
        sec_name_match = (sec.lower() == forcing_name.lower())
        
        # 规则2: Section 内的 ForcingDataName key 匹配
        key_match = False
        if env_cfg.has_option(sec, "ForcingDataName"):
            key_match = (env_cfg.get(sec, "ForcingDataName").lower() == forcing_name.lower())
            
        if sec_name_match or key_match:
            matched_sec = sec
            break
    
    if not matched_sec:
        _error(f"Case uses ForcingDataName='{forcing_name}', but no matching section found in env.ini")
    else:
        _ok(f"Matched Env Section: [{matched_sec}]", is_detail=False)
        
        # 检查 Forcing Section 内部的 _Path 文件
        # 仅检查以 _Path 结尾的 Key
        for key, val in env_cfg.items(matched_sec):
            if key.endswith("_Path") and val and val.lower() != 'none':
                # 处理模板路径 (含 <YYYY> 等)
                if "<" in val and ">" in val:
                    # 简单验证：尝试取第一个模板符之前的路径作为父目录
                    base_dir = val.split('<')[0]
                    parent = os.path.dirname(base_dir) # 再向上一级，确保安全
                    if parent and os.path.exists(parent):
                        _ok(f"{key.ljust(25)}: Template OK (Root exists)", is_detail=True)
                    else:
                        # 模板路径如果连根目录都没有，通常是错的，报 Warning 或 Error
                        logging.warning(f"{Consts.S4}[Warn] {key} template root not found: {parent}")
                else:
                    # 普通路径
                    if not os.path.exists(val):
                        _error(f"Forcing file missing [{matched_sec}] {key}: {val}")
                    else:
                        _ok(f"{key.ljust(25)}: OK", is_detail=True)

    # =========================================================
    # 3. 最终总结
    # =========================================================
    if ERROR_LIST:
        print("\n" + "!" * 60)
        logging.error(f"Config Check FAILED with {len(ERROR_LIST)} errors:")
        for msg in ERROR_LIST:
            # 再次打印错误列表，方便用户查看
            print(f"  - {msg}")
        print("!" * 60 + "\n")
        sys.exit(1) # 统一退出
    else:
        logging.info(f"{Consts.S4}✓ All Configuration Checks Passed.\n")




def Make_Dirs(casecfg, envcfg, gridname):
    """
    Make directories for the case
    """
    CaseOutputPath = casecfg.get(gridname, 'CaseOutputPath')
    casepath = f'{CaseOutputPath}/{gridname}'
    directories = [
        f'{casepath}',
        f'{casepath}/Log',
        f'{casepath}/NMLS',
        f'{casepath}/PrepCWRF',
        f'{casepath}/PrepCWRF/First_StaticData',
        f'{casepath}/PrepCWRF/First_StaticData/Geogrid',
        f'{casepath}/PrepCWRF/First_StaticData/Geog_{gridname}',
        f'{casepath}/PrepCWRF/First_StaticData/GeogPostProcess',
        f'{casepath}/PrepCWRF/First_StaticData/FVC',
        f'{casepath}/PrepCWRF/First_StaticData/IGBP',
        f'{casepath}/PrepCWRF/First_StaticData/temp',
        f'{casepath}/PrepCWRF/First_StaticData/LAI',
        f'{casepath}/PrepCWRF/First_StaticData/SAI',
        f'{casepath}/PrepCWRF/Second_ICBC',
        f'{casepath}/PrepCWRF/Second_ICBC/Geog_Gather',
        f'{casepath}/PrepCWRF/{gridname}',
        f'{casepath}/PrepCoLM',
        f'{casepath}/PrepCoLM/First_GenMesh',
        f'{casepath}/PrepCoLM/Second_MakeSrf',
        f'{casepath}/PrepCoLM/Second_MakeSrf/CoLMSrf_{gridname}',
        f'{casepath}/PrepCoLM/Third_Remap',
        f'{casepath}/PrepCoLM/{gridname}',
        f'{casepath}/PrepCRESM',
        f'{casepath}/PrepCRESM/{gridname}',
        f'{casepath}/PrepCRESM/{gridname}/cpl7data',
        f'{casepath}/{gridname}',
        f'{casepath}/{gridname}/Grid_{gridname}',
        f'{casepath}/{gridname}/ICBC_{gridname}',
    ]
    
    for directory in directories:
        if not os.path.exists(directory):
            os.makedirs(directory, exist_ok=True)
            logger.debug(f"{Consts.S4}Created directory: {directory}")
        else:
            logger.debug(f"{Consts.S4}Directory already exists: {directory}")
    logger.info(f"{Consts.S4}Directories for case < {gridname} > created.\n\n")



def Modify_CWPSNML(casecfg, envcfg, gridname):
    GeogDataPath = envcfg.get('Paths', 'GeogDataPath')
    ScriptPath = envcfg.get('Paths', 'ScriptPath')
    ForcName = casecfg.get(gridname, 'ForcingDataName')
    StartTime = casecfg.get(gridname, 'StartTime')
    EndTime = casecfg.get(gridname, 'EndTime')
    EdgeNum_WE = casecfg.getint(gridname, 'EdgeNum_WE')
    EdgeNum_SN = casecfg.getint(gridname, 'EdgeNum_SN')
    dx_WE = casecfg.get(gridname, 'dx_WE')
    dy_SN = casecfg.get(gridname, 'dy_SN')
    RefLat = casecfg.get(gridname, 'RefLat')
    RefLon = casecfg.get(gridname, 'RefLon')
    True_Lat1 = casecfg.get(gridname, 'True_Lat1')
    True_Lat2 = casecfg.get(gridname, 'True_Lat2')
    StandLon = casecfg.get(gridname, 'StandLon')
    BdyWidth = casecfg.getint(gridname, 'BdyWidth')
    CaseOutputPath = casecfg.get(gridname, 'CaseOutputPath')
    CtlCWPSNML = f"{ScriptPath}/NML/namelist.cwps.{ForcName.lower()}"

    # Check if the CWPS namelist file exists
    Tools.File_Exist(CtlCWPSNML, level='error')
    
    with open(CtlCWPSNML, 'r') as file:
        lines = file.readlines()
    for i, line in enumerate(lines):
        if 'StartTime' in line:
            lines[i] = lines[i].replace('StartTime', f'{StartTime}')
        elif 'EndTime' in line:
            lines[i] = lines[i].replace('EndTime', f'{EndTime}')
        elif 'ISTART' in line:
            lines[i] = lines[i].replace('ISTART', f'{BdyWidth}')
        elif 'JSTART' in line:
            lines[i] = lines[i].replace('JSTART', f'{BdyWidth}')
        elif 'EdgeNum_WE' in line:
            lines[i] = lines[i].replace('EdgeNum_WE', f'{EdgeNum_WE}, {int(EdgeNum_WE)-2*int(BdyWidth)}')
        elif 'EdgeNum_SN' in line:
            lines[i] = lines[i].replace('EdgeNum_SN', f'{EdgeNum_SN}, {int(EdgeNum_SN)-2*int(BdyWidth)}')
        elif 'dx_WE' in line:
            lines[i] = lines[i].replace('dx_WE', f'{dx_WE}')
        elif 'dy_SN' in line:
            lines[i] = lines[i].replace('dy_SN', f'{dy_SN}')
        elif 'RefLat' in line:
            lines[i] = lines[i].replace('RefLat', f'{RefLat}')
        elif 'RefLon' in line:
            lines[i] = lines[i].replace('RefLon', f'{RefLon}')
        elif 'True_Lat1' in line:
            lines[i] = lines[i].replace('True_Lat1', f'{True_Lat1}')
        elif 'True_Lat2' in line:
            lines[i] = lines[i].replace('True_Lat2', f'{True_Lat2}')
        elif 'StandLon' in line:
            lines[i] = lines[i].replace('StandLon', f'{StandLon}')
        elif 'GeogDataPath' in line:
            lines[i] = lines[i].replace('GeogDataPath', f'{GeogDataPath}')
    
    NewWPSNML = f'{CaseOutputPath}/{gridname}/NMLS/namelist.cwps.{gridname}'
    with open(NewWPSNML, 'w') as file:
        file.writelines(lines)

    logger.info(f"{Consts.S4}-> Modified CWPS namelist file: {NewWPSNML}")



def Modify_CWRFNML(casecfg, envcfg, gridname):
    ScriptPath = envcfg.get('Paths', 'ScriptPath')
    CaseOutputPath = casecfg.get(gridname, 'CaseOutputPath')
    ForcName = casecfg.get(gridname, 'ForcingDataName')
    StartTime = casecfg.get(gridname, 'StartTime')
    EndTime = casecfg.get(gridname, 'EndTime')
    EdgeNum_WE = casecfg.getint(gridname, 'EdgeNum_WE')
    EdgeNum_SN = casecfg.getint(gridname, 'EdgeNum_SN')
    dx_WE = casecfg.get(gridname, 'dx_WE')
    dy_SN = casecfg.get(gridname, 'dy_SN')
    BdyWidth = casecfg.getint(gridname, 'BdyWidth')
    StartTime = datetime.strptime(StartTime, '%Y-%m-%d_%H:%M:%S')
    EndTime = datetime.strptime(EndTime, '%Y-%m-%d_%H:%M:%S')
    CtlCWRFNML = f"{ScriptPath}/NML/namelist.cwrf.{ForcName.lower()}"
    
    # Check if the CWRF namelist file exists
    Tools.File_Exist(CtlCWRFNML, level='error')
    
    with open(CtlCWRFNML, 'r') as file:
        lines = file.readlines()
    for i, line in enumerate(lines):
        if 'SYEAR' in line:
            lines[i] = lines[i].replace('SYEAR', f'{StartTime.year}')
        elif 'SMONTH' in line:
            lines[i] = lines[i].replace('SMONTH', f'{StartTime.month}')
        elif 'SDAY' in line:
            lines[i] = lines[i].replace('SDAY', f'{StartTime.day}')
        elif 'SHOUR' in line:
            lines[i] = lines[i].replace('SHOUR', f'{StartTime.hour}')
        elif 'EYEAR' in line:
            lines[i] = lines[i].replace('EYEAR', f'{EndTime.year}')
        elif 'EMONTH' in line:
            lines[i] = lines[i].replace('EMONTH', f'{EndTime.month}')
        elif 'EDAY' in line:
            lines[i] = lines[i].replace('EDAY', f'{EndTime.day}')
        elif 'EHOUR' in line:
            lines[i] = lines[i].replace('EHOUR', f'{EndTime.hour}')
        elif 'EdgeNum_WE' in line:
            lines[i] = lines[i].replace('EdgeNum_WE', f'{EdgeNum_WE}')
        elif 'EdgeNum_SN' in line:
            lines[i] = lines[i].replace('EdgeNum_SN', f'{EdgeNum_SN}')
        elif 'dx_WE' in line:
            lines[i] = lines[i].replace('dx_WE', f'{dx_WE}')
        elif 'dy_SN' in line:
            lines[i] = lines[i].replace('dy_SN', f'{dy_SN}')
        elif 'BDYWIDTH' in line:
            lines[i] = lines[i].replace('BDYWIDTH', f'{str(BdyWidth)}')
        elif 'RELAX_ZONE' in line:
            lines[i] = lines[i].replace('RELAX_ZONE', f'{str(BdyWidth-1)}')
    
    NewCWRFNML = f'{CaseOutputPath}/{gridname}/NMLS/namelist.cwrf.{gridname}'
    with open(NewCWRFNML, 'w') as file:
        file.writelines(lines)

    logger.info(f"{Consts.S4}-> Modified CWRF namelist file: {NewCWRFNML}")



def Modify_CRESMNML(casecfg, envcfg, gridname):
    ScriptPath = envcfg.get('Paths', 'ScriptPath')
    CaseOutputPath = casecfg.get(gridname, 'CaseOutputPath')
    StartTime = casecfg.get(gridname, 'StartTime')
    EndTime = casecfg.get(gridname, 'EndTime')
    EdgeNum_WE = casecfg.getint(gridname, 'EdgeNum_WE')
    EdgeNum_SN = casecfg.getint(gridname, 'EdgeNum_SN')
    dx_WE = casecfg.get(gridname, 'dx_WE')
    dy_SN = casecfg.get(gridname, 'dy_SN')
    BdyWidth = casecfg.getint(gridname, 'BdyWidth')
    StartTime = datetime.strptime(StartTime, '%Y-%m-%d_%H:%M:%S')
    EndTime = datetime.strptime(EndTime, '%Y-%m-%d_%H:%M:%S')
    CtlCRESMNML = f"{ScriptPath}/NML/namelist.cresm.ctl"
    
    # Check if the CRESM namelist file exists
    Tools.File_Exist(CtlCRESMNML, level='error')
    
    with open(CtlCRESMNML, 'r') as file:
        lines = file.readlines()
    for i, line in enumerate(lines):
        if 'SYEAR' in line:
            lines[i] = lines[i].replace('SYEAR', f'{StartTime.year}')
        elif 'SMONTH' in line:
            lines[i] = lines[i].replace('SMONTH', f'{StartTime.month}')
        elif 'SDAY' in line:
            lines[i] = lines[i].replace('SDAY', f'{StartTime.day}')
        elif 'SHOUR' in line:
            lines[i] = lines[i].replace('SHOUR', f'{StartTime.hour}')
        elif 'EYEAR' in line:
            lines[i] = lines[i].replace('EYEAR', f'{EndTime.year}')
        elif 'EMONTH' in line:
            lines[i] = lines[i].replace('EMONTH', f'{EndTime.month}')
        elif 'EDAY' in line:
            lines[i] = lines[i].replace('EDAY', f'{EndTime.day}')
        elif 'EHOUR' in line:
            lines[i] = lines[i].replace('EHOUR', f'{EndTime.hour}')
        elif 'EdgeNum_WE' in line:
            lines[i] = lines[i].replace('EdgeNum_WE', f'{EdgeNum_WE}')
        elif 'EdgeNum_SN' in line:
            lines[i] = lines[i].replace('EdgeNum_SN', f'{EdgeNum_SN}')
        elif 'dx_WE' in line:
            lines[i] = lines[i].replace('dx_WE', f'{dx_WE}')
        elif 'dy_SN' in line:
            lines[i] = lines[i].replace('dy_SN', f'{dy_SN}')
        elif 'BDYWIDTH' in line:
            lines[i] = lines[i].replace('BDYWIDTH', f'{str(BdyWidth)}')
        elif 'RELAX_ZONE' in line:
            lines[i] = lines[i].replace('RELAX_ZONE', f'{str(BdyWidth-1)}')
    
    NewCRESMNML = f'{CaseOutputPath}/{gridname}/NMLS/namelist.cresm.{gridname}'
    with open(NewCRESMNML, 'w') as file:
        file.writelines(lines)

    logger.info(f"{Consts.S4}-> Modified CRESM namelist file: {NewCRESMNML}")



def Modify_CFNML(casecfg, envcfg, gridname):
    ScriptPath = envcfg.get('Paths', 'ScriptPath')
    CaseOutputPath = casecfg.get(gridname, 'CaseOutputPath')
    CtlCFNML = f"{ScriptPath}/NML/namelist.cf.ctl"
    EdgeNum_WE = casecfg.getint(gridname, 'EdgeNum_WE')
    EdgeNum_SN = casecfg.getint(gridname, 'EdgeNum_SN')
    # Check if the CF namelist file exists
    Tools.File_Exist(CtlCFNML, level='error')
    
    with open(CtlCFNML, 'r') as file:
        lines = file.readlines()
    for i, line in enumerate(lines):
        if 'CASENAME' in line:
            lines[i] = lines[i].replace('CASENAME', f'gridname')
        elif 'EdgeNum_WE' in line:
            lines[i] = lines[i].replace('EdgeNum_WE', f'{EdgeNum_WE-1}')
        elif 'EdgeNum_SN' in line:
            lines[i] = lines[i].replace('EdgeNum_SN', f'{EdgeNum_SN-1}')

    NewCFNML = f'{CaseOutputPath}/{gridname}/NMLS/namelist.cf.{gridname}'
    with open(NewCFNML, 'w') as file:
        file.writelines(lines)

    logger.info(f"{Consts.S4}-> Modified CF namelist file: {NewCFNML}")



def Modify_CoLMNML(casecfg, envcfg, gridname, run_type):
    ScriptPath = envcfg.get('Paths', 'ScriptPath')
    CaseOutputPath = casecfg.get(gridname, 'CaseOutputPath')
    CtlCoLMNML = f"{ScriptPath}/NML/unstructured_cwrf.colm.ctl"
    CoLMRawDataPath = envcfg.get('Paths', 'CoLMRawDataPath')
    CoLMRunDataPath = envcfg.get('Paths', 'CoLMRunDataPath')
    StartTime = casecfg.get(gridname, 'StartTime')
    EndTime = casecfg.get(gridname, 'EndTime')
    StartTime = datetime.strptime(StartTime, '%Y-%m-%d_%H:%M:%S')
    EndTime = datetime.strptime(EndTime, '%Y-%m-%d_%H:%M:%S')
    start_seconds = StartTime.hour * 3600 + StartTime.minute * 60 + StartTime.second
    end_seconds = EndTime.hour * 3600 + EndTime.minute * 60 + EndTime.second
    ICBCStartTime = StartTime
    ICBCEndTime = StartTime + timedelta(days=2)  # Add 2 days for ICBC run
    maxmin_wgs = Tools.Get_Area_MaxMin_Coords(casecfg, gridname)

    # Check if the CoLM namelist file exists
    Tools.File_Exist(CtlCoLMNML, level='error')
    
    with open(CtlCoLMNML, 'r') as file:
        lines = file.readlines()
    for i, line in enumerate(lines):
        if 'CASENAME' in line:
            if run_type == 'ICBC':
                lines[i] = lines[i].replace('CASENAME', f'unstructured_cwrf_{gridname}')
            else:
                lines[i] = lines[i].replace('CASENAME', f'unstructured_cwrf_{gridname}')
        elif 'SYEAR' in line:
            if run_type == 'ICBC':
                lines[i] = lines[i].replace('SYEAR', f'{ICBCStartTime.year}')
            else:
                lines[i] = lines[i].replace('SYEAR', f'styear')
        elif 'SMONTH' in line:
            if run_type == 'ICBC':
                lines[i] = lines[i].replace('SMONTH', f'{ICBCStartTime.month}')
            else:
                lines[i] = lines[i].replace('SMONTH', f'stmonth')
        elif 'SDAY' in line:
            if run_type == 'ICBC':
                lines[i] = lines[i].replace('SDAY', f'{ICBCStartTime.day}')
            else:
                lines[i] = lines[i].replace('SDAY', f'stday')
        elif 'SSEC' in line:
            if run_type == 'ICBC':
                lines[i] = lines[i].replace('SSEC', f'{start_seconds}')
            else:
                lines[i] = lines[i].replace('SSEC', f'stsec')
        elif 'EYEAR' in line:
            if run_type == 'ICBC':
                lines[i] = lines[i].replace('EYEAR', f'{ICBCEndTime.year}')
            else:
                lines[i] = lines[i].replace('EYEAR', f'etyear')
        elif 'EMONTH' in line:
            if run_type == 'ICBC':
                lines[i] = lines[i].replace('EMONTH', f'{ICBCEndTime.month}')
            else:
                lines[i] = lines[i].replace('EMONTH', f'etmonth')
        elif 'EDAY' in line:
            if run_type == 'ICBC':
                lines[i] = lines[i].replace('EDAY', f'{ICBCEndTime.day}')
            else:
                lines[i] = lines[i].replace('EDAY', f'etday')
        elif 'ESEC' in line:
            if run_type == 'ICBC':
                lines[i] = lines[i].replace('ESEC', f'{end_seconds}')
            else:
                lines[i] = lines[i].replace('ESEC', f'etsec')
        elif 'COLMTIMESTEP' in line:
            if run_type == 'ICBC':
                lines[i] = lines[i].replace('COLMTIMESTEP', f'3600')
            else:
                lines[i] = lines[i].replace('COLMTIMESTEP', f'600')
        elif 'COLMRAWDATA' in line:
            if run_type == 'ICBC':
                lines[i] = lines[i].replace('COLMRAWDATA', f'{CoLMRawDataPath}/')    
            else:
                lines[i] = lines[i].replace('COLMRAWDATA', f'CoLM_basic_data_dir/CoLMrawdata/')
        elif 'COLMRUNDATA' in line:
            if run_type == 'ICBC':
                lines[i] = lines[i].replace('COLMRUNDATA', f'{CoLMRunDataPath}/')
            else:
                lines[i] = lines[i].replace('COLMRUNDATA', f'CoLM_basic_data_dir/CoLMruntime/')
        elif 'COLMRUNPATH' in line:
            if run_type == 'ICBC':
                lines[i] = lines[i].replace('COLMRUNPATH', f'{CaseOutputPath}/{gridname}/PrepCoLM/Second_MakeSrf/')
            else:
                lines[i] = lines[i].replace('COLMRUNPATH', f'./CoLMrun/')
        elif 'MESHNAME' in line:
            lines[i] = lines[i].replace('MESHNAME', f'./mesh_cwrf_{gridname}.nc')
        elif 'WRESTFREQ' in line:
            if run_type == 'ICBC':
                lines[i] = lines[i].replace('WRESTFREQ', f'DAILY')
            else:
                lines[i] = lines[i].replace('WRESTFREQ', f'MONTHLY')
        elif 'HISTFREQ' in line:
            if run_type == 'ICBC':
                lines[i] = lines[i].replace('HISTFREQ', f'DAILY')
            else:
                lines[i] = lines[i].replace('HISTFREQ', f'DAILY')
        elif 'HISTGROUPBY' in line:
            if run_type == 'ICBC':
                lines[i] = lines[i].replace('HISTGROUPBY', f'YEAR')
            else:
                lines[i] = lines[i].replace('HISTGROUPBY', f'YEAR')
        elif 'EDGESSOUTH' in line:
            edges = max(math.ceil(maxmin_wgs["min_lat"] - 3), -90)
            if run_type == 'ICBC':
                lines[i] = lines[i].replace('EDGESSOUTH', f'{edges:.4f}')
            else:
                lines[i] = lines[i].replace('EDGESSOUTH', f'{edges:.4f}')
        elif 'EDGENORTH' in line:
            edgen = min(math.ceil(maxmin_wgs["max_lat"] + 3), 90)
            if run_type == 'ICBC':
                lines[i] = lines[i].replace('EDGENORTH', f'{edgen:.4f}')
            else:
                lines[i] = lines[i].replace('EDGENORTH', f'{edgen:.4f}')
        elif 'EDGEWEST' in line:
            edgew = max(math.ceil(maxmin_wgs["min_lon"] - 3), -180)
            if run_type == 'ICBC':
                lines[i] = lines[i].replace('EDGEWEST', f'{edgew:.4f}')
            else:
                lines[i] = lines[i].replace('EDGEWEST', f'{edgew:.4f}')
        elif 'EDGEEAST' in line:
            edgee = min(math.ceil(maxmin_wgs["max_lon"] + 3), 180)
            if run_type == 'ICBC':
                lines[i] = lines[i].replace('EDGEEAST', f'{edgee:.4f}')
            else:
                lines[i] = lines[i].replace('EDGEEAST', f'{edgee:.4f}')

        if run_type == 'ICBC':
            NewCoLMNML = f'{CaseOutputPath}/{gridname}/NMLS/unstructured_cwrf.colm.{gridname}.icbc'
            with open(NewCoLMNML, 'w') as file:
                file.writelines(lines)
        else:
            NewCoLMNML = f'{CaseOutputPath}/{gridname}/NMLS/unstructured_cwrf.colm.{gridname}.run'
            with open(NewCoLMNML, 'w') as file:
                file.writelines(lines)

    logger.info(f"{Consts.S4}-> Modified CoLM namelist file: {NewCoLMNML}")



def Show_Domain(casecfg, envcfg, gridname):
    """
    Show the domain of the case
    """
    old_path = os.getcwd()
    ScriptPath = envcfg.get('Paths', 'ScriptPath')
    GeogDataPath = envcfg.get('Paths', 'GeogDataPath')
    Go_ShowDomain = casecfg.getboolean('PrepCWRF', 'Go_ShowDomain')
    ProcessScriptPath = f"{ScriptPath}/ProcessScript"
    CaseOutputPath = casecfg.get(gridname, 'CaseOutputPath')
    RefLat = casecfg.getfloat(gridname, 'RefLat')
    RefLon = casecfg.getfloat(gridname, 'RefLon')
    True_Lat1 = casecfg.getfloat(gridname, 'True_Lat1')
    True_Lat2 = casecfg.getfloat(gridname, 'True_Lat2')
    dx_WE = casecfg.getfloat(gridname, 'dx_WE')
    dy_SN = casecfg.getfloat(gridname, 'dy_SN')
    EdgeNum_WE = casecfg.getint(gridname, 'EdgeNum_WE')
    EdgeNum_SN = casecfg.getint(gridname, 'EdgeNum_SN')
    BdyWidth = casecfg.getint(gridname, 'BdyWidth')
    cresmenv = envcfg.get('Environment', 'CONDA_CRESM')

    dem_coarsen = 3  # Coarsen the DEM for better visualization
    draw_lake = 'true'  # Draw lakes
    draw_river = 'true'  # Draw rivers
    draw_province = 'false'  # Draw provinces
    draw_country = 'true'  # Draw countries
    draw_city = 'false'  # Draw cities
    shapefile = ''
    # shapefile = '/stu01/wumej22/data/ERA5/yangtze_res_shp/YangtzeBasin.shp'

    if Go_ShowDomain:
        # Check if the process script exists
        logger.info(f'{Consts.S4}==========> Domain Plot <==========')
        Tools.File_Exist(f'{ProcessScriptPath}/PrepCWRF/NewPlot.py', level='error')

        # Prepare the command to run the script
        cmd = f'conda run -n {cresmenv} --no-capture-output python -u {ProcessScriptPath}/PrepCWRF/NewPlot.py --casename {gridname} '
        cmd += f' --EdgeNum_WE {EdgeNum_WE} --EdgeNum_SN {EdgeNum_SN} --dx_WE {dx_WE} --dy_SN {dy_SN} '
        cmd += f' --RefLat {RefLat} --RefLon {RefLon} --True_Lat1 {True_Lat1} --True_Lat2 {True_Lat2} '
        cmd += f' --BdyWidth {BdyWidth} --topodir {GeogDataPath}/topo_30s/ '
        cmd += f' --savepath {CaseOutputPath}/{gridname}/{gridname}.png '
        cmd += f' --plotcfg dem_coarsen={dem_coarsen} draw_lake={draw_lake} draw_river={draw_river} draw_province={draw_province} draw_country={draw_country} draw_city={draw_city} '
        cmd += f' shapefile={shapefile}'
        cmd += f' > {CaseOutputPath}/{gridname}/Log/log.geogrid_plot 2>&1'

        # Run the command
        Tools.Run_CMD(cmd, "Show Domain of the case")
        logger.info(f"{Consts.S4}-> Domain Plot Path: {CaseOutputPath}/{gridname}/{gridname}.png")
        logger.info(f'{Consts.S4}✓  Domain Plot finished!')
    else:
        logger.info(f'{Consts.S4}==========> Skip Domain Plot <==========')
        logger.info(f'{Consts.S4}!!! Skip the whole process !!!')

    logger.info(f'{Consts.S4}◉  Show Domain Complete!\n\n')
    os.chdir(old_path)




def Gather_Prepare_Data(casecfg, envcfg, gridname):
    old_path = os.getcwd()
    CaseOutputPath = casecfg.get(gridname, 'CaseOutputPath')
    CDOPath = envcfg.get('Paths', 'CDOPath')
    ScriptPath = envcfg.get('Paths', 'ScriptPath')
    ProcessScriptPath = f"{ScriptPath}/ProcessScript"
    Collect_CWRF_Output = casecfg.getboolean('GatherData', 'Collect_CWRF_Output')
    Collect_CoLM_Output = casecfg.getboolean('GatherData', 'Collect_CoLM_Output')
    Collect_CRESM_Output = casecfg.getboolean('GatherData', 'Collect_CRESM_Output')
    StartTime = casecfg.get(gridname, 'StartTime')
    StartTime = datetime.strptime(StartTime, '%Y-%m-%d_%H:%M:%S')

    os.chdir(f'{CaseOutputPath}/{gridname}/{gridname}/')

    if Collect_CWRF_Output or Collect_CoLM_Output or Collect_CRESM_Output: 

        if Collect_CWRF_Output:
            # cmd = f'rm -rf {CaseOutputPath}/{gridname}/{gridname}/*'
            # Tools.Run_CMD(cmd, "Remove old files")
            os.chdir(f'{CaseOutputPath}/{gridname}/{gridname}') 
            #---------------------- Collect CWRF Data ----------------------
            logger.info(f'{Consts.S4}==========> Collect CWRF Data <==========')
            geogdata = f'{CaseOutputPath}/{gridname}/PrepCWRF/{gridname}/Geog_{gridname}'
            if Tools.File_Exist(geogdata, level='warning'):
                cmd = f'rm -rf ./Grid_{gridname}/Geog_{gridname}'
                Tools.Run_CMD(cmd, "Remove old Geog data")
                Tools.Copy(f'{CaseOutputPath}/{gridname}/PrepCWRF/{gridname}/Geog_{gridname}', f'./Grid_{gridname}/')

            geo_em_veg_path = f'{CaseOutputPath}/{gridname}/PrepCWRF/{gridname}/Geog_{gridname}/geo_em.d01_veg.nc'
            if Tools.File_Exist(geo_em_veg_path, level='warning'):
                cmd = f'rm -f ./Grid_{gridname}/geo_em.d01_veg.nc'
                Tools.Run_CMD(cmd, "Remove old geo_em.d01_veg.nc file")
                Tools.Copy(geo_em_veg_path, f'./Grid_{gridname}/geo_em.d01_veg.nc')
                cmd = f'rm -f ./Grid_{gridname}/geo_em.d01.nc'
                Tools.Run_CMD(cmd, "Remove old geo_em.d01.nc file")
                Tools.Copy(geo_em_veg_path, f'./Grid_{gridname}/geo_em.d01.nc')
            
            FVCPath = f'{CaseOutputPath}/{gridname}/PrepCWRF/{gridname}/Geog_{gridname}/MODIS2CWRF_SBC_d01.nc'
            if Tools.File_Exist(FVCPath, level='warning'):
                cmd = f'rm -f ./Grid_{gridname}/modis_FVC_d01.nc'
                Tools.Run_CMD(cmd, "Remove old modis_FVC_d01.nc file")
                Tools.Copy(FVCPath, f'./Grid_{gridname}/modis_FVC_d01.nc')
            
            wrfinput = f'{CaseOutputPath}/{gridname}/PrepCWRF/{gridname}/wrfinput_d01'
            if Tools.File_Exist(wrfinput, level='warning'):
                cmd = f'rm -f ./ICBC_{gridname}/wrfinput_d01'
                Tools.Run_CMD(cmd, "Remove old wrfinput file")
                Tools.Copy(wrfinput, f'./ICBC_{gridname}/wrfinput_d01')
            
            wrfbdy = f'{CaseOutputPath}/{gridname}/PrepCWRF/{gridname}/wrfbdy_d01'
            if Tools.File_Exist(wrfbdy, level='warning'):
                cmd = f'rm -f ./ICBC_{gridname}/wrfbdy_d01'
                Tools.Run_CMD(cmd, "Remove old wrfbdy file")
                Tools.Copy(wrfbdy, f'./ICBC_{gridname}/wrfbdy_d01')
            
            wrflowinp_d01 = f'{CaseOutputPath}/{gridname}/PrepCWRF/{gridname}/wrflowinp_d01'
            if Tools.File_Exist(wrflowinp_d01, level='warning'):
                cmd = f'rm -f ./ICBC_{gridname}/wrflowinp_d01'
                Tools.Run_CMD(cmd, "Remove old wrflowinp file")
                Tools.Copy(wrflowinp_d01, f'./ICBC_{gridname}/wrflowinp_d01')
            
            wrfveg_d01 = f'{CaseOutputPath}/{gridname}/PrepCWRF/{gridname}/wrfveg_d01'
            if Tools.File_Exist(wrfveg_d01, level='warning'):
                cmd = f'rm -f ./ICBC_{gridname}/wrfveg_d01'
                Tools.Run_CMD(cmd, "Remove old wrfveg file")
                Tools.Copy(wrfveg_d01, f'./ICBC_{gridname}/wrfveg_d01')
            
            wrfsst_d01 = f'{CaseOutputPath}/{gridname}/PrepCWRF/{gridname}/wrfsst_d01'
            if Tools.File_Exist(wrfsst_d01, level='warning'):
                cmd = f'rm -f ./ICBC_{gridname}/wrfsst_d01'
                Tools.Run_CMD(cmd, "Remove old wrfsst file")
                Tools.Copy(wrfsst_d01, f'./ICBC_{gridname}/wrfsst_d01')
                logger.info(f'{Consts.S4}✓  Collect CWRF Data finished!')
                os.chdir(old_path)
            
        if Collect_CoLM_Output:
            os.chdir(f'{CaseOutputPath}/{gridname}/{gridname}') #/Basic
            #---------------------- Gather CoLM Data ----------------------    
            logger.info(f'{Consts.S4}==========> Gather CoLM Data <==========')
            
            glmask = f'{CaseOutputPath}/{gridname}/PrepCoLM/{gridname}/GLMASK.nc'
            if Tools.File_Exist(glmask, level='warning'):
                cmd = f'rm -f ./Grid_{gridname}/GLMASK_{gridname}_noFVCOM.nc'
                Tools.Run_CMD(cmd, "Remove old GLMASK file")
                Tools.Copy(glmask, f'./Grid_{gridname}/GLMASK_{gridname}_noFVCOM.nc')
            
            htop_rcm = f'{CaseOutputPath}/{gridname}/PrepCoLM/{gridname}/htop_rcm.nc'
            if Tools.File_Exist(htop_rcm, level='warning'):
                cmd = f'rm -f ./Grid_{gridname}/htop_rcm_{gridname}.nc'
                Tools.Run_CMD(cmd, "Remove old htop_rcm file")
                Tools.Copy(htop_rcm, f'./Grid_{gridname}/htop_rcm_{gridname}.nc')
            
            # link CoLM ref data
            colmref = f'{CaseOutputPath}/{gridname}/PrepCoLM/{gridname}/CoLM_ref_{gridname}.nc'
            if Tools.File_Exist(colmref, level='warning'):
                cmd = f'rm -f ./Grid_{gridname}/CoLM_ref_{gridname}.nc'
                Tools.Run_CMD(cmd, "Remove old CoLM_ref file")
                Tools.Copy(colmref, f'./Grid_{gridname}/CoLM_ref_{gridname}.nc')
            
            # link elmindex
            elmindex = f'{CaseOutputPath}/{gridname}/PrepCoLM/{gridname}/unstructured_cwrf_{gridname}/history/unstructured_cwrf_{gridname}_hist_{StartTime.year}.nc'
            if Tools.File_Exist(elmindex, level='warning'):
                cmd = f'rm -f ./Grid_{gridname}/elmindex.nc'
                Tools.Run_CMD(cmd, "Remove old elmindex file")
                cmd = f'{CDOPath} -selvar,elmindex {elmindex} ./Grid_{gridname}/elmindex.nc'
                Tools.Run_CMD(cmd, f"Link elmindex.nc")
            
            # link unstructured_cwrf_{gridname} files
            unstructured_cwrf_path = f'{CaseOutputPath}/{gridname}/PrepCoLM/{gridname}/unstructured_cwrf_{gridname}'
            if Tools.File_Exist(unstructured_cwrf_path, level='warning'):
                cmd = f'rm -rf ./Grid_{gridname}/unstructured_cwrf_{gridname}'
                Tools.Run_CMD(cmd, "Remove old unstructured_cwrf file")
                Tools.Copy(unstructured_cwrf_path, f'./Grid_{gridname}/')
            
            meshfile = f'{CaseOutputPath}/{gridname}/PrepCoLM/{gridname}/mesh_cwrf_{gridname}.nc'
            if Tools.File_Exist(meshfile, level='warning'):
                cmd = f'rm -f ./Grid_{gridname}/mesh_cwrf_{gridname}.nc'
                Tools.Run_CMD(cmd, "Remove old mesh_cwrf file")
                Tools.Copy(meshfile, f'./Grid_{gridname}/mesh_cwrf_{gridname}.nc')
            
            # copy CoLM Srf landdata
            CoLMSrf = f'{CaseOutputPath}/{gridname}/PrepCoLM/Second_MakeSrf/CoLMSrf_{gridname}'
            if Tools.File_Exist(CoLMSrf, level='warning'):
                cmd = f'rm -rf {CaseOutputPath}/{gridname}/{gridname}/Grid_{gridname}/CoLMSrf_{gridname}'
                Tools.Run_CMD(cmd, "Remove old CoLMSrf file")
                Tools.Copy(CoLMSrf, f'{CaseOutputPath}/{gridname}/{gridname}/Grid_{gridname}/')

            logger.info(f'{Consts.S4}✓  Gather CoLM Data finished!')
            os.chdir(old_path)

        if Collect_CRESM_Output:
            os.chdir(f'{CaseOutputPath}/{gridname}/{gridname}')  #/Basic
            #---------------------- Gather CRESM Data ----------------------  
            logger.info(f'{Consts.S4}==========> Gather CRESM Data <==========') 
            cpl7data = f'{CaseOutputPath}/{gridname}/PrepCRESM/{gridname}/cpl7data'
            if Tools.File_Exist(cpl7data, level='warning'):
                cmd = f'rm -rf ./Grid_{gridname}/cpl7data'
                Tools.Run_CMD(cmd, "Remove old cpl7data file")
                Tools.Copy(cpl7data, f'./Grid_{gridname}/')
            
            chanlu = f'{ProcessScriptPath}/PrepCRESM/chanlu.ncl'
            if Tools.File_Exist(chanlu, level='warning'):
                cmd = f'rm -f ./Grid_{gridname}/chanlu.ncl'
                Tools.Run_CMD(cmd, "Remove old chanlu file")
                Tools.Copy(chanlu, f'./Grid_{gridname}/')
            
            alignlucc = f'{ProcessScriptPath}/PrepCRESM/alignlucc.ncl'
            if Tools.File_Exist(alignlucc, level='warning'):
                cmd = f'rm -f ./Grid_{gridname}/alignlucc.ncl'
                Tools.Run_CMD(cmd, "Remove old alignlucc file")
                Tools.Copy(alignlucc, f'./Grid_{gridname}/')
            
            #---------------------- Gather namelist ----------------------
            cresmnml = f'{CaseOutputPath}/{gridname}/NMLS/namelist.cresm.{gridname}'
            if Tools.File_Exist(cresmnml, level='warning'):
                cmd = f'rm -f ./Grid_{gridname}/namelist.input_CRESM'
                Tools.Run_CMD(cmd, "Remove old CRESM namelist file")
                Tools.Copy(cresmnml, f'./Grid_{gridname}/namelist.input_CRESM')
            
            cwrfnml = f'{CaseOutputPath}/{gridname}/NMLS/namelist.cwrf.{gridname}'
            if Tools.File_Exist(cwrfnml, level='warning'):
                cmd = f'rm -f ./Grid_{gridname}/namelist.input_ICBC'
                Tools.Run_CMD(cmd, "Remove old CWRF namelist file")
                Tools.Copy(cwrfnml, f'./Grid_{gridname}/namelist.input_ICBC')
            
            colmnml = f'{CaseOutputPath}/{gridname}/NMLS/unstructured_cwrf.colm.{gridname}.run'
            if Tools.File_Exist(colmnml, level='warning'):
                cmd = f'rm -f ./Grid_{gridname}/unstructured_cwrf_{gridname}.nml'
                Tools.Run_CMD(cmd, "Remove old CoLM namelist file")
                Tools.Copy(colmnml, f'./Grid_{gridname}/unstructured_cwrf_{gridname}.nml')
            
            cfnml = f'{CaseOutputPath}/{gridname}/NMLS/namelist.cf.{gridname}'
            if Tools.File_Exist(cfnml, level='warning'):
                cmd = f'rm -f ./Grid_{gridname}/namelist.cf'
                Tools.Run_CMD(cmd, "Remove old CF namelist file")
                Tools.Copy(cfnml, f'./Grid_{gridname}/namelist.cf')

            historynml = f'{ScriptPath}/NML/history.colm.ctl'
            if Tools.File_Exist(historynml, level='warning'):
                cmd = f'rm -f ./Grid_{gridname}/history.nml'
                Tools.Run_CMD(cmd, "Remove old history.nml file")
                Tools.Copy(historynml, f'./Grid_{gridname}/history.nml')
            
            nofocingnml = f'{ScriptPath}/NML/CoLM_Forcing/noforcing.nml'
            if Tools.File_Exist(nofocingnml, level='warning'):
                cmd = f'rm -f ./Grid_{gridname}/noforcing.nml'
                Tools.Run_CMD(cmd, "Remove old noforcing.nml file")
                Tools.Copy(nofocingnml, f'./Grid_{gridname}/noforcing.nml')
            
            submitlsf = f'{ScriptPath}/NML/submit.lsf'
            if Tools.File_Exist(submitlsf, level='warning'):
                cmd = f'rm -f ./Grid_{gridname}/submit.lsf'
                Tools.Run_CMD(cmd, "Remove old submit.lsf file")
                Tools.Copy(submitlsf, f'./Grid_{gridname}/submit.lsf')
            
            submitslurm = f'{ScriptPath}/NML/submit.slurm'
            if Tools.File_Exist(submitslurm, level='warning'):
                cmd = f'rm -f ./Grid_{gridname}/submit.slurm'
                Tools.Run_CMD(cmd, "Remove old submit.slurm file")
                Tools.Copy(submitslurm, f'./Grid_{gridname}/submit.slurm')
            
            create_run = f'{ProcessScriptPath}/PrepCRESM/create_run_from_cps.py'
            if Tools.File_Exist(create_run, level='warning'):
                cmd = f'rm -f ./Create_Run_From_CPS.py'
                Tools.Run_CMD(cmd, "Remove old Create_Run_From_CPS.py file")
                Tools.Copy(create_run, f'./Create_Run_From_CPS.py')

            logger.info(f'{Consts.S4}✓  Gather CRESM Data finished!')
            os.chdir(old_path)
        logger.info(f'{Consts.S4}✓  Gather Data finished!')
    else:
        logger.info(f'{Consts.S4}==========> Skip Gather Data <==========')
        logger.info(f'{Consts.S4}!!! Skip the whole process !!!')
    logger.info(f'{Consts.S4}◉  Gather Prep Data Complete!\n\n')
    os.chdir(old_path)



def Collect_Yearly_Data(casecfg, envcfg, gridname):
    old_path = os.getcwd()
    CaseOutputPath = casecfg.get(gridname, 'CaseOutputPath')
    CoreNum = casecfg.getint('BaseInfo', 'TimeChunkCount')
    casesdir = glob.glob(f'{CaseOutputPath}/{gridname}.*')

    logger.info(f'{Consts.S4}==========> Collect Yearly Data <==========')

    if not casesdir:
        logger.error(f'No cases found with prefix {gridname}')
        sys.exit(1)

    target_dir = os.path.join(CaseOutputPath, f'Collect_{gridname}')
    os.makedirs(target_dir, exist_ok=True)
    grid_dir = os.path.join(CaseOutputPath, f'Collect_{gridname}/Grid_{gridname}')
    os.makedirs(grid_dir, exist_ok=True)
    icbc_dir = os.path.join(CaseOutputPath, f'Collect_{gridname}/ICBC_{gridname}')
    os.makedirs(icbc_dir, exist_ok=True)

    logger.info(f'Created directory: {target_dir}')

    casesdir.sort()
    logger.info(f'Found {len(casesdir)} cases with prefix <{gridname}>')

    for case in casesdir:
        # ✅ 必须放到这里：每个 case 单独一批任务，避免累积重复执行
        tasks = []
        # 记录需要做“目录内部递归改名”的目标目录（只对目录做）
        rename_targets = []

        casename = os.path.basename(case)
        caseyear = casename.split('.')[-1]
        casefiles = glob.glob(f'{CaseOutputPath}/{casename}/{casename}/*')

        if not casefiles:
            logger.warning(f'No files found in {case}/{casename}')
            continue

        logger.info(f'-> Collecting files from {casename}...')

        grid_items = [
            'alignlucc.ncl',
            'chanlu.ncl',
            f'CoLM_ref_{casename}.nc',
            'namelist.input_CRESM',
            'namelist.input_ICBC',
            'namelist.cf',
            'noforcing.nml',
            'history.nml',
            'submit.lsf',
            'submit.slurm',
            f'CoLMSrf_{casename}',
            f'Geog_{casename}',
            'cpl7data',
            'elmindex.nc',
            'geo_em.d01.nc',
            'geo_em.d01_veg.nc',
            f'GLMASK_{casename}_noFVCOM.nc',
            f'htop_rcm_{casename}.nc',
            f"mesh_cwrf_{casename}.nc",
            f"modis_FVC_d01.nc",
            f"unstructured_cwrf_{casename}",
            f'unstructured_cwrf_{casename}.nml',
        ]

        for filename in grid_items:
            src = f'{CaseOutputPath}/{casename}/{casename}/Grid_{casename}/{filename}'
            if not Tools.File_Exist(src, level='warning'):
                continue

            newfilename = filename.replace(casename, gridname)
            dst = f'{target_dir}/Grid_{gridname}/{newfilename}'

            if os.path.isdir(src):
                if os.path.exists(dst) and not os.path.isdir(dst):
                    logger.warning(f'Destination exists but is not a directory: {dst}')
                    continue

                # 仍旧是“补齐缺失文件”的语义：--ignore-existing
                # 注意：这里用 rsync 源目录末尾 "/"，确保拷贝目录内容
                cmd = (
                    f'mkdir -p "{dst.rstrip("/")}" && '
                    f'rsync -a --ignore-existing "{src.rstrip("/")}/" "{dst.rstrip("/")}/"'
                )
                tasks.append((cmd, f"Fill missing files in dir {src} -> {dst}"))

                # 关键：目录内部可能还有 casename，需要在 rsync 后递归改名
                rename_targets.append(dst)

            else:
                if os.path.exists(dst) and os.path.isdir(dst):
                    logger.warning(f'Destination exists but is a directory: {dst}')
                    continue

                if not os.path.exists(dst):
                    parent = os.path.dirname(dst)
                    cmd = f'mkdir -p "{parent}" && rsync -a "{src}" "{dst}"'
                    tasks.append((cmd, f"Copy file {src} to {dst}"))
                else:
                    logger.info(f"Skipping existing file: {dst}")

        # year suffix files
        icbc_items = ['wrfinput_d01', 'wrfbdy_d01', 'wrflowinp_d01', 'wrfveg_d01', 'wrfsst_d01']
        for filename in icbc_items:
            file = f'{CaseOutputPath}/{casename}/{casename}/ICBC_{casename}/{filename}'
            if not Tools.File_Exist(file, level='warning'):
                continue
            newfile = f'{target_dir}/ICBC_{gridname}/{filename}.{caseyear}'
            cmd = f'rsync -a "{file}" "{newfile}"'
            tasks.append((cmd, f"Copy {file} to {newfile}"))

        # ✅ 并行执行复制任务（每个 case 一次）
        Tools.Run_Parallel(Tools.Run_CMD, tasks, CoreNum, "Collect Case")

        # ✅ 复制完成后：把目录内部所有 “casename” 递归改成 “gridname”
        # 只处理 rename_targets（那些目录型 grid_item）
        for d in rename_targets:
            Tools.rename_tree_tokens(d, casename, gridname, logger=logger)
       
        # ✅ 针对特定的 unstructured nml 文件做全局内容替换
        specific_nml = os.path.join(target_dir, f'Grid_{gridname}', f'unstructured_cwrf_{gridname}.nml')
        
        if os.path.exists(specific_nml):
            logger.info(f"-> Updating content in {os.path.basename(specific_nml)}...")
            try:
                with open(specific_nml, 'r') as f:
                    content = f.read()
                
                if casename in content:
                    new_content = content.replace(casename, gridname)
                    with open(specific_nml, 'w') as f:
                        f.write(new_content)
                    logger.info(f"   Successfully replaced '{casename}' with '{gridname}'")
            except Exception as e:
                logger.error(f"   Error updating {specific_nml}: {e}")

    logger.info(f'{Consts.S4}◉  Collect Case finished!\n\n')



def Clean_Temporary_Files(casecfg, envcfg, gridname):
    old_path = os.getcwd()
    CaseOutputPath = casecfg.get(gridname, 'CaseOutputPath')
    CleanTempFiles = casecfg.getboolean('BaseInfo', 'CleanTempFiles')

    tmpfiles = []

    if CleanTempFiles:
        logger.info('')
        logger.warning('You have chosen to clean temporary files to save disk space.')
        logger.warning('If you need to re-run any preparation steps, you may need to regenerate some data.')
        logger.warning('Please ensure you have backed up any important data before proceeding.\n')
        logger.warning(f'{Consts.S4}==========> Clean Temporary Files <==========')
        tmpfiles.append(f"{CaseOutputPath}/{gridname}/PrepCWRF/")
        tmpfiles.append(f"{CaseOutputPath}/{gridname}/PrepCoLM/")
        tmpfiles.append(f"{CaseOutputPath}/{gridname}/PrepCRESM/")
        tmpfiles.append(f"{CaseOutputPath}/{gridname}/NMLS/")
        tmpfiles.append(f"{CaseOutputPath}/{gridname}/Log/")

        for tmpfile in tmpfiles:
            cmd = f'rm -rf {tmpfile}'
            logger.warning(f'{Consts.S4}-> Remove {tmpfile}')
            Tools.Run_CMD(cmd, f"Remove {tmpfile}")
        logger.info(f'{Consts.S4}✓  Clean Temporary Files finished!')
    else:
        logger.info(f'{Consts.S4}==========> Skip Clean Temporary Files <==========')
        logger.info(f'{Consts.S4}!!! Skip the whole process !!!')
    logger.info(f'{Consts.S4}◉  Clean Temporary Files Complete!\n\n')
    os.chdir(old_path)



def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="CRESM Preprocessing System (CPS)",
        formatter_class=argparse.RawTextHelpFormatter
    )

    parser.add_argument(
        '-v', '--version',
        action='version',
        version='%(prog)s 1.0.0',
        help='Show version information and exit\n'
    )

    parser.add_argument(
        '-d', '--debug',
        action='store_true',
        help='Enable debug mode\n'
    )

    parser.add_argument(
        '-ch', '--confighelp',
        action='store_true',
        help='Display configuration help and exit\n'
    )

    parser.add_argument(
        '-l', '--listcases',
        action='store_true',
        help='List available “useful cases” in case.ini and exit\n'
    )

    parser.add_argument(
        '-n', '--gridname',
        type=str,
        default=None,
        metavar='NAME',
        help='Specify the case name (matches a section in case.ini)\n'
    )

    parser.add_argument(
        '-g', '--geogdir',
        type=str,
        default='',
        metavar='DIR',
        help=(
            'Use existing geography data from DIR for IC/BC preparation;\n'
            'if omitted, new geography data will be generated\n'
        )
    )

    parser.add_argument(
        '-s', '--colmsrf',
        type=str,
        default='',
        metavar='DIR',
        help=(
            'Use existing CoLM surface data from DIR for PrepCoLM preparation;\n'
            'if omitted, new surface data will be generated\n'
        )
    )

    parser.add_argument(
        '-y', '--year',
        type=int,
        default=None,
        metavar='YEAR',
        help=(
            'Override start/end year in the case (for yearly forcing data),\n'
            'e.g., --year 2023\n'
        )
    )

    parser.add_argument(
        '-c', '--collectcase',
        type=str,
        default=None,
        metavar='NAME',
        help=(
            'When using --year, gather all annual forcing data into the folder NAME;\n'
            'inside that folder, each forcing file is renamed with the year suffix.\n'
        )
    )

    return parser


def parse_args(argv=None):
    parser = build_parser()
    return parser.parse_args(argv)



def setup_main_logger(gridname: str, debug: bool) -> logging.Logger:
    cur_dir = os.getcwd()
    logfile = f'{cur_dir}/log.DataPrepare.{gridname}.log'
    loglevel = logging.DEBUG if debug else logging.INFO
    return Setup_Logger(logfile, loglevel, logger_name="main", Enable_Color=True)


def run_pipeline(args):
    """
    The “real” main workflow (no argparse here).
    """
    codestart = time.time()

    # 先读配置（不依赖 logger）
    casecfg = Read_Config('case.ini')
    envcfg  = Read_Config('env.ini')

    if args.confighelp:
        Tools.Print_Config_Help()
        return 0

    if args.listcases:
        Print_Useful_Cases(casecfg)
        return 0

    gridname = args.collectcase if args.collectcase else args.gridname

    if not gridname:
        print("Please provide a case name using -n or --gridname")
        return 2

    if args.year is not None:
        casecfg, gridname = Modify_Config(casecfg, gridname, year=args.year)

    # Setup logger
    cur_dir = os.getcwd()
    logfile = f'{cur_dir}/DataPrepare.{gridname}.log'
    loglevel = logging.DEBUG if args.debug else logging.INFO
    logger = Setup_Logger(logfile, loglevel, logger_name="CRESMPrep", enable_color=True)

    # Collect all cases with the same prefix
    if args.collectcase:
        Collect_Yearly_Data(casecfg, envcfg, args.collectcase)
        elapsed = int(time.time() - codestart)
        t = time.gmtime(elapsed)
        days = t.tm_yday - 1
        logger.info(f"Elapsed time: {days:02d}:{t.tm_hour:02d}:{t.tm_min:02d}:{t.tm_sec:02d}")
        sys.exit(0)

    # 建议所有模块也使用 logging.getLogger("CRESMPrep."+__name__)
    main_logger = logging.getLogger("CRESMPrep.main")

    main_logger.info('')
    main_logger.info('      *************************************')
    main_logger.info('       *** CRESM Preprocessing System ***  ')
    main_logger.info('      *************************************\n\n')
    main_logger.info(f'       Case Name: {gridname} \n\n')

    main_logger.info('Reading configuration file...')
    Check_AllConfig(casecfg, envcfg, gridname, logging.INFO)

    main_logger.info('Making directory...')
    Make_Dirs(casecfg, envcfg, gridname)

    main_logger.info('Modifying namelist files...')
    Modify_CWPSNML(casecfg, envcfg, gridname)
    Modify_CWRFNML(casecfg, envcfg, gridname)
    Modify_CRESMNML(casecfg, envcfg, gridname)
    Modify_CFNML(casecfg, envcfg, gridname)
    Modify_CoLMNML(casecfg, envcfg, gridname, run_type='ICBC')
    Modify_CoLMNML(casecfg, envcfg, gridname, run_type='RUN')
    main_logger.info(f'{Consts.S4}✓  Modify namelist files finished!\n\n')

    # =========> PrepCWRF <==========
    main_logger.info('[PrepCWRF] *** Show case Domain Info ***')
    Show_Domain(casecfg, envcfg, gridname)

    if not args.geogdir:
        main_logger.info('[PrepCWRF] *** Making Static Data ***')
        PrepCWRF.First_StaticData(casecfg, envcfg, gridname)
    else:
        main_logger.info('[PrepCWRF] *** Using Existing Geog Data ***')
        PrepCWRF.Copy_Exist_GeogData(casecfg, envcfg, gridname, args.geogdir)

    main_logger.info('[PrepCWRF] *** Making ICBC ***')
    PrepCWRF.Second_ICBC(casecfg, envcfg, gridname)

    main_logger.info('[PrepCWRF] *** Copying CWRF Result ***')
    PrepCWRF.Gather_CWRF_Output(casecfg, envcfg, gridname)

    # =========> PrepCoLM <==========
    if not args.colmsrf:
        main_logger.info('[PrepCoLM] *** Making Mesh grid file ***')
        PrepCoLM.First_GenMesh(casecfg, envcfg, gridname)
        main_logger.info('[PrepCoLM] *** Making CoLM Surface data ***')
        PrepCoLM.Second_MakeSrf(casecfg, envcfg, gridname)
    else:
        main_logger.info('[PrepCoLM] *** Using existing CoLM Surface data ***')
        PrepCoLM.Copy_Exist_CoLMSrf(casecfg, envcfg, gridname, args.colmsrf)

    main_logger.info('[PrepCoLM] *** Make CoLM Initial file ***')
    PrepCoLM.Second_CoLMIni(casecfg, envcfg, gridname)

    main_logger.info('[PrepCoLM] *** CoLM Run file ***')
    PrepCoLM.Second_CoLMRun(casecfg, envcfg, gridname)

    main_logger.info('[PrepCoLM] *** Remap CoLM History ***')
    PrepCoLM.Third_Remap(casecfg, envcfg, gridname)

    main_logger.info('[PrepCoLM] *** Copying CoLM Result ***')
    PrepCoLM.CopyPrepCoLMResult(casecfg, envcfg, gridname)

    # =========> PrepCRESM <==========
    main_logger.info('[PrepCRESM] *** Making CRESM data ***')
    PrepCRESM.Coupler_Prep(casecfg, envcfg, gridname)

    main_logger.info('[GatherData] *** Copying Result ***')
    Gather_Prepare_Data(casecfg, envcfg, gridname)

    main_logger.info('[PostProc] *** Cleaning Temporary Files ***')
    Clean_Temporary_Files(casecfg, envcfg, gridname)

    elapsed = int(time.time() - codestart)
    t = time.gmtime(elapsed)
    days = t.tm_yday - 1
    main_logger.info('')
    main_logger.info('      *********************************')
    main_logger.info('        *** Successfully Finished ***  ')
    main_logger.info('      *********************************\n')
    main_logger.info(f"Elapsed time: {days} days {t.tm_hour} hours {t.tm_min} minutes {t.tm_sec} seconds")
    return 0



def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        return run_pipeline(args)
    except KeyboardInterrupt:
        # 注意：这里可能还没 setup logger，所以用 print 保底
        print("Aborted by user (Ctrl+C).")
        return 130
    except Exception:
        # 尽量记录 traceback（如果 logger 已 setup）
        log = logging.getLogger("CRESMPrep.main")
        if log.handlers:
            log.exception("Fatal error occurred.")
        else:
            # logger 未配置时的保底输出
            import traceback
            traceback.print_exc()
        return 1



if __name__ == "__main__":
    sys.exit(main())

