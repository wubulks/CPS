#! /stu01/wumej22/Anaconda3/bin/python
# -*- coding: utf-8 -*-

"""
===============================================================================
Module Name   : Utils.Tools
Description   : Core utility library for CRESM Data Preparation.
                Contains generic functions used across all modules.

                Key Features:
                - Run_CMD         : Robust shell command execution wrapper.
                - File/Dir Ops    : Safe copy, check existence, make dirs.

Author        : Omarjan @ SYSU
Created       : 2025-05-25
Last Modified : 2026-01-21
===============================================================================
"""

import os
import re
import sys
import glob
import shutil
import logging
import tempfile
import subprocess
import pandas as pd
import numpy as np
from pathlib import Path
import cartopy.crs as ccrs
from datetime import timedelta, datetime
from concurrent.futures import ProcessPoolExecutor, as_completed
from .Logger import Log_Redirect_Tail
from . import Consts as Consts

logger = logging.getLogger("CRESMPrep." + __name__)



def Run_CMD(cmd, description=None, env=None):
    """
    Execute shell command with optional environment source.
    Uses interactive bash shell (-i) to ensure proper environment loading.
    """
    if description:
        logger.debug(description)
    
    # 检查环境文件是否存在
    if Consts.UseExternalEnv and env:
        if not os.path.exists(env):
            logger.error(f"Environment file not found: {env}")
            raise FileNotFoundError(f"Environment file not found: {env}")
        logger.debug(f"Sourcing environment: {env}")
    
    # 构建最终命令 - 使用bash -i -c确保交互式shell模式
    if Consts.UseExternalEnv and env:
        # 使用交互式shell模式，这更接近您在终端中的操作
        final_cmd = f"bash -i -c 'source {env} && {cmd}'"
    else:
        final_cmd = cmd
    
    logger.debug(f"Executing command: {final_cmd}")
    
    try:
        # 执行命令
        result = subprocess.run(
            final_cmd,
            shell=True,
            executable="/bin/bash",
            text=True,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        
        # 记录输出（如果输出太长，只显示前几行）
        if result.stdout:
            output_lines = result.stdout.strip().split('\n')
            if len(output_lines) > 10:
                logger.debug(f"Command output (first 10 lines):")
                for line in output_lines[:10]:
                    logger.debug(f"  {line}")
                logger.debug(f"  ... and {len(output_lines) - 10} more lines")
            else:
                logger.debug("Command output:")
                for line in output_lines:
                    logger.debug(f"  {line}")
        
        logger.debug(f"Command executed successfully (exit code: {result.returncode})")
        return result
        
    except subprocess.CalledProcessError as e:
        logger.error(f"Command failed with exit code {e.returncode}: {final_cmd}")
        
        # 处理错误输出
        if e.stderr:
            stderr = e.stderr.strip() if e.stderr else "<no stderr captured>"
            logger.error("Error output:")
            for line in stderr.splitlines():
                logger.error("    " + line)
        
        # 处理重定向日志
        if ">" in cmd:
            try:
                Log_Redirect_Tail(logger, cmd)
            except:
                pass
        
        logger.error(f"Error at: {os.getcwd()}")
        raise



def File_Exist(filepath, level=None, count=None):
    """
    检查文件是否存在，支持通配符，并可选检查匹配到的文件总数。

    Args:
        filepath (str | os.PathLike | list[str | os.PathLike]):
            文件路径、Path对象，或它们组成的列表。
        level (str): 报错级别 ("error", "warning", "info")。
        count (int, optional): 期望匹配到的文件总数。如果匹配数不符，将触发警告/错误。
    """

    def _Handle_Error(msg):
        if level == "error":
            logger.error(msg)
            raise FileNotFoundError(msg)
        elif level == "warning":
            logger.warning(f"{Consts.S4}{msg}")
        else:
            logger.debug(f"{Consts.S4}{msg}")

    def _to_path_str(p):
        if isinstance(p, (str, os.PathLike)):
            return os.fspath(p)   # 可同时支持 str 和 Path
        raise TypeError(f"Each filepath must be str or os.PathLike, got {type(p)}")

    # 1. 统一输入为列表
    if isinstance(filepath, (str, os.PathLike)):
        paths_to_check = [_to_path_str(filepath)]
    elif isinstance(filepath, list):
        paths_to_check = [_to_path_str(p) for p in filepath]
    else:
        raise TypeError("filepath must be str, os.PathLike, or list")

    if not paths_to_check:
        _Handle_Error("File list is empty.")
        return False

    # 2. 逐项检查
    for p in paths_to_check:
        matched_files = glob.glob(p)
        actual_count = len(matched_files)

        # 检查项 A: 基础存在性检查
        if actual_count == 0:
            _Handle_Error(f"File or Pattern not found: {p}")
            return False

        # 检查项 B: 可选的文件总数检查
        if count is not None and actual_count != count:
            _Handle_Error(
                f"File count mismatch for pattern [{p}]: "
                f"Expected {count}, but found {actual_count}."
            )
            return False

        # 日志记录
        if "*" in p or "?" in p:
            logger.debug(f"Pattern matched {actual_count} files: {p}")
        else:
            logger.debug(f"File exists: {p}")

    return True

def Link(src_in, dst_in, force=True):
    """
    Create symbolic link (ln -sf).
    """
    # Check source using File_Exist
    
    def _Link_src(src, dst):
        if not File_Exist(src, level='error'):
         # This block is technically unreachable if level='error' raises exception,
         # but kept for logical completeness or if behavior of File_Exist changes.
         return
        # 2. 构造命令
        # -s: symbolic, -f: force
        # 如果 src 包含通配符，shell=True 会自动处理它
        cmd = f"ln -sf {src} {dst}"
        
        try:
            Run_CMD(cmd, description=f"Linking {src} to {dst}")
        except Exception as e:
            logger.error(f"Failed to create link via shell: {e}")
            raise
        logger.debug(f"Link created: {dst} -> {src}")

    if isinstance(src_in, str):
        src_abs = os.path.abspath(src_in)
        dst_abs = os.path.abspath(dst_in)
        _Link_src(src_abs, dst_abs)

    elif isinstance(src_in, list):
        for s in src_in:
            src_abs = os.path.abspath(s)
            dst_abs = os.path.abspath(dst_in)
            _Link_src(src_abs, dst_abs)


def Copy(src, dst, overwrite=True):
    """
    Copy file(s) or directory(s).
    Supports wildcard patterns (glob).

    - file: shutil.copy2
    - dir : shutil.copytree
    """

    # Expand wildcard
    if isinstance(src, str):
        src_list = glob.glob(src)
    elif isinstance(src, list):
        src_list = []
        for s in src:
            src_list.extend(glob.glob(s))

    if not src_list:
        logger.error(f"No source matched: {src}")
        raise FileNotFoundError(f"No source matched: {src}")

    # If multiple sources -> dst must be directory
    multi_src = len(src_list) > 1

    if multi_src:
        os.makedirs(dst, exist_ok=True)

    for src_item in src_list:

        # Decide target path
        if multi_src or os.path.isdir(dst):
            dst_item = os.path.join(dst, os.path.basename(src_item))
        else:
            dst_item = dst

        # Ensure parent directory exists
        dst_dir = os.path.dirname(dst_item)
        if dst_dir and not os.path.exists(dst_dir):
            os.makedirs(dst_dir, exist_ok=True)

        # Handle overwrite
        if os.path.exists(dst_item):
            if overwrite:
                logger.debug(f"Removing existing destination: {dst_item}")
                if os.path.isfile(dst_item) or os.path.islink(dst_item):
                    os.remove(dst_item)
                else:
                    shutil.rmtree(dst_item)
            else:
                logger.error(f"Destination already exists: {dst_item}")
                raise FileExistsError(f"Destination already exists: {dst_item}")

        # Copy operation
        if os.path.isfile(src_item) or os.path.islink(src_item):
            shutil.copy2(src_item, dst_item)
            logger.debug(f"File copied: {src_item} -> {dst_item}")

        elif os.path.isdir(src_item):
            shutil.copytree(src_item, dst_item)
            logger.debug(f"Directory copied: {src_item} -> {dst_item}")

        else:
            logger.error(f"Unsupported source type: {src_item}")
            raise ValueError(f"Unsupported source type: {src_item}")


def Split_Days(start, end, parts):
    """
    Split [start, end] into parts by day.
    """
    start = start.replace(minute=0, second=0, microsecond=0)
    end   = end.replace(minute=0, second=0, microsecond=0)

    total = (end.date() - start.date()).days + 1
    base  = total // parts
    extra = total % parts

    edges = [start]

    for i in range(parts - 1):
        step = base + (1 if i < extra else 0)
        edges.append(edges[-1] + timedelta(days=step))

    edges.append(end + timedelta(hours=12))

    return [(edges[i], edges[i + 1]) for i in range(parts)]


def Get_Forc_File_Path(path, date):
    """
    Replace forcing path placeholders.
    """
    return (path.replace("<YYYY>", str(date.year))
                .replace("<MM>", f"{date.month:02d}")
                .replace("<DD>", f"{date.day:02d}")
                .replace("<HH>", f"{date.hour:02d}"))


def Check_Ungrib_Finish(path, prefix, interval, start_time, end_time):
    """
    Check ungrib output completeness.
    """
    for itime in pd.date_range(start=start_time, end=end_time, freq=f"{interval}h"):
        file_path = f"{path}/{prefix}:{itime.strftime('%Y-%m-%d_%H')}"
        File_Exist(file_path, level="error")


def Check_Metgrid_Finish(path, prefix, interval, start_time, end_time):
    """
    Check metgrid output completeness.
    """
    for itime in pd.date_range(start=start_time, end=end_time, freq=f"{interval}h"):
        file_path = f"{path}/{prefix}.{itime.strftime('%Y-%m-%d_%H:%M:%S')}.nc"
        File_Exist(file_path, level="error")


def Extract_Dates_From_String(raw):
    """
    Robust extraction of date strings.
    """
    if raw is None:
        return pd.DatetimeIndex([])

    cleaned = (raw.replace("\r", " ")
                    .replace("\n", " ")
                    .replace("\t", " ")
                    .replace("\u00A0", " "))

    tokens = re.findall(
        r"\d{4}-\d{2}-\d{2}(?:[ T]\d{2}:\d{2}(?::\d{2})?)?",
        cleaned
    )

    if not tokens:
        return pd.DatetimeIndex([])

    try:
        return pd.to_datetime(tokens, format="mixed", errors="raise")
    except TypeError:
        return pd.to_datetime(tokens, errors="raise")



def Get_Area_MaxMin_Coords(config, gridname):
    """
    Get the area max and min coordinates from the configuration file
    """
    RefLat = config.getfloat(gridname, 'RefLat')
    RefLon = config.getfloat(gridname, 'RefLon')
    True_Lat1 = config.getfloat(gridname, 'True_Lat1')
    True_Lat2 = config.getfloat(gridname, 'True_Lat2')
    dx_WE = config.getint(gridname, 'dx_WE')
    dy_SN = config.getint(gridname, 'dy_SN')
    EdgeNum_WE = config.getint(gridname, 'EdgeNum_WE')
    EdgeNum_SN = config.getint(gridname, 'EdgeNum_SN')
    
    proj = ccrs.LambertConformal(
        central_longitude=RefLon,
        central_latitude=RefLat,
        standard_parallels=(True_Lat1, True_Lat2),
    )
    # 1. 计算外框四角（Lambert 坐标）
    half_we = (EdgeNum_WE - 1) / 2 * dx_WE
    half_sn = (EdgeNum_SN - 1) / 2 * dy_SN
    x0, y0 = -half_we, -half_sn
    x1, y1 = x0 + (EdgeNum_WE - 1) * dx_WE, y0 + (EdgeNum_SN - 1) * dy_SN
    
    # 3. 计算整个边界的经纬度坐标来找到真正的最大最小值
    geo = ccrs.PlateCarree()
    # 采样密度（每边取多少个点）
    n_samples = 100
      
    # 四条边界线的Lambert坐标
    # 底边：从左下到右下
    x_bottom = np.linspace(x0, x1, n_samples)
    y_bottom = np.full(n_samples, y0)
    
    # 顶边：从左上到右上  
    x_top = np.linspace(x0, x1, n_samples)
    y_top = np.full(n_samples, y1)
    
    # 左边：从左下到左上
    x_left = np.full(n_samples, x0)
    y_left = np.linspace(y0, y1, n_samples)
    
    # 右边：从右下到右上
    x_right = np.full(n_samples, x1)
    y_right = np.linspace(y0, y1, n_samples)
    
    # 合并所有边界点
    x_boundary = np.concatenate([x_bottom, x_top, x_left, x_right])
    y_boundary = np.concatenate([y_bottom, y_top, y_left, y_right])
    
    # 转换为经纬度
    boundary_wgs = geo.transform_points(proj, x_boundary, y_boundary)
    lons_boundary = boundary_wgs[:, 0]
    lats_boundary = boundary_wgs[:, 1]
    
    # 计算真正的经纬度范围
    maxmin_wgs = {
        "min_lon": lons_boundary.min(),
        "max_lon": lons_boundary.max(),
        "min_lat": lats_boundary.min(),
        "max_lat": lats_boundary.max(),
    }

    return maxmin_wgs



def Get_Unique_GeogID(casecfg, envcfg, gridname):
    """
    Get the unique geog ID from the configuration file
    """
    EdgeNum_WE = casecfg.getint(gridname, 'EdgeNum_WE')
    EdgeNum_SN = casecfg.getint(gridname, 'EdgeNum_SN')
    dx_WE = casecfg.getfloat(gridname, 'dx_WE')
    dy_SN = casecfg.getfloat(gridname, 'dy_SN')
    RefLat = casecfg.getfloat(gridname, 'RefLat')
    RefLon = casecfg.getfloat(gridname, 'RefLon')
    True_Lat1 = casecfg.getfloat(gridname, 'True_Lat1')
    True_Lat2 = casecfg.getfloat(gridname, 'True_Lat2')
    StandLon = casecfg.getfloat(gridname, 'StandLon')
    BdyWidth = casecfg.getint(gridname, 'BdyWidth')
    LakeThreshold = casecfg.getfloat(gridname, 'LakeThreshold')

    # Generate a unique ID based on the parameters
    GeogID = (
        f"X[{EdgeNum_WE:d}]_Y[{EdgeNum_SN:d}]"           # Edge number
        f"_dx[{dx_WE:.6f}]_dy[{dy_SN:.6f}]"              # resolution
        f"_lat[{RefLat:.6f}]_lon[{RefLon:.6f}]"          # reference lat/lon
        f"_tl1[{True_Lat1:.6f}]_tl2[{True_Lat2:.6f}]"    # true lat1/lat2
        f"_slon[{StandLon:.6f}]"                       # standard longitude
        f"_bw[{BdyWidth:d}]"                           # boundary width
        f"_lk[{LakeThreshold:.6f}]"                    # lake threshold
    )
    return GeogID



def Get_Unique_CoLMSrfID(casecfg, envcfg, gridname):
    """
    Get the unique CoLMSrf ID from the configuration file
    """
    CoLMModelPath = envcfg.get('Paths', 'CoLMModelPath')

    define_file = os.path.join(CoLMModelPath, 'include', 'define.h')
    File_Exist(define_file, level='error')
    define_option = macros_as_bracketed_tokens(define_file)

    GeogID = Get_Unique_GeogID(casecfg, envcfg, gridname)

    CoLMSrfID = f"{GeogID}_|_{define_option}"

    return CoLMSrfID



def macros_as_bracketed_tokens(src):
    """
    提取所有出现过的宏开关名（#define / #undef 后的标识符），去重后输出为:
      _[MACRO1]_[MACRO2]_...[MACROn]_
    其中每个宏名用 _[ ]_ 包裹，便于区分。
    """
    p = Path(src) if isinstance(src, (str, Path)) else None
    if p is not None and p.exists() and p.is_file():
        text = p.read_text(encoding="utf-8", errors="ignore")
    else:
        text = str(src)

    macro_re = re.compile(r'^\s*#\s*(define|undef)\s+([A-Za-z_]\w*)\b')
    names = set()

    for raw_line in text.splitlines():
        line = raw_line.rstrip()

        # 跳过 Fortran 注释整行
        if line.lstrip().startswith("!"):
            continue

        # 去掉行内 Fortran 注释（若存在）
        if "!" in line:
            line = line.split("!", 1)[0].rstrip()

        m = macro_re.match(line)
        if m:
            names.add(m.group(2))

    # 你要用 _[define option]_ 区分；这里按字母排序以保证结果稳定
    return "".join(f"_[{name}]_" for name in names)



def Build_SinGridList_From_MaxMinWGS(maxmin_wgs, Expand_Deg=2, Return_String=False):
    """
    Build MODIS Sinusoidal tile list from lon/lat bounding box (WGS84).
    Expand_Deg : float
        Expand bbox by degrees to avoid missing boundary tiles. Recommended 0.1~0.25.
    Return_String : bool
        If True, return "hXXvYY,..." string; else return list[str].
    Returns
    -------
    list[str] or str
    """

    import math

    logger.debug(f"Original WGS bbox: {maxmin_wgs}")
    logger.debug(f"Expanded by {Expand_Deg} degrees: ")
    for key, value in maxmin_wgs.items():
        logger.debug(f"  {key}: {value}")

    # ---- Read + expand ----
    min_lon = float(maxmin_wgs["min_lon"]) - Expand_Deg
    max_lon = float(maxmin_wgs["max_lon"]) + Expand_Deg
    min_lat = float(maxmin_wgs["min_lat"]) - Expand_Deg
    max_lat = float(maxmin_wgs["max_lat"]) + Expand_Deg

    # ---- Clamp to valid WGS range ----
    min_lon = max(-180.0, min(180.0, min_lon))
    max_lon = max(-180.0, min(180.0, max_lon))
    min_lat = max(-90.0,  min(90.0,  min_lat))
    max_lat = max(-90.0,  min(90.0,  max_lat))

    if min_lon > max_lon or min_lat > max_lat:
        raise ValueError(f"Invalid bbox after clamp: {maxmin_wgs}")

    # ---- MODIS tile index rules (36x18, 10-degree) ----
    # h: 0..35, v: 0..17
    h_min = int(math.floor((min_lon + 180.0) / 10.0))
    h_max = int(math.floor((max_lon + 180.0) / 10.0))
    v_min = int(math.floor((90.0 - max_lat) / 10.0))
    v_max = int(math.floor((90.0 - min_lat) / 10.0))

    # ---- Clamp to tile index range ----
    h_min = max(0, min(35, h_min))
    h_max = max(0, min(35, h_max))
    v_min = max(0, min(17, v_min))
    v_max = max(0, min(17, v_max))

    tiles = []
    for v in range(v_min, v_max + 1):
        for h in range(h_min, h_max + 1):
            tiles.append(f"h{h:02d}v{v:02d}")

    # ---- Safety ----
    if not tiles:
        raise RuntimeError(f"Empty SinGridList computed from bbox: {maxmin_wgs}")

    # deterministic ordering already ensured by loops (v then h)
    if Return_String:
        return ",".join(tiles)

    return tiles



def Run_Parallel(func, args_list, workers, label="Parallel Task"):
    """
    通用并行执行封装
    Args:
        func: 要并行调用的函数
        args_list: 参数列表，每个元素应为元组 (tuple)，对应 func 的参数
        workers: 并行进程数
        label: 任务标签，用于日志打印
    """
    future_to_arg = {}
    with ProcessPoolExecutor(max_workers=workers) as executor:
        # 1. 提交所有任务
        for args in args_list:
            # 如果 args 不是元组，自动包装一下
            if not isinstance(args, (tuple, list)):
                future = executor.submit(func, args)
            else:
                future = executor.submit(func, *args)
            future_to_arg[future] = args
        try:
            # 2. 迭代已完成的任务
            for future in as_completed(future_to_arg):
                arg = future_to_arg[future]
                try:
                    future.result()  # 检查子进程是否抛出异常
                except Exception as exc:
                    # 3. 打印错误信息
                    logger.error("-" * 60)
                    logger.error(f"[{label}] Failed!")
                    logger.error(f"  - Arguments: {arg}")
                    logger.error(f"  - Error Detail: {exc}")
                    logger.error("-" * 60)
                    # 4. 尝试取消排队中的其他任务
                    for f in future_to_arg:
                        if not f.done():
                            f.cancel()
                    # 5. 立即抛出异常终止主程序
                    raise RuntimeError(f"{label} failed during parallel execution.") from exc
        except Exception:
            # 确保异常能传递到主进程，触发 executor 的清理逻辑
            raise
    return True



def rename_tree_tokens(root_dir: str, old_token: str, new_token: str, logger=None):
    """
    递归重命名：把 root_dir 下所有文件/目录名中包含 old_token 的部分替换为 new_token。
    采用 bottom-up，避免先改父目录导致子路径失效。
    """
    root = Path(root_dir)
    if not root.exists():
        return

    # bottom-up：深的先改
    all_paths = sorted(root.rglob("*"), key=lambda p: len(p.parts), reverse=True)

    for p in all_paths:
        # 跳过不存在（可能前面已经被重命名影响）
        if not p.exists():
            continue
        name = p.name
        if old_token not in name:
            continue

        new_name = name.replace(old_token, new_token)
        new_p = p.with_name(new_name)

        # 冲突处理：若目标名已存在，跳过并告警（避免覆盖）
        if new_p.exists():
            if logger:
                logger.warning(f"[Rename] Skip conflict: {p} -> {new_p} (target exists)")
            continue

        try:
            p.rename(new_p)
            if logger:
                logger.info(f"[Rename] {p} -> {new_p}")
        except Exception as e:
            if logger:
                logger.warning(f"[Rename] Failed: {p} -> {new_p}, err={e}")



def Print_Config_Help():
    """
    Print optimized help information for configuration files.
    Supports 'rich' library for beautified output, falls back to plain text if not installed.
    """
    import sys

    # 尝试导入 rich 库
    try:
        from rich.console import Console
        from rich.table import Table
        from rich.text import Text
        from rich.align import Align
        from rich.padding import Padding
        from rich.panel import Panel
        from rich import box
        has_rich = True
    except ImportError:
        has_rich = False

    # ==========================================================================
    # 方案 A: 纯文本回退方案 (当用户没有安装 rich 时使用)
    # ==========================================================================
    if not has_rich:
        print("no rich")
        help_msg = """
================================================================================
               CRESM Preprocessing System (CPS) Configuration Help
================================================================================
Author: Omarjan @ SYSU
Version: v1.2.0

This tool relies on two configuration files:
  1. case.ini : Experiment workflow and domain settings (Varies per case).
  2. env.ini  : System paths and environmental settings (Fixed per machine).


--------------------------------------------------------------------------------
PART 1: case.ini (Experiment Configuration)
--------------------------------------------------------------------------------

[BaseInfo]  -- Global Workflow Settings
  CleanTempFiles           : If True, deletes intermediate files to save disk space.
                             [!] CAUTION: Hard to debug if enabled.
  Use_CoLMLAI              : If True, uses CoLM's LAI data instead of default MODIS.
  Enable_TimeChunk         : Enable time-splitting for long simulations.
  TimeChunkCount           : Number of chunks to split the run into (suggest 4-10).

[PrepCWRF]  -- CWRF Preprocessing Switches
  CWRFCoreNum              : CPU cores for CWRF MPI tasks (suggest 4-24).
  Go_ShowDomain            : Plot domain preview (check domain coverage).
  Go_Geogrid               : Run Geogrid (Generate static geographical data).
  Go_FVC / LAI / SAI       : Switches for specific vegetation parameter processing.
  Collect_GeogData         : Collect required geog data files.
  Go_Ungrib                : Run Ungrib (Decode GRIB forcing data, e.g., ERA5).
  Go_Metgrid               : Run Metgrid (Interpolate met data to model grid).
  Go_Real                  : Run Real (Generate wrfinput/wrfbdy).
  Go_VBS                   : Run VBS (Vegetation/Albedo processing).
  Copy_CWRF_Output         : Copy final CWRF inputs to case dir.

[PrepCoLM]  -- CoLM Preprocessing Switches 
  CoLMCoreNum              : CPU cores for CoLM MPI tasks.
  Go_MeshGrid              : Generate unstructured mesh mapping files for CoLM and CWRF.
  Go_MakeSrf               : Generate surface datasets (mksrf).
  Go_MakeIni               : Generate initial conditions (mkinidata).
  Go_CoLMTempRun           : Perform a temp run for surface spin-up.
  Go_Remap                 : Remap history/restart files.
  Copy_CoLM_Output         : Copy final CoLM inputs to case dir.

[PrepCRESM] -- Coupler weight file
  Go_Coupler_Prep          : Generate weights and maps for the coupler.

[GatherData] -- Final Output Collection
  Collect_CWRF_Output      : Gather final CWRF inputs.
  Collect_CoLM_Output      : Gather final CoLM inputs.
  Collect_CRESM_Output     : Gather Coupler namelists and mapping files.

[<GridName>] -- Domain & Experiment Specifics (e.g., [Yangtze_6km])
  CaseOutputPath           : Root directory to store case output files.
  ForcingDataName          : name of forcing data (Must match a section in env.ini).
                             should be one of: CFSV2, ERA5, MPI-ESM1-2-HR_ssp245 etc.
  StartTime                : Format YYYY-MM-DD_HH:MM:SS (e.g., 2021-01-01_00:00:00).
  EndTime                  : Format YYYY-MM-DD_HH:MM:SS (e.g., 2021-12-31_23:59:59).
  EdgeNum_WE               : Grid points+1 West-East.
  EdgeNum_SN               : Grid points+1 South-North.
  dx_WE                    : Grid resolution West-East (meters).
  dy_SN                    : Grid resolution South-North (meters).
  RefLat                   : Center latitude of the domain.
  RefLon                   : Center longitude of the domain.
  True_Lat1                : True latitude 1 for Lambert projection.
  True_Lat2                : True latitude 2 for Lambert projection.
  StandLon                 : Standard longitude for Lambert projection.
  BdyWidth                 : Lateral boundary relaxation width (Sponge layer).
                             [!] Must be ODD.
  LakeThreshold            : Fraction (0.0-1.0). Grid cells with lake fraction
                             greater than this value are treated as lake points.
  MeshSize                 : Resolution level for CoLM raw surface database.
                             1=Coarse (21600*43200), 2=Medium, 3=Fine (86400*172800).


--------------------------------------------------------------------------------
PART 2: env.ini (Environment Configuration)
--------------------------------------------------------------------------------
[Environment]
SYS_CWRF                : Path to the environment setup script (to be sourced) for CWRF execution (e.g., /home/user/.cresm).
SYS_CoLM                : Path to the environment setup script (to be sourced) for CoLM execution. (e.g., /home/user/.bashrc_CoLM202X_gnu).
CONDA_XESMF             : Conda environment name for XESMF remapping (e.g., 'cresm_xesmf').
CONDA_CHAO              : Conda environment name for Chaomodis tools (e.g., 'Chaomodis').
CONDA_UNGRIB            : Conda environment name for Ungrib tools (e.g., 'ungrid').

[Paths] -- Core System Paths
  ScriptPath               : Absolute path to the 'PrepScript' folder.
  
  ; CoLM Model & Data
  CoLMModelPath            : Path to CoLM source code/compiled model root.
  CoLMRawDataPath          : Path to CoLM raw geographical/soil datasets.
  CoLMRunDataPath          : Path to CoLM runtime data directory.
  CoLMForcingPath          : Path to CoLM forcing data (e.g., ERA5-Land).

  ; ToolBox & Static Data (Can use ${Paths:RootToolBox} variable)
  RootToolBox              : Root directory of CRESM Data Prep Toolbox.
  CWPSPath                 : Path to CWRF-CWPS tool.
  CWRFToolPath             : Path to CWRF specific tools.
  GeogDataPath             : Path to CWRF Geogrid binary data (geog_wm_modified_lake).
  CWPSStaticPath           : Path to CWPS static tables/files.
  GlobalLakeDepth          : Path to Global Lake Depth data file (.dat).
  GlobalLakeStatus         : Path to Global Lake Status data file (.dat).
  WMEJUngrib               : Path to WMEJ_NC2IM tool.
  WMEJModis                : Paths to CoLM LAI data processing tools.
  ChaoModis                : Paths to MODIS processing tools.

  ; External Programs
  NCOPath                  : Path to 'ncks' executable (e.g., /usr/bin/ncks).
  CDOPath                  : Path to 'cdo' executable.
  NCLPath                  : Path to 'ncl' executable.

[<ForcingName>] -- Forcing Data Source Configuration (e.g., [era5])
  ForcingDataName          : name of the forcing (Must match 'ForcingDataName' in case.ini).
  Forc_Info                : Path to forcing info file (or None), only needs for NC data.
  Forc_2D_Path             : Path to 2D forcing files (Raw GRIB/NC).
  Forc_3D_Path             : Path to 3D forcing files.
  Forc_SST_Path            : Path to SST forcing files.
  * Note: You can define multiple forcing sections (e.g., [cfsv2], [gfs]).

================================================================================
        """
        print(help_msg)
        sys.exit(0)

    # ==========================================================================
    # 方案 B: Rich 美化方案 (限制长度 + 左对齐)
    # ==========================================================================
    MAX_WIDTH = 100
    INDENT_OVERVIEW = 10
    INDENT_TABLE = 4
    console = Console()

    # =========================
    # 1) 通用：短 Rule（不铺满终端）
    # =========================
    def short_rule(
        title: str = "",
        width: int = MAX_WIDTH,
        align: str = "center",         # "left" / "center" / "right"
        char: str = "━",
        style: str = "magenta",
        title_style: str = "bold magenta",
    ):
        """
        生成“短 Rule”效果：严格限制宽度，不铺满终端。
        """
        title = title.strip()
        if title:
            t = f" {title} "
        else:
            t = ""

        # 纯线
        if not t:
            line = char * width
            console.print(f"[{style}]{line}[/]")
            return

        # 线 + 标题 + 线
        if len(t) >= width - 2:
            # 标题太长则直接输出标题，不强行画线
            console.print(Text(title, style=title_style))
            return

        line_len = width - len(t)
        left = line_len // 2
        right = line_len - left

        if align == "left":
            # 标题靠左：标题后补线
            left = 2
            right = width - len(t) - left
        elif align == "right":
            # 标题靠右：标题前补线
            right = 2
            left = width - len(t) - right

        console.print(
            f"[{style}]{char * left}[/]"
            f"[{title_style}]{t}[/]"
            f"[{style}]{char * right}[/]"
        )

    # =========================
    # 3) Overview：可右缩进（宽度=100）
    # =========================
    def print_overview(
        author="Omarjan @ SYSU",
        version="v1.0.0",
        date="2025-05-25",
        envs=None,
    ):
        if envs is None:
            envs = ["cresm", "xesmf", "Chaomodis"]

        body = (
            f"[bold]Author : {author}[/]\n"
            f"[bold]Version: {version}[/]\n"
            f"[bold]Date   : {date}[/]\n"
            f"[bold]Conda  : {', '.join(envs)}[/]\n\n"
            "[bold]This tool relies on two configuration files:[/]\n"
            "1. [blue]case.ini[/] : Experiment workflow & domain settings.\n"
            "2. [green]env.ini[/]  : System paths & environments."
        )

        panel = Panel(
            body,
            title="[magenta]Overview[/]",
            border_style="magenta",
            box=box.SQUARE,
            padding=(0, 1),
            width=MAX_WIDTH - INDENT_OVERVIEW,
        )

        console.print(Padding(panel, (0, 0, 0, INDENT_OVERVIEW)))


    # =========================
    # 4) PART：用短 Rule（宽度=100，不铺满）
    # =========================
    def print_part(title: str, color: str = "blue", width: int = MAX_WIDTH):
        console.print()
        console.print()
        short_rule(title=title, width=width, style=color, title_style=f"bold white on {color}", align="left", char="—")


    # =========================
    # 5) Section Table：标题贴表格（无空行）
    # =========================
    def create_section_table(
        section_title,
        data,
        color = "cyan",
        width = MAX_WIDTH,
        indent_left = 4,
        captions=None,
        ):
        """
        关键点：
        1) section_title 放到 table.title，避免 Group(title, table) 产生“标题与表头间空行”
        2) title_justify="left" 强制标题左对齐（Rich 默认居中）
        """
        title = Text(section_title, style=f"bold {color}", justify="left")

        table = Table(
            title=title,
            title_justify="left",          # <-- 关键：强制左对齐
            title_style=f"bold {color}",
            box=box.HORIZONTALS,
            show_lines=False,
            padding=(0, 1),
            pad_edge=False,
            width=width - indent_left,
            collapse_padding=True,         # 可选：进一步压缩视觉空隙（建议保留）
            caption=captions,
            caption_justify="left",
            caption_style="dim"
        )

        table.add_column("Key", style=f"bold {color}", no_wrap=True)
        table.add_column("Type", style="magenta", no_wrap=True)
        table.add_column("Description", style="white", overflow="fold")

        for key, val_type, desc in data:
            table.add_row(key, val_type, desc)
        console.print()
        console.print(Padding(table, (0, 0, 0, indent_left)))


    # =========================
    # 6) 你的 Help 输出：统一宽度=100
    # =========================
    console.print()
    console.print()
    short_rule(title="CRESM Preprocessing System (CPS) Configuration Help", width=MAX_WIDTH, style="magenta", title_style="bold magenta", align="center", char="━")
    # print_banner("CRESM Preprocessing System (CPS) Configuration Help")

    # Overview：右缩进
    print_overview(
        author=Consts.author,
        version=Consts.version,
        date=Consts.last_modified,
    )

    # PART 1
    print_part("PART 1: case.ini (Experiment Configuration)", color="blue", width=MAX_WIDTH)

    data_base = [
        ("CleanTempFiles", "bool", "Delete intermediate files? [bold red][!] CAUTION[/]"),
        ("Use_CoLMLAI", "bool", "Use CoLM's LAI data instead of MODIS for CWRF."),
        ("Enable_TimeChunk", "bool", "Enable time-splitting for long simulations."),
        ("TimeChunkCount", "int", "Number of chunks (suggest 4-10)."),
    ]
    create_section_table("[BaseInfo]", data_base, color="blue", width=MAX_WIDTH, indent_left=4)

    data_cwrf = [
        ("CWRFCoreNum", "int", "MPI cores for CWRF tasks (suggest 4-24)."),
        ("Go_ShowDomain", "switch", "Plot domain preview (check coverage)."),
        ("Go_Geogrid", "switch", "Run Geogrid (Static geographical data)."),
        ("Go_FVC/LAI/SAI", "switch", "Vegetation parameter processing."),
        ("Collect_GeogData", "switch", "Collect required geog data files."),
        ("Go_Ungrib", "switch", "Run Ungrib (Decode GRIB/NC forcing)."),
        ("Go_Metgrid", "switch", "Run Metgrid (Interpolate met data)."),
        ("Go_Real", "switch", "Run Real (Generate wrfinput/wrfbdy)."),
        ("Go_VBS", "switch", "Run VBS (Vegetation/Albedo processing)."),
        ("Copy_CWRF_Output", "switch", "Copy final CWRF inputs to case dir."),
    ]
    create_section_table("[PrepCWRF]", data_cwrf, color="blue", width=MAX_WIDTH, indent_left=4)

    data_colm = [
        ("CoLMCoreNum", "int", "MPI cores for CoLM tasks."),
        ("Go_MeshGrid", "switch", "Generate unstructured mesh grid."),
        ("Go_MakeSrf", "switch", "Generate surface datasets (mksrf)."),
        ("Go_MakeIni", "switch", "Generate initial conditions (mkinidata)."),
        ("Go_CoLMTempRun", "switch", "Perform temp run for spin-up."),
        ("Go_Remap", "switch", "Remap history/restart files."),
        ("Copy_CoLM_Output", "switch", "Copy final CoLM inputs to case dir."),
    ]
    create_section_table("[PrepCoLM]", data_colm, color="blue", width=MAX_WIDTH, indent_left=4)

    data_cresm = [
        ("Go_Coupler_Prep", "switch", "Generate weights and maps for the coupler.")
    ]
    create_section_table("[PrepCRESM]", data_cresm, color="blue", width=MAX_WIDTH, indent_left=4)

    data_gather = [
        ("Collect_CWRF_Output", "switch", "Gather final CWRF inputs."),
        ("Collect_CoLM_Output", "switch", " Gather final CoLM inputs."),
        ("Collect_CRESM_Output", "switch", "Gather Coupler namelists and mapping files."),
    ]
    create_section_table("[GatherData]", data_gather, color="blue", width=MAX_WIDTH, indent_left=4)

    data_grid = [
        ("CaseOutputPath", "path", "Root directory to store case output files."),
        ("ForcingDataName", "str", "name of forcing data (Must match [env.ini])."),
        ("StartTime", "time", "Format YYYY-MM-DD_HH:MM:SS (e.g., 2021-01-01_00:00:00)."),
        ("EndTime", "time", "Format YYYY-MM-DD_HH:MM:SS (e.g., 2021-12-31_23:59:59)."),
        ("EdgeNum_WE", "int", "Grid points+1 West-East."),
        ("EdgeNum_SN", "int", "Grid points+1 South-North."),
        ("dx_WE", "int", "Resolution West-East (meters)."),
        ("dy_SN", "int", "Resolution South-North (meters)."),
        ("RefLat", "float", "Center latitude of the domain."),
        ("RefLon", "float", "Center longitude of the domain."),
        ("True_Lat1", "float", "True latitude 1 for Lambert projection."),
        ("True_Lat2", "float", "True latitude 2 for Lambert projection."),
        ("StandLon", "float", "Standard longitude for Lambert projection."),
        ("BdyWidth", "int", "Lateral boundary relaxation width (Sponge layer). [bold red][!] Must be ODD[/]."),
        ("LakeThreshold", "0.0-1.0", "Lake fraction threshold, greater than this value are treated as lake points."),
        ("MeshSize", "1/2/3", "Resolution level for CoLM raw surface database. 1=Coarse (21600*43200), 2=Medium, 3=Fine (86400*172800)."),
    ]
    create_section_table("[<GridName>] (e.g., [Yangtze_6km])", data_grid, color="blue", width=MAX_WIDTH, indent_left=4)

    # PART 2
    print_part("PART 2: env.ini (Environment Configuration)", color="green", width=MAX_WIDTH)

    data_envs = [
        ("SYS_CWRF", "path", "Path to CWRF environment setup script (to be sourced)."),
        ("SYS_CoLM", "path", "Path to CoLM environment setup script (to be sourced)."),
        ("CONDA_XESMF", "str", "Conda environment name for XESMF remapping."),
        ("CONDA_CHAO", "str", "Conda environment name for Chaomodis tools."),
        ("CONDA_UNGRIB", "str", "Conda environment name for Ungrib tools."),
    ]
    create_section_table("[Environment]", data_envs, color="green", width=MAX_WIDTH, indent_left=4)

    data_paths = [
        ("ScriptPath", "path", "Absolute path to 'PrepScript' folder."),
        ("CoLMModelPath", "path", "Path to CoLM source code/compiled model root."),
        ("CoLMRawDataPath", "path", "Path to CoLM raw geographical/soil datasets."),
        ("CoLMRunDataPath", "path", "Path to CoLM runtime data directory."),
        ("CoLMForcingPath", "path", "Path to CoLM forcing data (e.g., ERA5-Land)."),
        ("RootToolBox", "path", "Root directory of CRESM Data Prep Toolbox. "),
        ("CWPSPath", "path", "Path to CWRF-CWPS tool.(Supports ${var})."),
        ("CWRFToolPath", "path", "Path to CWRF specific tools.(Supports ${var})."),
        ("GeogDataPath", "path", "Path to CWRF Geogrid binary data (geog_wm_modified_lake).(Supports ${var})."),
        ("CWPSStaticPath", "path", "Path to CWPS static tables/files.(Supports ${var})."),
        ("GlobalLakeDepth", "path", "Path to Global Lake Depth data file (.dat).(Supports ${var})."),
        ("GlobalLakeStatus", "path", "Path to Global Lake Status data file (.dat).(Supports ${var})."),
        ("WMEJUngrib", "path", "Path to WMEJ_NC2IM tool."),
        ("WMEJModis", "path", "Paths to CoLM LAI data processing tools."),
        ("ChaoModis", "path", "Paths to MODIS processing tools."),
        ("NCOPath", "exe", "Path to 'ncks' executable"),
        ("CDOPath", "exe", "Path to 'cdo' executable."),
        ("NCLPath", "exe", "Path to 'ncl' executable."),
    ]
    create_section_table("[Paths]", data_paths, color="green", width=MAX_WIDTH, indent_left=4)

    data_forcing = [
        ("ForcingDataName", "str", "name of the forcing  (Matches case.ini)."),
        ("Forc_Info", "file", "Path to forcing info file (NC only)."),
        ("Forc_2D_Path", "dir", "Path to 2D forcing files (Raw GRIB/NC)."),
        ("Forc_3D_Path", "dir", "Path to 3D forcing files."),
        ("Forc_SST_Path", "dir", "Path to SST forcing files."),
    ]
    captions= '* Note: You can define multiple forcing sections (e.g., cfsv2, CWRF).'
    create_section_table("[<ForcingName>] (e.g., [era5])", data_forcing, color="green", width=MAX_WIDTH, indent_left=4,captions=captions)

    # Footer：短 Rule + End 文本（宽度=100）
    short_rule(title="End of Help", width=MAX_WIDTH, style="magenta", title_style="bold magenta", align="center", char="━")
    console.print()



