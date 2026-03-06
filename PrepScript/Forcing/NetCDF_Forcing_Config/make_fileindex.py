"""
make_fileindex.py — Final Robust Version
---------------------------------------
生成：
  1. forc_index.<dataname>.csv   —— 文件索引表（文件路径、变量、时间范围）
  2. miss_<var>.txt              —— 每个变量独立缺失时间文件（仅在确实存在缺失时生成）

特性：
  - 从命令行参数传入配置文件路径 (--config)
  - 完全采用新版本标准 (VarNameInData)
  - 高效扫描 NetCDF 文件，只读取 metadata
  - 改进的缺失时间检测（容忍时间漂移，支持文件边界重叠）
  - 精确到小时，自动裁剪到配置的时间范围
  - 不生成空的缺失文件
"""

import os
import re
import logging
import argparse
from typing import Optional
import pandas as pd
import xarray as xr
import configparser
from glob import glob
from typing import Tuple


# =========> 时间工具 <==========

def _parse_temporal_res(res_str: str) -> Tuple[str, int]:
    """解析时间分辨率字符串，如 6H、1D、1M 等"""
    s = res_str.strip().lower()
    aliases = {
        "day": "1d", "daily": "1d", "d": "1d",
        "mon": "1m", "month": "1m", "monthly": "1m",
        "year": "1y", "yr": "1y", "annual": "1y", "a": "1y",
    }
    s = aliases.get(s, s)
    if s == "24h":
        s = "1d"
    m = re.fullmatch(r"(\d+)\s*h(r)?", s)
    if m:
        return "hourly", int(m.group(1))
    m = re.fullmatch(r"(\d+)\s*d", s)
    if m:
        return "daily", int(m.group(1))
    m = re.fullmatch(r"(\d+)\s*m", s)
    if m:
        return "monthly", int(m.group(1))
    m = re.fullmatch(r"(\d+)\s*(y|a)", s)
    if m:
        return "yearly", int(m.group(1))
    if s in ("const", "constant"):
        return "constant", 0
    raise ValueError(f"Unrecognized TemporalRes: {res_str}")


def _normalize_time_index(times: pd.DatetimeIndex, category: str) -> pd.DatetimeIndex:
    """规范时间到指定粒度"""
    if category == "hourly":
        return times.floor("h")
    if category == "daily":
        return times.normalize()
    if category == "monthly":
        return times.to_period("M").to_timestamp(how="start")
    if category == "yearly":
        return times.to_period("Y").to_timestamp(how="start")
    return times

def _expected_range(start_str: str, end_str: str, category: str, step: int, align_ref: Optional[pd.Timestamp] = None) -> pd.DatetimeIndex:
    """
    生成期望时间序列：
      - 若提供 align_ref（实际数据首时间点），则自动对齐相位；
      - 保证覆盖 DataStart~DataEnd；
      - 对于小时分辨率，智能匹配 align_ref 的小时相位（如06、12、18）。
    """
    start_cfg = pd.to_datetime(start_str.replace("_", " "))
    end_cfg = pd.to_datetime(end_str.replace("_", " "))

    start = start_cfg

    # 智能相位对齐
    if align_ref is not None and category == "hourly":
        ref_hour = align_ref.hour % 24
        # 例如 align_ref=06:00 且 step=6 → 对齐06、12、18、00序列
        phase = ref_hour % step
        start_hour = (start.hour % step)
        if start_hour != phase:
            diff = (phase - start_hour) % step
            start = start + pd.Timedelta(hours=diff)
        start = start.floor("h")

    elif align_ref is not None and category == "daily":
        start = align_ref.normalize()

    # 生成时间序列
    if category == "hourly":
        return pd.date_range(start=start, end=end_cfg, freq=f"{step}h")
    if category == "daily":
        return pd.date_range(start=start, end=end_cfg, freq=f"{step}D")
    if category == "monthly":
        return pd.date_range(start=start.to_period("M").to_timestamp(how="start"),
                             end=end_cfg.to_period("M").to_timestamp(how="start"),
                             freq=f"{step}MS")
    if category == "yearly":
        return pd.date_range(start=start.to_period("Y").to_timestamp(how="start"),
                             end=end_cfg.to_period("Y").to_timestamp(how="start"),
                             freq=f"{step}YS-JAN")
    return pd.DatetimeIndex([])



def _format_missing(ts: pd.Timestamp, category: str) -> str:
    """按粒度格式化时间"""
    if category == "hourly":
        return ts.strftime("%Y-%m-%d %H:%M")
    if category == "daily":
        return ts.strftime("%Y-%m-%d")
    if category == "monthly":
        return ts.strftime("%Y-%m")
    if category == "yearly":
        return ts.strftime("%Y")
    return str(ts)


# =========> 配置读取 <==========

def ReadConfig(cfg_path: str) -> configparser.ConfigParser:
    if not os.path.exists(cfg_path):
        raise FileNotFoundError(f"Configuration file not found: {cfg_path}")
    config = configparser.ConfigParser(interpolation=None)
    config.read(cfg_path)
    logging.info("Configuration loaded: %s", cfg_path)
    return config


# =========> 构建文件索引表 <==========

def BuildFileIndex(config, dataname: str) -> pd.DataFrame:
    datadir = config.get("BaseInfo", "DataDir").strip()
    fileindex = config.get("BaseInfo", "FileIndex").strip()
    all_nc = sorted(glob(os.path.join(datadir, "**/*.nc"), recursive=True))
    logging.info("Scanning %d NetCDF files in %s ...", len(all_nc), datadir)

    records = []
    for f in all_nc:
        try:
            with xr.open_dataset(f, decode_times=True) as ds:
                vars_in_file = list(ds.variables.keys())

                if "time" in ds.coords or "time" in ds.variables:
                    t = pd.to_datetime(ds["time"].values, errors="coerce")
                    t = t.dropna()
                    if len(t) > 0:
                        start = pd.Timestamp(t.min()).floor("h")
                        end = pd.Timestamp(t.max()).floor("h")
                        ntime = len(t)
                    else:
                        start = end = pd.NaT
                        ntime = 0
                else:
                    start = end = pd.NaT
                    ntime = 0

            records.append({
                "FilePath": os.path.abspath(f),
                "FileName": os.path.basename(f),
                "Variables": ",".join(vars_in_file),
                "StartTime": start.strftime("%Y-%m-%d %H:%M") if pd.notna(start) else "",
                "EndTime": end.strftime("%Y-%m-%d %H:%M") if pd.notna(end) else "",
                "NTimes": ntime,
            })

        except Exception as e:
            logging.warning("Failed to read %s: %s", f, e)

    df = pd.DataFrame(records)
    df.to_csv(f"{datadir}/{fileindex}", index=False)
    logging.info("File index saved: %s (%d files)", fileindex, len(df))
    return df


# =========> 改进版缺失时间检测 <==========

def DetectMissing(config, df: pd.DataFrame, dataname: str):
    """
    改进版缺失时间检测：
      - 自动根据数据首时间点对齐相位（防止6H偏移误判）
      - 支持文件边界重叠与非整点漂移
      - 精确到小时，自动裁剪时间范围
      - 仅在确实存在缺失时生成 miss_<var>.txt
    """
    datadir = config.get("BaseInfo", "DataDir").strip()
    datastart = config.get("BaseInfo", "DataStart").strip()
    dataend = config.get("BaseInfo", "DataEnd").strip()

    for var in [s for s in config.sections() if s != "BaseInfo"]:
        varname = config.get(var, "VarNameInData").strip()
        tres = config.get(var, "TemporalRes").strip()
        cat, step = _parse_temporal_res(tres)

        related = df[df["Variables"].str.contains(varname, na=False)]
        if related.empty:
            logging.warning("[%s] No file found containing variable '%s'", var, varname)
            continue

        all_times = []
        for f in related["FilePath"]:
            try:
                with xr.open_dataset(f) as ds:
                    if "time" in ds:
                        times = pd.to_datetime(ds["time"].values, errors="coerce").dropna()
                        times = _normalize_time_index(pd.DatetimeIndex(times), cat)
                        all_times.append(times)
            except Exception as e:
                logging.warning("Failed reading %s for %s: %s", f, var, e)

        if not all_times:
            continue

        # ===== 合并所有时间点 =====
        combined = pd.DatetimeIndex(sorted(set().union(*[set(t.tolist()) for t in all_times])))

        # ===== 生成期望时间序列（自动对齐） =====
        align_ref = combined.min() if len(combined) > 0 else None
        start_cfg = pd.to_datetime(datastart.replace("_", " "))
        end_cfg = pd.to_datetime(dataend.replace("_", " "))

        start = start_cfg
        if align_ref is not None and cat == "hourly":
            ref_hour = align_ref.hour % 24
            phase = ref_hour % step
            start_hour = (start.hour % step)
            if start_hour != phase:
                diff = (phase - start_hour) % step
                start = start + pd.Timedelta(hours=diff)
            start = start.floor("h")
            logging.info("[%s] Aligning hourly sequence to %02d:00 phase (step=%dh)", var, ref_hour, step)
        elif align_ref is not None and cat == "daily":
            start = align_ref.normalize()

        if cat == "hourly":
            expected = pd.date_range(start=start, end=end_cfg, freq=f"{step}h")
        elif cat == "daily":
            expected = pd.date_range(start=start, end=end_cfg, freq=f"{step}D")
        elif cat == "monthly":
            expected = pd.date_range(start=start.to_period("M").to_timestamp(how="start"),
                                     end=end_cfg.to_period("M").to_timestamp(how="start"),
                                     freq=f"{step}MS")
        elif cat == "yearly":
            expected = pd.date_range(start=start.to_period("Y").to_timestamp(how="start"),
                                     end=end_cfg.to_period("Y").to_timestamp(how="start"),
                                     freq=f"{step}YS-JAN")
        else:
            expected = pd.DatetimeIndex([])

        # ===== 剪裁并去重 =====
        combined = combined[(combined >= expected.min()) & (combined <= expected.max())].drop_duplicates()

        # ===== 缺失时间检测 =====
        tol = pd.Timedelta(hours=step / 3 if cat == "hourly" else 0)
        missing = []
        for t in expected:
            nearby = combined[(combined >= t - tol) & (combined <= t + tol)]
            if nearby.empty:
                missing.append(t)

        # ===== 输出结果 =====
        if not missing:
            logging.info("[%s] No missing timestamps detected.", var)
            continue  # 不生成 miss 文件

        miss_file = f"{datadir}/miss_{var}.txt"
        with open(miss_file, "w", encoding="utf-8") as f:
            f.write(f"# Missing timestamps for variable {var} ({varname})\n")
            f.write(f"# TemporalRes = {tres}\n")
            f.write(f"# Total Missing = {len(missing)}\n")
            f.write("# ---------------------------------\n")

            line_group = []
            for i, ts in enumerate(missing, 1):
                line_group.append(_format_missing(ts, cat))
                # 每行控制长度，写4个时间为一行
                if len(line_group) == 4 or i == len(missing):
                    f.write("  ".join(line_group) + "\n")
                    line_group.clear()

        logging.info("[%s] Missing file saved: %s (%d missing)", var, miss_file, len(missing))


# =========> 主程序入口 <==========

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate forcing file index and detect missing time steps.")
    parser.add_argument("-c", "--config", required=True, help="Path to CRESM_Forcing.ini")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s| %(message)s",
        datefmt="%m-%d %H:%M:%S",
        force=True,
    )

    config = ReadConfig(args.config)
    dataname = config.get("BaseInfo", "ForcingDataName").strip()

    df_index = BuildFileIndex(config, dataname)
    DetectMissing(config, df_index, dataname)

    logging.info("All done successfully.")
