"""
make_fileindex.py — Final Robust Version
---------------------------------------
生成：
  1. forc_index.<dataname>.csv   —— 文件索引表（文件路径、变量、时间范围）
  2. miss_<var>.txt              —— 每个变量独立缺失时间文件（仅在确实存在缺失时生成）

特性：
  - 从命令行参数传入配置文件路径 (--config)
  - 默认只扫描目标目录当前层；传入 -r/--recursive 时递归扫描子目录
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
import warnings
from typing import Optional
import calendar as pycalendar
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


def _normalize_calendar_name(calendar_name: str) -> str:
    """标准化 calendar 名称。"""
    calendar_name = calendar_name.split("#", 1)[0].strip()
    aliases = {
        "gregorian": "standard",
        "standard": "standard",
        "proleptic_gregorian": "standard",
        "julian": "standard",
        "no-leap": "noleap",
        "noleap": "noleap",
        "365_day": "noleap",
        "all_leap": "all_leap",
        "366_day": "all_leap",
        "360_day": "360_day",
    }
    key = calendar_name.strip().lower()
    if key not in aliases:
        raise ValueError(f"Unsupported calendar: {calendar_name}")
    return aliases[key]


def _parse_datetime_string(dt_str: str) -> Tuple[int, int, int, int, int]:
    """解析配置中的时间字符串，兼容 YYYY-MM-DD_HH 和 YYYY-MM-DD HH:MM。"""
    normalized = dt_str.strip().replace("_", " ")
    match = re.fullmatch(
        r"(\d{4})-(\d{2})-(\d{2})(?:\s+(\d{2})(?::(\d{2}))?)?",
        normalized,
    )
    if not match:
        raise ValueError(f"Unsupported datetime format: {dt_str}")
    year, month, day, hour, minute = match.groups()
    return int(year), int(month), int(day), int(hour or 0), int(minute or 0)


def _days_in_month(year: int, month: int, calendar_name: str) -> int:
    if calendar_name == "360_day":
        return 30
    if calendar_name == "noleap" and month == 2:
        return 28
    if calendar_name == "all_leap" and month == 2:
        return 29
    return pycalendar.monthrange(year, month)[1]


def _add_days(year: int, month: int, day: int, delta_days: int, calendar_name: str) -> Tuple[int, int, int]:
    y, m, d = year, month, day
    remain = delta_days
    while remain > 0:
        dim = _days_in_month(y, m, calendar_name)
        if d < dim:
            d += 1
        else:
            d = 1
            if m < 12:
                m += 1
            else:
                m = 1
                y += 1
        remain -= 1
    return y, m, d


def _add_hours(components: Tuple[int, int, int, int], delta_hours: int, calendar_name: str) -> Tuple[int, int, int, int]:
    year, month, day, hour = components
    total_hours = hour + delta_hours
    extra_days, new_hour = divmod(total_hours, 24)
    year, month, day = _add_days(year, month, day, extra_days, calendar_name)
    return year, month, day, new_hour


def _time_value_to_components(value, category: str) -> Tuple[int, ...]:
    """将 pandas/cftime 时间值统一转换为可比较的离散时间组件。"""
    year = int(value.year)
    if category == "yearly":
        return (year,)

    month = int(value.month)
    if category == "monthly":
        return (year, month)

    day = int(value.day)
    if category == "daily":
        return (year, month, day)

    hour = int(value.hour)
    return (year, month, day, hour)


def _format_components(components: Tuple[int, ...], category: str) -> str:
    if category == "yearly":
        return f"{components[0]:04d}"
    if category == "monthly":
        return f"{components[0]:04d}-{components[1]:02d}"
    if category == "daily":
        return f"{components[0]:04d}-{components[1]:02d}-{components[2]:02d}"
    return f"{components[0]:04d}-{components[1]:02d}-{components[2]:02d} {components[3]:02d}:00"


def _generate_expected_components(
    start_str: str,
    end_str: str,
    category: str,
    step: int,
    calendar_name: str,
    align_ref: Optional[Tuple[int, ...]] = None,
) -> list[Tuple[int, ...]]:
    """按配置 calendar 生成期望时间组件序列。"""
    start_year, start_month, start_day, start_hour, _ = _parse_datetime_string(start_str)
    end_year, end_month, end_day, end_hour, _ = _parse_datetime_string(end_str)

    if category == "yearly":
        current = (start_year,)
        end_comp = (end_year,)
    elif category == "monthly":
        current = (start_year, start_month)
        end_comp = (end_year, end_month)
    elif category == "daily":
        current = (start_year, start_month, start_day)
        end_comp = (end_year, end_month, end_day)
        if align_ref is not None:
            current = align_ref[:3]
    elif category == "hourly":
        current = (start_year, start_month, start_day, start_hour)
        end_comp = (end_year, end_month, end_day, end_hour)
        if align_ref is not None:
            ref_hour = align_ref[3] % 24
            phase = ref_hour % step
            start_phase = current[3] % step
            if start_phase != phase:
                current = _add_hours(current, (phase - start_phase) % step, calendar_name)
    else:
        return []

    expected = []
    while current <= end_comp:
        expected.append(current)
        if category == "yearly":
            current = (current[0] + step,)
        elif category == "monthly":
            year, month = current
            total_month = (year * 12 + (month - 1)) + step
            current = (total_month // 12, total_month % 12 + 1)
        elif category == "daily":
            current = _add_days(*current, step, calendar_name)
        elif category == "hourly":
            current = _add_hours(current, step, calendar_name)
    return expected


def _extract_time_components(ds: xr.Dataset, category: str) -> list[Tuple[int, ...]]:
    """从数据集中提取并规范化 time 坐标。"""
    if "time" not in ds.coords and "time" not in ds.variables:
        return []
    values = ds["time"].values
    if len(values) == 0:
        return []
    extracted = []
    for value in values:
        try:
            extracted.append(_time_value_to_components(value, category))
        except Exception:
            ts = pd.Timestamp(value)
            extracted.append(_time_value_to_components(ts, category))
    return extracted


# =========> 配置读取 <==========

def ReadConfig(cfg_path: str) -> configparser.ConfigParser:
    if not os.path.exists(cfg_path):
        raise FileNotFoundError(f"Configuration file not found: {cfg_path}")
    config = configparser.ConfigParser(interpolation=None)
    config.read(cfg_path)
    logging.info("Configuration loaded: %s", cfg_path)
    return config


# =========> 构建文件索引表 <==========

def BuildFileIndex(config, dataname: str, recursive: bool = False) -> pd.DataFrame:
    datadir = config.get("BaseInfo", "DataDir").strip()
    fileindex = config.get("BaseInfo", "FileIndex").strip()
    pattern = os.path.join(datadir, "**", "*.nc") if recursive else os.path.join(datadir, "*.nc")
    all_nc = sorted(glob(pattern, recursive=recursive))
    logging.info(
        "Scanning %d NetCDF files in %s (%s) ...",
        len(all_nc),
        datadir,
        "recursive" if recursive else "top-level only",
    )

    records = []
    for f in all_nc:
        try:
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", category=xr.SerializationWarning)
                ds = xr.open_dataset(f, decode_times=True)
            with ds:
                vars_in_file = list(ds.variables.keys())
                time_components = _extract_time_components(ds, "hourly")
                if time_components:
                    start = _format_components(min(time_components), "hourly")
                    end = _format_components(max(time_components), "hourly")
                    ntime = len(time_components)
                else:
                    start = end = ""
                    ntime = 0

            records.append({
                "FilePath": os.path.abspath(f),
                "FileName": os.path.basename(f),
                "Variables": ",".join(vars_in_file),
                "StartTime": start,
                "EndTime": end,
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
    calendar_name = _normalize_calendar_name(config.get("BaseInfo", "Calender", fallback="standard"))

    if df.empty or "Variables" not in df.columns:
        logging.warning("File index is empty. Skip missing-time detection.")
        return

    for var in [s for s in config.sections() if s != "BaseInfo"]:
        varname = config.get(var, "VarNameInData").strip()
        tres = config.get(var, "TemporalRes").strip()
        cat, step = _parse_temporal_res(tres)

        related = df[df["Variables"].fillna("").apply(lambda s: varname in [v.strip() for v in s.split(",")])]
        if related.empty:
            logging.warning("[%s] No file found containing variable '%s'", var, varname)
            continue

        all_times = []
        for f in related["FilePath"]:
            try:
                with warnings.catch_warnings():
                    warnings.filterwarnings("ignore", category=xr.SerializationWarning)
                    ds = xr.open_dataset(f, decode_times=True)
                with ds:
                    times = _extract_time_components(ds, cat)
                    if times:
                        all_times.extend(times)
            except Exception as e:
                logging.warning("Failed reading %s for %s: %s", f, var, e)

        if not all_times:
            continue

        combined = sorted(set(all_times))
        align_ref = combined[0] if combined else None
        expected = _generate_expected_components(
            datastart,
            dataend,
            cat,
            step,
            calendar_name,
            align_ref=align_ref,
        )

        if cat == "hourly" and align_ref is not None:
            logging.info("[%s] Aligning hourly sequence to %02d:00 phase (step=%dh, calendar=%s)",
                         var, align_ref[3], step, calendar_name)

        if not expected:
            logging.warning("[%s] Empty expected time axis generated.", var)
            continue

        expected_min = expected[0]
        expected_max = expected[-1]
        combined = [t for t in combined if expected_min <= t <= expected_max]
        actual = set(combined)
        missing = [t for t in expected if t not in actual]

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
                line_group.append(_format_components(ts, cat))
                # 每行控制长度，写4个时间为一行
                if len(line_group) == 4 or i == len(missing):
                    f.write("  ".join(line_group) + "\n")
                    line_group.clear()

        logging.info("[%s] Missing file saved: %s (%d missing)", var, miss_file, len(missing))


# =========> 主程序入口 <==========

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate forcing file index and detect missing time steps.")
    parser.add_argument("-c", "--config", required=True, help="Path to CRESM_Forcing.ini")
    parser.add_argument(
        "-r",
        "--recursive",
        action="store_true",
        help="Recursively scan subdirectories under DataDir for NetCDF files.",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s| %(message)s",
        datefmt="%m-%d %H:%M:%S",
        force=True,
    )

    config = ReadConfig(args.config)
    dataname = config.get("BaseInfo", "ForcingDataName").strip()

    df_index = BuildFileIndex(config, dataname, recursive=args.recursive)
    DetectMissing(config, df_index, dataname)

    logging.info("All done successfully.")
