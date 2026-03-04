#!/usr/bin/env python3
"""
根据用户指定的时间范围检查 CFSv2 数据文件，支持指定开始时间和结束时间。
"""

import os
import pandas as pd
from datetime import datetime, timedelta
import argparse

# ===== 配置参数 =====
sfckb = 3891   # 小于此值视为异常（3.8MB）
base_dir = "/home/chengs24/stu02/CFSV2"

# ===== 使用 argparse 解析命令行参数 =====
def parse_arguments():
    """解析命令行输入"""
    parser = argparse.ArgumentParser(
        description="检查指定时间范围内的 CFSv2 数据文件是否存在且大于指定大小"
    )
    parser.add_argument(
        "start_date", 
        type=str, 
        help="起始日期 (格式: YYYY-MM-DD)"
    )
    parser.add_argument(
        "end_date", 
        type=str, 
        help="结束日期 (格式: YYYY-MM-DD)"
    )
    args = parser.parse_args()

    try:
        start_dt = datetime.strptime(args.start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(args.end_date, "%Y-%m-%d")
    except ValueError:
        print("日期格式错误，请使用格式: YYYY-MM-DD")
        sys.exit(1)

    if start_dt > end_dt:
        print("结束日期不能早于开始日期！")
        sys.exit(1)

    return start_dt, end_dt

# ===== 开始检查 =====
def check_files(start_dt, end_dt):
    print(f"开始检查 CFSv2 数据文件是否存在且大于 {sfckb/1024:.1f} MB...")

    bad_files = []
    retry_list = []

    current_date = start_dt

    # 遍历日期范围
    while current_date <= end_dt:
        year = current_date.year
        init_date = current_date.strftime("%m%d")  # 起报时间：每年3月1日00时
        init_time = "00"

        # 计算结束时间
        end_date = datetime(year, 12, 31, 0)
        start_date = datetime(year, 6, 1, 0)

        # 计算总小时数（从6月1日00时到12月31日00时）
        total_hours = int((end_date - start_date).total_seconds() / 3600)

        # 地面场数据检查目录
        flux_dir = os.path.join(base_dir, "operational-9-month-forecast", "6-hourly-flux", '060100', str(year))

        # 确保目录存在
        if not os.path.exists(flux_dir):
            print(f"警告: 目录不存在 {flux_dir}")
            current_date += timedelta(days=1)
            continue

        for fhr in range(0, total_hours + 1, 6):
            # 计算目标日期时间
            target_time = start_date + timedelta(hours=fhr)
            target_date = target_time.strftime("%Y%m%d")
            target_hour = target_time.strftime("%H")

            # 构建文件名
            filename = f"flxf{target_date}{target_hour}.01.{init_date}{init_time}.grb2"
            filepath = os.path.join(flux_dir, filename)

            # 检查文件
            if not os.path.exists(filepath):
                bad_files.append((filepath, "缺失"))
                retry_list.append(filepath)
            else:
                size_kb = os.path.getsize(filepath) / 1024
                if size_kb < sfckb:
                    bad_files.append((filepath, f"过小 ({size_kb/1024:.1f} MB)"))
                    retry_list.append(filepath)

        current_date += timedelta(days=1)

    return bad_files


# ===== 输出结果 =====
def output_results(bad_files):
    if bad_files:
        print(f"\n共发现 {len(bad_files)} 个异常文件：")
        for path, reason in bad_files:
            print(f"[异常] {path} —— {reason}")
        print(f"\n共发现 {len(bad_files)} 个异常文件：")
    else:
        print("没有发现异常文件。")


# ===== 主函数 =====
def main():
    # 解析命令行参数
    start_dt, end_dt = parse_arguments()

    # 执行文件检查
    bad_files = check_files(start_dt, end_dt)

    # 输出结果
    output_results(bad_files)


if __name__ == "__main__":
    main()
