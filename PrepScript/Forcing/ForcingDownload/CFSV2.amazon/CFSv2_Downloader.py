#!/usr/bin/env python3
"""
定期检查 NOAA CFSv2 数据集中最新的 00 起报 cycle_time,
如果本地未处理过,则调用 download_cfs.py 执行下载.
支持 Python 3.7+
"""

import boto3
from botocore import UNSIGNED
from botocore.config import Config
import os
import re
import subprocess
import sys
from typing import Optional

# === 配置项 ===
BUCKET = "noaa-cfs-pds"
DOWNLOAD_SCRIPT = "download_cfs.py"               # 下载脚本
DOWNLOAD_LOG = "downloaded_cycles.txt"            # 下载记录文件

s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))


def get_latest_s3_date() -> Optional[str]:
    """从 S3 根路径列出所有 cfs.YYYYMMDD/，返回最新的日期字符串"""
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=BUCKET, Delimiter="/")

    date_list = []
    for page in pages:
        for prefix in page.get("CommonPrefixes", []):
            match = re.match(r"cfs\.(\d{8})/", prefix["Prefix"])
            if match:
                date_list.append(match.group(1))

    if not date_list:
        return None

    return max(date_list, key=lambda d: int(d))


def already_downloaded(cycle_time: str) -> bool:
    """判断该 cycle_time 是否已在本地记录中"""
    if not os.path.exists(DOWNLOAD_LOG):
        return False
    with open(DOWNLOAD_LOG, "r") as f:
        return cycle_time in f.read().splitlines()


def mark_downloaded(cycle_time: str) -> None:
    """将已下载的 cycle_time 写入本地记录"""
    with open(DOWNLOAD_LOG, "a") as f:
        f.write(cycle_time + "\n")


def main() -> None:
    latest_date = get_latest_s3_date()
    if latest_date is None:
        print("⚠️  未能从 S3 获取可用日期")
        sys.exit(1)

    cycle_time = latest_date + "00"
    print(f"📌 最新可用 cycle_time(00 起报）: {cycle_time}")

    if already_downloaded(cycle_time):
        print(f"✅ 已下载过 {cycle_time}，跳过")
        return

    # 实时调用 download_cfs.py，显示进度
    print(f"🚀 正在调用下载脚本: python {DOWNLOAD_SCRIPT} {cycle_time}")
    result = subprocess.run(
        ["python", DOWNLOAD_SCRIPT, cycle_time]
    )

    if result.returncode == 0:
        print(f"✅ 下载完成: {cycle_time}")
        mark_downloaded(cycle_time)
    else:
        print(f"❌ 下载失败: {cycle_time}，返回码 {result.returncode}")
        sys.exit(result.returncode)


if __name__ == "__main__":
    main()

