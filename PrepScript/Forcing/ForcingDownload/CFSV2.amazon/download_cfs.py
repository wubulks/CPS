import boto3
from botocore import UNSIGNED
from botocore.config import Config
from tqdm import tqdm
import re
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import calendar
from dateutil.relativedelta import relativedelta
from threading import Lock

# ========= 参数解析 =========
if len(sys.argv) != 2:
    print("用法: python download_cfs.py <cycle_time>")
    print("示例: python download_cfs.py 2025052600")
    sys.exit(1)

cycle_time = sys.argv[1]
if not re.match(r"^\d{10}$", cycle_time):
    print("错误: cycle_time 必须是10位数字,例如 2025052600")
    sys.exit(1)

# ========= 时间与目录 =========
start_dt = datetime.strptime(cycle_time, "%Y%m%d%H")
end_dt_raw = start_dt + relativedelta(months=1)
end_day = calendar.monthrange(end_dt_raw.year, end_dt_raw.month)[1]
end_dt = datetime(end_dt_raw.year, end_dt_raw.month, end_day, 23, 59)

output_dir = os.path.join(start_dt.strftime("%Y/%m/%d/%H"))
os.makedirs(output_dir, exist_ok=True)

# ========= S3 设置 =========
s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
bucket_name = 'noaa-cfs-pds'
prefix = f'cfs.{cycle_time[:8]}/{cycle_time[8:]}/6hrly_grib_01/'

# ========= 获取目标文件 =========
def list_target_files():
    target_files = []
    patterns = [r'^flxf(\d{10})\.01\.' + cycle_time + r'\.grb2$',
                r'^pgbf(\d{10})\.01\.' + cycle_time + r'\.grb2$']
    paginator = s3.get_paginator('list_objects_v2')

    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        if 'Contents' in page:
            for obj in page['Contents']:
                filename = os.path.basename(obj['Key'])
                for pattern in patterns:
                    match = re.match(pattern, filename)
                    if match:
                        valid_time = datetime.strptime(match.group(1), "%Y%m%d%H")
                        if start_dt <= valid_time <= end_dt:
                            target_files.append(filename)
    return sorted(target_files)

# ========= 下载函数 =========
progress_lock = Lock()

def download_file(file, global_pbar):
    s3_key = prefix + file
    local_path = os.path.join(output_dir, file)

    # 如果文件存在,验证大小
    if os.path.exists(local_path):
        size = os.path.getsize(local_path)
        if file.startswith('flxf') and size < 3_900_000:
            tqdm.write(f"[WARN] {file} too small ({size} bytes), re-downloading...")
            os.remove(local_path)
        elif file.startswith('pgbf') and size < 20_000_000:
            tqdm.write(f"[WARN] {file} too small ({size} bytes), re-downloading...")
            os.remove(local_path)
        else:
            with progress_lock:
                global_pbar.update(1)
            return f"[SKIP] {file}"

    try:
        with open(local_path, 'wb') as f:
            s3.download_fileobj(bucket_name, s3_key, f)

        with progress_lock:
            global_pbar.update(1)
        return f"[OK] {file}"
    except Exception as e:
        return f"[ERROR] {file} failed: {e}"

# ========= 主流程 =========
if __name__ == '__main__':
    print(f"📅 Cycle time: {cycle_time}")
    print(f"📆 Download window: {start_dt.strftime('%Y-%m-%d')} → {end_dt.strftime('%Y-%m-%d')}")
    print(f"📁 Output directory: {output_dir}\n")

    print("🔍 Fetching available files from S3...")
    files = list_target_files()
    print(f"📦 Found {len(files)} files within date range.\n")

    with tqdm(total=len(files), unit='file', desc="Total Progress") as global_pbar:
        with ThreadPoolExecutor(max_workers=8) as executor:
            future_to_file = {
                executor.submit(download_file, file, global_pbar): file
                for file in files
            }
            for future in as_completed(future_to_file):
                tqdm.write(future.result())

    print("\n✅ All done.")

