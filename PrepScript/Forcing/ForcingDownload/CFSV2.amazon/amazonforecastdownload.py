from datetime import datetime, timedelta
import pandas as pd
from multiprocessing import Pool
from tqdm import tqdm
import os

# 参数设置
start_year = 2023 #修改点1：起报年份2012-2025,amazon2018-2025
end_year = 2023
output_dir_flux = "/home/chengs24/stu02/CFSV2/operational-9-month-forecast/6-hourly-flux"
output_dir_pressure = "/home/chengs24/stu02/CFSV2/operational-9-month-forecast/6-hourly-by-pressure"

# 创建输出目录
os.makedirs(output_dir_flux, exist_ok=True)
os.makedirs(output_dir_pressure, exist_ok=True)

# 构建起报日期列表（每年3月1日）
timelist = [datetime(year, 6, 1).strftime('%Y%m%d') for year in range(start_year, end_year + 1)]#修改点2：起报时间030100,040100,050100,060100

# 构建命令列表
cmdlist = []
for init_date in timelist:
    year = init_date[:4]
    month = init_date[4:6]
    day = init_date[6:8]
    hour = "00"  # 固定起报时间为00时
    
    # 计算预报结束日期（10月1日）
    end_date = datetime(int(year), 12, 31) #修改点3：起报月份和结束月份3-10；4-11；5-12；6-1(按12.31算)
    total_hours = int((end_date - datetime(int(year), 6, 1)).total_seconds() / 3600) #修改点4：起报月份和日期
    
    # 创建按年份的子目录
    flux_year_dir = os.path.join(output_dir_flux, year)
    pressure_year_dir = os.path.join(output_dir_pressure, year)
    os.makedirs(flux_year_dir, exist_ok=True)
    os.makedirs(pressure_year_dir, exist_ok=True)
    
    # 遍历每个6小时的预报时间
    for fhr in range(0, total_hours + 1, 6):
        target_time = datetime(int(year), 6, 1) + timedelta(hours=fhr) #修改点5：起报月份和日期
        target_date = target_time.strftime("%Y%m%d")
        target_hour = target_time.strftime("%H")

        # 构建气压场下载链接和文件名
        pressure_url = (
            f"https://noaa-cfs-pds.s3.amazonaws.com/cfs.{init_date}/{hour}/6hrly_grib_01/"
            f"pgbf{target_date}{target_hour}.01.{init_date}{hour}.grb2"
        )
        pressure_out = os.path.join(
            pressure_year_dir, f"pgbf{target_date}{target_hour}.01.{init_date}{hour}.grb2"
        )
        if not os.path.exists(pressure_out):
            cmdlist.append(f"wget -c -q '{pressure_url}' -O '{pressure_out}'")


        # 构建地面通量场下载链接和文件名
        flux_url = (
            f"https://noaa-cfs-pds.s3.amazonaws.com/cfs.{init_date}/{hour}/6hrly_grib_01/"
            f"flxf{target_date}{target_hour}.01.{init_date}{hour}.grb2"
        )
        flux_out = os.path.join(
            flux_year_dir, f"flxf{target_date}{target_hour}.01.{init_date}{hour}.grb2"
        )
        if not os.path.exists(flux_out):
            cmdlist.append(f"wget -c -q '{flux_url}' -O '{flux_out}'")

print(f"共生成 {len(cmdlist)} 个下载任务")

# 下载函数
def download(cmd):
    os.system(cmd)
    return 1

if __name__ == "__main__":
    # 使用多进程并行下载
    with Pool(processes=12) as pool:
        results = list(tqdm(pool.imap_unordered(download, cmdlist), total=len(cmdlist)))
    
    print("所有下载任务完成！")
    print(f"成功下载 {sum(results)}/{len(cmdlist)} 个文件")