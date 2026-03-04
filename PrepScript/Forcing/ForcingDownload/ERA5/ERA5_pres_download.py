# r'''
# Author: ChenHJ
# Date: 2025-01-23 20:52:20
# LastEditors: ChenHJ
# LastEditTime: 2025-01-23 21:12:21
# FilePath: /CWRF_icbc_pure/SCRIPTS/ICBCScripts/PrepICBC/ERA5_pres_download.py
# Description: 
# '''
import cdsapi
import os
import calendar
# import netCDF4 as nc
import threading
from queue import Queue
from datetime import datetime
import subprocess
os.environ['KMP_DUPLICATE_LIB_OK'] = 'True'
# 创建一个函数来构建下载请求
def download_era5_data(year, month, day, download_dir):
    dataset = "reanalysis-era5-pressure-levels"
    request = {
        "product_type": ["reanalysis"],
        "variable": [
            "geopotential",
            "relative_humidity",
            "specific_humidity",
            "temperature",
            "u_component_of_wind",
            "v_component_of_wind"
        ],
        "year": [year],
        "month": [month],
        "day": [day],
        "time": [
            "00:00", "06:00", "12:00",
            "18:00"
        ],
        "pressure_level": [
            "1", "2", "3",
            "5", "7", "10",
            "20", "30", "50",
            "70", "100", "125",
            "150", "175", "200",
            "225", "250", "300",
            "350", "400", "450",
            "500", "550", "600",
            "650", "700", "750",
            "775", "800", "825",
            "850", "875", "900",
            "925", "950", "975",
            "1000"
        ],
        "data_format": "grib",
        "download_format": "unarchived"
    }

    # 定义文件名格式为 年月日.nc，并设置下载路径
    date_object = datetime(int(year),int(month),int(day))
    date_string = date_object.strftime("%Y%m%d")
    filename = "PRESS{}.grib".format(date_string)
    target_dir = os.path.join(download_dir,date_string[:4])
    filepath = os.path.join(target_dir, filename)
    
    print(f"Checking if download directory {target_dir} exists...")
    # 检查目录是否存在，不存在则创建
    if not os.path.exists(target_dir):
        print(f"Directory {target_dir} does not exist. Creating directory...")
        os.makedirs(target_dir)
    else:
        print(f"Directory {target_dir} already exists.")

    print(f"Checking if file {filename} exists and is complete...")
    # 检查文件是否已存在，且文件完整
    if os.path.exists(filepath):
        # try:
        #     # 尝试打开文件以验证其完整性
        #     with nc.Dataset(filepath, 'r') as ds:
        #         print(f"File {filename} is complete and valid.")
        # except OSError as e:
        #     # 如果文件不完整或损坏，删除并重新下载
        #     print(f"File {filename} is corrupted. Redownloading...")
        #     os.remove(filepath)
        #     download_file_from_era5(request, filepath)
        pass
    else:
        # 如果文件不存在，则直接下载
        print(f"File {filename} does not exist. Starting download...")
        download_file_from_era5(request, filepath)

# 创建一个函数来执行实际下载
def download_file_from_era5(request, filepath):
    print(f"Downloading data to {filepath}...")
    client = cdsapi.Client()
    client.retrieve("reanalysis-era5-pressure-levels", request, filepath)
    print(f"Download completed for {filepath}")

# ========================CONFIG================================
# 定义下载目录
start_year = 1994
end_year = 1994
start_month = 1
end_month = 12
download_dir = r"/tera07/zhangsl/wumej22/Omarjan/ERA5_RAW"#r"/stu01/chenhj23/ERA5" # 将利用
download_log = r"/tera07/zhangsl/wumej22/Omarjan/ERA5_RAW/log.era5_press_"+str(start_year)  # 下载日志文件，如果使用slurm或lsf作业提交系统，务必在代码中实时重定向log，以便提取下载失败的url
# ========================CONFIG================================

print(f"Checking if download directory {download_dir} exists...")
# 检查目录是否存在，不存在则创建
if not os.path.exists(download_dir):
    print(f"Directory {download_dir} does not exist. Creating directory...")
    os.makedirs(download_dir)
else:
    print(f"Directory {download_dir} already exists.")


# 创建一个下载工作线程类
class DownloadWorker(threading.Thread):
    def __init__(self, queue):
        threading.Thread.__init__(self)
        self.queue = queue

    def run(self):
        while True:
            year, month, day = self.queue.get()
            try:
                # 将月份和日期格式化为两位数
                month_str = f"{month:02d}"
                day_str = f"{day:02d}"
                print(f"Worker {threading.current_thread().name} processing download for {year}-{month_str}-{day_str}...")
                
                # 执行下载逻辑
                download_era5_data(str(year), month_str, day_str, download_dir)
            
            except Exception as e:
                print(f"Error downloading data for {year}-{month_str}-{day_str}: {e}, now retrying with wget...")
                
                # 从 url_mapping.log 中查找对应日期的 URL
                try:
                    grep_pattern = 'CHJJJJ[[:space:]]*[a-z0-9A-Z/]*_'+str(year)+month_str+day_str+'.grib'
                    grep_result = subprocess.run(
                        ["grep", grep_pattern, download_log],
                        text=True,
                        capture_output=True,
                        check=False
                    )
                    if grep_result.stdout:
                        # 提取 URL 和目标文件名
                        log_line = grep_result.stdout.strip()
                        url_to_download = log_line.split()[-1]  # 去掉 "url:"
                        target_file = log_line.split()[1]  # 获取 target: 后的目标文件名
                        print("CHJJJJ,debug log_line:", log_line)
                        print("CHJJJJ,debug url_to_download:", url_to_download)
                        print("CHJJJJ,debug target_file:", target_file)

                        # 使用 wget 下载
                        print(f"Retrying download with wget: {url_to_download} -> {target_file}")
                        subprocess.run(
                            ["wget", "-c", url_to_download, "-O", target_file],
                            check=True
                        )
                    else:
                        print(f"URL for {year}-{month_str}-{day_str} not found in {download_log}.")
                except Exception as retry_e:
                    print(f"Failed to retry download for {year}-{month_str}-{day_str}: {retry_e}")
            finally:
                print(f"Worker {threading.current_thread().name} finished processing download for {year}-{month:02d}-{day:02d}.")
                self.queue.task_done()

# 创建队列并初始化线程
queue = Queue()

# 创建四个工作线程
print("Creating worker threads...")
for x in range(4):
    worker = DownloadWorker(queue)
    worker.daemon = True
    worker.start()
    print(f"Worker thread {worker.name} started.")

# 循环遍历2000到2023年，将任务加入队列
print("Adding download tasks to the queue...")
for year in range(start_year, end_year+1):
    for month in range(start_month, end_month+1):
        # 获取当前月份的最大天数
        _, max_day = calendar.monthrange(year, month)
        for day in range(1, max_day + 1):
            print(f"Adding task for {year}-{month:02d}-{day:02d} to the queue...")
            queue.put((year, month, day))

# 等待所有任务完成
print("Waiting for all tasks to complete...")
queue.join()
print("All download tasks completed.")


# import cdsapi

# dataset = "reanalysis-era5-pressure-levels"
# request = {
#     "product_type": ["reanalysis"],
#     "variable": [
#         "geopotential",
#         "relative_humidity",
#         "specific_humidity",
#         "temperature",
#         "u_component_of_wind",
#         "v_component_of_wind"
#     ],
#     "year": ["1998"],
#     "month": ["02"],
#     "day": ["27"],
#     "time": [
#         "00:00", "06:00", "12:00",
#         "18:00"
#     ],
#     "pressure_level": [
#         "1", "2", "3",
#         "5", "7", "10",
#         "20", "30", "50",
#         "70", "100", "125",
#         "150", "175", "200",
#         "225", "250", "300",
#         "350", "400", "450",
#         "500", "550", "600",
#         "650", "700", "750",
#         "775", "800", "825",
#         "850", "875", "900",
#         "925", "950", "975",
#         "1000"
#     ],
#     "data_format": "grib",
#     "download_format": "unarchived"
# }

# client = cdsapi.Client()
# client.retrieve(dataset, request, "PRESS_19980227.grib")
