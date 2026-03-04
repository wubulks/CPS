from datetime import datetime, timedelta
import os
import pandas as pd
import subprocess

# ===== 配置参数 =====
start_year = 2011 #修改点1：起报年份2012-2025
end_year = 2025
base_dir = "/home/chengs24/stu02/CFSV2"
presskb = 20480  # 小于此值视为异常（20MB）
num_retry = 3  # 补下重试次数
retry_delay = 10  # 重试间隔（秒）

# ===== 开始检查 =====
print(f"开始检查 CFSv2 气压场预报数据文件是否存在且大于 {presskb/1024:.1f} MB...")

bad_files = []
retry_list = []

# 处理每个年份
for year in range(start_year, end_year + 1):
    # 起报时间：每年4月1日00时
    init_date = f"{year}0601" #修改点2：起报时间030100,040100,050100,060100
    init_time = "00"
    
    # 计算结束时间（11s月1日00时）
    end_date = datetime(year, 12, 31, 0)
    start_date = datetime(year, 6, 1, 0)    #修改点3：起报月份和结束月份3-10；4-11；5-12
    
    # 计算总小时数（从3月1日00时到10月1日00时）
    total_hours = int((end_date - start_date).total_seconds() / 3600)
    
    # 气压场数据检查
    pressure_dir = os.path.join(base_dir, "operational-9-month-forecast", "6-hourly-by-pressure", '060100',str(year))   #修改点4：起报时间030100,040100,050100,060100
    
    # 确保目录存在
    if not os.path.exists(pressure_dir):
        print(f"警告: 目录不存在 {pressure_dir}")
        continue
    
    for fhr in range(0, total_hours + 1, 6):
        # 计算目标日期时间
        target_time = start_date + timedelta(hours=fhr)
        target_date = target_time.strftime("%Y%m%d")
        target_hour = target_time.strftime("%H")
        
        # 构建文件名
        filename = f"pgbf{target_date}{target_hour}.01.{init_date}{init_time}.grb2"
        filepath = os.path.join(pressure_dir, filename)
        
        # 检查文件
        if not os.path.exists(filepath):
            bad_files.append((filepath, "缺失"))
            retry_list.append(filepath)
        else:
            size_kb = os.path.getsize(filepath) / 1024
            if size_kb < presskb:
                bad_files.append((filepath, f"过小 ({size_kb/1024:.1f} MB)"))
                retry_list.append(filepath)

# ===== 输出结果 =====
if bad_files:
    print(f"\n共发现 {len(bad_files)} 个异常文件：")
    for path, reason in bad_files:
        print(f"[异常] {path} —— {reason}")
    print(f"\n共发现 {len(bad_files)} 个异常文件：")
    # ===== 补下功能 =====
