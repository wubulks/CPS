#!/usr/bin/env bash
set -euo pipefail

# 年份列表
years=(1996 1995 1994 1993)

# Python 脚本所在目录（如果和脚本不在同一目录，改为对应路径）
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

for year in "${years[@]}"; do
  echo "===== 处理年份：${year} ====="

  # 1. 替换 ERA5_sfc_download.py 中的 start_year 和 end_year
  sed -i.bak -E "s/^(start_year\s*=\s*)[0-9]{4}/\1${year}/" "${SCRIPT_DIR}/ERA5_sfc_download.py"
  sed -i.bak -E "s/^(end_year\s*=\s*)[0-9]{4}/\1${year}/"   "${SCRIPT_DIR}/ERA5_sfc_download.py"

  # 2. 运行 surface 下载脚本，并重定向日志
  echo "运行 ERA5_sfc_download.py（surface），日志：log.era5_sf_${year}"
  /stu01/wumej22/Anaconda3/envs/ERA5_download/bin/python "${SCRIPT_DIR}/ERA5_sfc_download.py" &> "${SCRIPT_DIR}/log.era5_sf_${year}"
  echo "surface 下载完成"

  # 4. 替换 ERA5_pres_download.py 中的 start_year 和 end_year
  sed -i.bak -E "s/^(start_year\s*=\s*)[0-9]{4}/\1${year}/" "${SCRIPT_DIR}/ERA5_pres_download.py"
  sed -i.bak -E "s/^(end_year\s*=\s*)[0-9]{4}/\1${year}/"   "${SCRIPT_DIR}/ERA5_pres_download.py"

  # 5. 运行 pressure 下载脚本，并重定向日志
  echo "运行 ERA5_pres_download.py（pressure），日志：log.era5_press_${year}"
  python "${SCRIPT_DIR}/ERA5_pres_download.py" &> "${SCRIPT_DIR}/log.era5_press_${year}"
  echo "pressure 下载完成"

  echo
done

echo "所有年份处理完毕！"
