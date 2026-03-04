#!/bin/bash

# 指定文件夹路径
DIR=/tera07/zhangsl/wumej22/Omarjan/ERA5_RAW/1999/


# 检查文件夹是否存在
if [ ! -d "$DIR" ]; then
  echo "文件夹不存在: $DIR"
  exit 1
fi

# 重命名 SFC_YYYYMMDD.grib 文件
for file in "$DIR"/SFC_*.grib; do
  if [ -f "$file" ]; then
    new_name=$(basename "$file" | sed 's/SFC_/SFC/')
    mv "$file" "$DIR/$new_name"
    echo "重命名: $file -> $DIR/$new_name"
  fi
done

# 重命名 PRESS_YYYYMMDD.grib 文件
for file in "$DIR"/PRESS_*.grib; do
  if [ -f "$file" ]; then
    new_name=$(basename "$file" | sed 's/PRESS_/PRESS/')
    mv "$file" "$DIR/$new_name"
    echo "重命名: $file -> $DIR/$new_name"
  fi
done

echo "重命名完成！"
