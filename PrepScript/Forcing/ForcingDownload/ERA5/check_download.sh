#!/bin/bash

# 检查输入参数
if [[ $# -ne 1 ]]; then
    echo "用法: $0 <输入文件>"
    exit 1
fi

input_file="$1"

while read -r line; do
    # 跳过空行和注释行
    [[ -z "$line" || "$line" == \#* ]] && continue

    # 解析行内容
    identifier=$(awk '{print $1}' <<< "$line")
    output_path=$(awk '{print $2}' <<< "$line")
    url=$(awk '{print $3}' <<< "$line")

    # 验证必要参数
    if [[ -z "$output_path" || -z "$url" ]]; then
        echo "错误：无效的行格式 - $line"
        exit 1
    fi

    # 创建输出目录
    mkdir -p "$(dirname "$output_path")"

    # 下载文件（带断点续传）
    echo "正在下载: $output_path"
    if ! wget -c -O "$output_path" "$url"; then
        echo "错误：下载失败 - $url"
        exit 1
    fi

    # 完整性检查
    remote_size=$(wget --spider --server-response "$url" 2>&1 | 
                 awk '/Content-Length/ {len=$2} END {print len}' | tr -d '\r')
    local_size=$(stat -c %s "$output_path" 2>/dev/null || echo 0)

    if [[ -z "$remote_size" ]]; then
        echo "警告：无法验证完整性 - 未获取到远程文件大小"
        [[ -s "$output_path" ]] && echo "下载完成（未验证完整性）" || { echo "文件为空"; exit 1; }
    elif (( local_size != remote_size )); then
        echo "错误：文件大小不匹配（本地: $local_size, 远程: $remote_size）"
        exit 1
    else
        echo "下载验证成功（$local_size 字节）"
    fi

done < "$input_file"

echo "所有下载任务完成"
