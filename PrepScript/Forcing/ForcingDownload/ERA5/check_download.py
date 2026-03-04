import os
import re
import logging
import subprocess
import requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s', datefmt="%y-%m-%d %H:%M:%S",)

def format_size(size_bytes):
    """将字节大小转换为人类可读的格式。"""
    if size_bytes is None:
        return "N/A"
    if size_bytes < 1024:
        return f"{size_bytes} B"
    for unit in ['KiB', 'MiB', 'GiB', 'TiB']:
        size_bytes /= 1024.0
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
    return f"{size_bytes:.2f} PiB"

def get_local_size(path):
    """返回本地文件大小（字节），如果文件不存在返回 0"""
    try:
        return os.path.getsize(path)
    except OSError:
        return 0

def get_remote_size(url, timeout=10):
    """发送 HEAD 请求，返回 Content-Length（字节），出错时返回 None"""
    try:
        r = requests.head(url, allow_redirects=True, timeout=timeout)
        r.raise_for_status()
        size = r.headers.get('Content-Length')
        return int(size) if size is not None else None
    except Exception as e:
        logging.error(f"无法获取远端大小：{url} → {e}")
        return None

def download_file(url, dst_path):
    """用 wget 下载到目标路径（会覆盖）"""
    dst_dir = os.path.dirname(dst_path)
    os.makedirs(dst_dir, exist_ok=True)
    cmd = ['wget', '-c', '-O', dst_path, url]
    try:
        logging.info(f"开始下载: {url}")
        subprocess.run(cmd, check=True, capture_output=True, text=True)
        logging.info(f"下载完成: {dst_path}")
    except subprocess.CalledProcessError as e:
        logging.error(f"下载失败: {e.stderr.strip()}")

def process_entry(identifier, local_path, url):
    fname = os.path.basename(local_path)
    m = re.search(r'(\d{8})', fname)
    date_str = f"{m.group(1)[:4]}-{m.group(1)[4:6]}-{m.group(1)[6:]}" if m else 'Unknown'

    status_prefix = f"[{identifier} | {date_str}]"
    logging.info(f"{status_prefix} 检查: {fname}")

    local_size = get_local_size(local_path)
    remote_size = get_remote_size(url)

    if remote_size is None:
        logging.warning(f"{status_prefix} 无法获取远端大小，跳过")
        return

    if local_size != remote_size:
        local_size_str = format_size(local_size)
        remote_size_str = format_size(remote_size)
        logging.info(f"{status_prefix} 大小不匹配 (本地: {local_size_str}, 远端: {remote_size_str})，需要重新下载")
        download_file(url, local_path)
    else:
        logging.info(f"{status_prefix} 已完整下载 ({format_size(local_size)})，跳过")

def main(list_file):
    """
    list_file 中每行格式：
    标识符 本地路径 URL
    中间用空格或制表符分隔
    """
    with open(list_file, 'r') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            parts = line.split()
            if len(parts) != 3:
                logging.warning(f"无法解析行：{line}")
                continue
            identifier, local_path, url = parts
            process_entry(identifier, local_path, url)

if __name__ == '__main__':
    import sys
    if len(sys.argv) != 2:
        print(f"用法: python {sys.argv[0]} 清单文件.txt")
        sys.exit(1)
    main(sys.argv[1])
