#! /stu01/wumej22/Anaconda3/bin/python
# -*- coding: utf-8 -*-

"""
===============================================================================
Module Name   : Utils.Logger
Description   : Logging configuration setup.
                Provides a standardized logger with both file and console output
                support, handling formatting and log levels.

Author        : Omarjan @ SYSU
Created       : 2025-05-25
===============================================================================
"""

import os
import sys
import time
import shlex
import logging
from collections import deque


class Adaptive_Level_Formatter(logging.Formatter):
    """
    针对终端显示优化的格式化器：
    1. INFO及以下：保持原色
    2. WARNING/ERROR/CRITICAL：将 [时间 | 级别] 整体染色
    3. 自动处理对齐问题，防止颜色代码干扰补白
    """

    RESET = "\033[0m"
    COLOR_MAP = {
            # 格式：\033[背景;前景m
            "DEBUG":   "\033[101;37m",             # 
            "WARNING":  "\033[43;37m",          # 黄底白字 (43背景, 37前景)
            "ERROR":    "\033[41;37m",          # 红底白字 (41背景, 37前景)
            "CRITICAL": "\033[101;97;1m",       # 明红底白字加粗 (101高亮背景, 97高亮前景, 1加粗)
        }

    def __init__(self, fmt_normal, fmt_debug, datefmt=None, enable_color=True):
        super().__init__(datefmt=datefmt)
        self.formatter_normal = logging.Formatter(fmt_normal, datefmt)
        self.formatter_debug = logging.Formatter(fmt_debug, datefmt)
        self.enable_color = enable_color

    def format(self, record):
            formatter = self.formatter_debug if record.levelno == logging.DEBUG else self.formatter_normal
            s = formatter.format(record)

            if not self.enable_color or record.levelname not in self.COLOR_MAP:
                return s

            parts = s.split('|', 2)
            if len(parts) >= 2:
                color = self.COLOR_MAP[record.levelname]
                # 这里的 header 加上了前后的空格，会让底色块看起来更整齐
                header = f"{parts[0]}|{parts[1]}| " 
                message = parts[2]
                # 组合：[底色][时间|级别][重置] 消息内容
                return f"{color}{header}{self.RESET}{message}"

            return s


def Setup_Logger(
    logfile: str,
    loglevel: int,
    logger_name: str = "CRESMPrep",
    enable_color: bool = True,
):
    """
    Configure ONLY a named logger tree (default: "CRESMPrep").
    - Does NOT clear or modify root logger handlers.
    - Prevents duplicate logs by setting propagate=False.
    - File handler: no color
    - Console handler: color WARNING/ERROR/CRITICAL if TTY
    """
    # Ensure log directory exists
    logdir = os.path.dirname(os.path.abspath(logfile))
    if logdir and (not os.path.exists(logdir)):
        os.makedirs(logdir, exist_ok=True)

    # Reset logfile
    if os.path.exists(logfile):
        os.remove(logfile)

    logger = logging.getLogger(logger_name)
    logger.setLevel(loglevel)
    logger.propagate = False  # critical: avoid bubbling to root

    # Clear ONLY this logger's handlers (do NOT touch root)
    if logger.handlers:
        logger.handlers.clear()

    # Formats
    fmt_normal = "%(asctime)s | %(levelname)-8s|  %(message)s"
    fmt_debug = (
        "%(asctime)s | %(levelname)-8s| "
        "PID=%(process)d | "
        "%(filename)s:%(funcName)s:%(lineno)d | "
        "%(message)s"
    )
    datefmt = "%m-%d %H:%M:%S"

    # File handler (no color)
    fh = logging.FileHandler(logfile)
    fh.setLevel(loglevel)
    fh.setFormatter(
        Adaptive_Level_Formatter(
            fmt_normal=fmt_normal,
            fmt_debug=fmt_debug,
            datefmt=datefmt,
            enable_color=False,
        )
    )
    logger.addHandler(fh)

    # Console handler
    use_color = bool(enable_color and sys.stdout.isatty())
    sh = logging.StreamHandler()
    sh.setLevel(loglevel)
    sh.setFormatter(
        Adaptive_Level_Formatter(
            fmt_normal=fmt_normal,
            fmt_debug=fmt_debug,
            datefmt=datefmt,
            enable_color=use_color,
        )
    )
    logger.addHandler(sh)

    return logger


def Tail(path: str, n: int = 50) -> str:
    """Return last n lines of a text file."""
    if not path or (not os.path.exists(path)):
        return ""
    with open(path, "r", errors="ignore") as f:
        return "".join(deque(f, n))


def Extract_Redirect_Logfile(cmd: str):
    """
    Parse shell redirection:  cmd > logfile  (or ... 2>&1)
    Return logfile path or None.
    """
    if ">" not in cmd:
        return None

    parts = shlex.split(cmd)
    if ">" in parts:
        idx = parts.index(">")
        if idx + 1 < len(parts):
            return parts[idx + 1]
    return None


def Log_Redirect_Tail(logger: logging.Logger, cmd: str, sleep_sec: float = 0.25, n: int = 50) -> None:
    """
    Print last lines of redirected log file (if command used '> logfile').
    Intended to be called when a subprocess fails.
    """
    log_file = Extract_Redirect_Logfile(cmd)
    if not log_file:
        return

    time.sleep(sleep_sec)
    if os.path.exists(log_file):
        logger.error("Last few lines from redirected log file:")
        for line in Tail(log_file, n=n).splitlines():
            logger.error("    " + line)
    else:
        logger.error(f"Cannot find redirected log file: {log_file}")
