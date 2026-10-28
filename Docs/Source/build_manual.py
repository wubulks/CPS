#!/usr/bin/env python3
"""从 LaTeX 源文件生成 CPS 用户手册。"""
from argparse import ArgumentParser
from pathlib import Path
import shutil
import subprocess
import tempfile

SOURCE_DIR = Path(__file__).resolve().parent
DOCS_DIR = SOURCE_DIR.parent
DEFAULT_OUTPUT = DOCS_DIR / "社区区域气候模式预处理系统用户手册.pdf"


def main():
    parser = ArgumentParser(description="编译 CPS 用户手册")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT,
                        help="输出 PDF 路径；相对路径以 Docs 目录为基准")
    args = parser.parse_args()
    output = args.output if args.output.is_absolute() else DOCS_DIR / args.output
    xelatex = shutil.which("xelatex")
    if not xelatex:
        raise SystemExit("找不到 xelatex，请安装支持中文的 TeX 发行版。")
    with tempfile.TemporaryDirectory(prefix="cps-manual-") as tmp:
        command = [xelatex, "-interaction=nonstopmode", "-halt-on-error",
                   f"-output-directory={tmp}", "manual.tex"]
        for _ in range(2):
            subprocess.run(command, cwd=SOURCE_DIR, check=True)
        output.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(Path(tmp) / "manual.pdf", output)
    print(f"已生成：{output}")


if __name__ == "__main__":
    main()
