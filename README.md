# CRESM Preprocessing System (CPS)

## **Version: v1.2.3**

[![License: GPL](https://img.shields.io/badge/License-GPL-blue.svg)](#license)
[![Platform: Linux](https://img.shields.io/badge/Platform-Linux-success.svg)](#requirements)
[![Python](https://img.shields.io/badge/Python-%3E%3D3.9-blue.svg)](#requirements)

> 社区区域气候模式预处理系统（CRESM Preprocessing System, **CPS**）
>
> 面向 **CRESM** 的统一化、自动化、高性能数据前处理系统。

CPS 是为新一代国产区域地球系统模式 **CRESM** 开发的数据前处理系统，用于统一管理并自动执行模式运行前所需的数据准备任务。系统围绕 **CWRF**、**CoLM2024** 与 **CPL7** 耦合流程组织，可完成静态地理数据、初始/边界场、陆面参数场以及耦合映射文件等关键前处理步骤。

---

## 功能特性

- **统一流程入口**：通过单一主程序管理多阶段前处理流程
- **模块化设计**：支持 `PrepCWRF`、`PrepCoLM`、`PrepCRESM` 三大模块
- **配置驱动运行**：通过 `env.ini` 与 `case.ini` 控制环境与实验流程
- **静态数据可复用**：支持复用 `Geog_[GridName]` 与 `CoLMSrf_[GridName]`
- **适配高性能环境**：面向 Linux + MPI + 科学计算库环境设计
- **便于批量实验**：支持按案例、按年度组织数据处理任务
- **日志清晰**：提供主日志与流程日志，便于调试与故障排查

---

## 更新日志

### v1.2.3
- 改进了 `CRESM_Preprocessing_System.py` 的按年重配置逻辑：在切换实验年份时保留原始月、日和时分秒信息，并增加起止时间合法性检查，避免生成无效时间范围。
- 精简了 `history.colm.ctl`，移除了大量显式的历史输出变量开关，使默认的 CoLM history 控制文件更简洁。
- 增强了 `PrepCWRF.py`，支持可配置的海陆掩膜来源，并可在静态地理场预处理阶段选择使用 CoLM 的高分辨率海陆掩膜数据。
- 更新了 `chanlu.ncl`，增加对 `SC_WATER` 的修正与写回，提高地表水体分类与植被相关变量之间的一致性。
- 重构了 `CorrectGeoEM.py`，以高分辨率 `land_ocean_mask` 替代旧的感染算法海洋识别流程，同时保留基于 `LANDUSEF` 的保守一致性回修，并支持 shapefile 与 NetCDF 两种海陆边界输入。
- 扩展了 `ICBC.py`，增加基于 calendar 的时间序列处理能力，并新增对 `CESM2_hist` forcing 的支持，同时同步更新 Ungrib、Metgrid、Real 以及 CWPS/CWRF 相关文件链接流程。

### v1.2.2
- 首个公开版本发布。

---

## 工作流程概览

CPS 的标准工作流如下：

```text
case.ini + env.ini
        |
        v
CRESM_Preprocessing_System.py
        |
        +-- PrepCWRF
        |    |- Domain Visualization
        |    |- Static Geogrid
        |    |- Vegetation Parameters
        |    `- Initial/Boundary Data
        |
        +-- PrepCoLM
        |    |- Mesh Generation
        |    |- Surface Data
        |    |- Initial Conditions
        |    `- Spin-up & Remapping
        |
        `-- PrepCRESM
             `- Coupler Mapping Files
```

---

## 项目结构

```text
CPS/
├── PrepScript/              # 核心前处理脚本
├── SpinUpScript/            # CoLM Spin-Up 脚本
├── CRESM_ToolBox/           # CRESM 工具箱
├── Case/                    # 个例与实验工作目录
└── README.md
```

### 核心脚本

```text
PrepScript/
├── CRESM_Preprocessing_System.py   # 主入口程序
├── PrepCWRF.py                     # CWRF 前处理模块
├── PrepCoLM.py                     # CoLM 前处理模块
├── PrepCRESM.py                    # CPL7 映射文件生成模块
├── env.ini                         # 环境配置文件
├── case.ini                        # 实验配置文件
├── Utils/                          # 内部工具库
├── ProcessScript/                  # 外部处理脚本库
├── NML/                            # namelist 模板库
└── Forcing/                        # 驱动数据信息库
```

### Case 目录结构

```text
Case/
└── [GridName]/
    ├── Create_Run_From_CPS.py
    ├── Grid_[GridName]/
    ├── ICBC_[GridName]/
    ├── PrepCWRF/
    ├── PrepCoLM/
    ├── PrepCRESM/
    ├── NMLS/
    └── Log/
```

---

## 安装说明

### 1. 创建 Conda 环境

CPS 使用 Conda 环境管理依赖。按照用户手册，通常需要创建以下三个环境：

```bash
conda env create -f cresm.yml
conda env create -f cresm_xesmf.yml
conda env create -f Chaomodis.yml
```

这些环境通常分别承担如下职责：

- `cresm`：主程序、FlowDir、绘图及相关脚本
- `cresm_xesmf`：网格生成与重映射
- `Chaomodis`：FVC / IGBP / LAI / SAI 相关处理

### 2. 安装 CRESM_ToolBox

在配置好 `Makeoptions.ini` 之后，可使用以下命令安装工具箱：

```bash
python SetupAll.py
```

---

## 配置说明

CPS 主要由两个配置文件控制：

- `env.ini`：运行环境、可执行程序路径、工具箱路径以及 forcing 定义
- `case.ini`：实验开关、时间范围、区域设置和流程控制

### `env.ini`

该文件定义系统**在哪里运行**、**使用什么环境**。典型内容包括：

- CWRF / CoLM 环境脚本路径
- Conda 环境名称
- 脚本目录路径
- CRESM 工具箱路径
- 原始数据与运行数据路径
- `ncks`、`cdo`、`ncl` 等外部程序路径
- forcing 数据集定义

### `case.ini`

该文件定义系统**运行什么**、**面向哪个实验**。典型内容包括：

- 临时文件清理策略
- 时间分块选项
- CWRF / CoLM 的 CPU 核数设置
- `Go_Geogrid`、`Go_MakeSrf`、`Go_Coupler_Prep` 等模块开关
- 输出收集开关
- 模拟区域配置与实验时段

---

## 快速开始

### 查看帮助

```bash
python CRESM_Preprocessing_System.py -h
```

### 列出可用实验案例

```bash
python CRESM_Preprocessing_System.py -l
```

### 运行一个案例

```bash
python CRESM_Preprocessing_System.py -n CN_30km
```

### 调试模式

```bash
python CRESM_Preprocessing_System.py -n CN_30km -d
```

### 复用已有地理静态数据

```bash
python CRESM_Preprocessing_System.py -n CN_30km -g path/to/Geog_[GridName]
```

### 复用已有 CoLM 地表数据

```bash
python CRESM_Preprocessing_System.py -n CN_30km -s path/to/CoLMSrf_[GridName]
```

### 按年份处理 forcing

```bash
python CRESM_Preprocessing_System.py -n CN_30km -y 2000 \
  -g path/to/Geog_[GridName] \
  -s path/to/CoLMSrf_[GridName]
```

### 汇总按年输出结果

```bash
python CRESM_Preprocessing_System.py -c CN_30km
```

---

## 命令行参数

| 参数 | 说明 |
|---|---|
- `-h, --help` | 显示帮助信息并退出 |
- `-v, --version` | 显示版本信息并退出 |
- `-d, --debug` | 启用调试模式 |
- `-ch, --confighelp` | 显示配置帮助并退出 |
- `-l, --listcases` | 列出 `case.ini` 中定义的可运行案例 |
- `-n, --gridname` | 指定案例名称 |
- `-g, --geogdir` | 复用已有地理静态数据 |
- `-s, --colmsrf` | 复用已有 CoLM 地表数据 |
- `-y, --year` | 覆盖年份，用于按年 forcing 流程 |
- `-c, --collectcase` | 将逐年输出汇总到同一个案例目录 |

---

## 推荐使用方式

当需要处理长时间序列数据时，推荐工作方式如下：

1. 先做一次短时间测试，生成静态产物。
2. 后续逐年或多年运行时，复用 `Geog_[GridName]` 和 `CoLMSrf_[GridName]`。
3. 使用 `-y YEAR` 提交逐年任务。
4. 全部年份完成后，再统一收集输出。

这种方式可以减少重复前处理，特别适合大区域、长时段实验。

---

## 日志系统

CPS 采用两级日志结构。

### 主日志

默认文件名：

```text
DataPrepare.[GridName].log
```

典型内容包括：

- 程序启动与结束状态
- 配置解析结果
- 环境与路径检查
- 各模块开始与结束标记
- 调试模式下的外部命令
- 全局错误回溯信息

### 过程日志

默认目录：

```text
CaseOutputPath/[GridName]/Log/
```

命名规则：

```text
log.<ProcessName>[.<TimeTag>]
```

典型内容包括：

- 外部可执行程序的 stdout/stderr
- MPI 运行信息
- 具体过程级错误
- 数值处理与文件处理细节

---

## 故障排查

### 建议检查项

- 确认 `env.ini` 中所有路径都有效
- 确认 `ForcingDataName` 与 `env.ini` 中的 forcing 定义一致
- 确认 `ncl`、`cdo`、`ncks` 等命令在 shell 中可调用
- 尝试启用 `--debug` 重新运行
- 同时检查主日志和对应的过程日志

### 已知注意事项

- 调试时**不要**启用 `CleanTempFiles=True`，否则中间文件可能被删除。
- 某些系统上，内部环境切换可能不稳定；必要时请先手动 `source` 所需环境后再运行 CPS。
- 某些网格划分方式可能与设置的 CPU 核数冲突；若 MPI 任务失败，请尝试调整核数。
- CoLM 的 `define` 选项会影响目标实验配置，需要仔细检查。

---

## 许可协议

本项目采用 **GNU General Public License (GPL)** 许可证。
