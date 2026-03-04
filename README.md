# CRESM Preprocessing System (CPS)

## **Version: v1.2.2**

[![License: GPL](https://img.shields.io/badge/License-GPL-blue.svg)](#license)
[![Platform: Linux](https://img.shields.io/badge/Platform-Linux-success.svg)](#requirements)
[![Python](https://img.shields.io/badge/Python-%3E%3D3.9-blue.svg)](#requirements)

> 社区区域气候模式预处理系统（CRESM Preprocessing System, **CPS**）
>
> 面向 **CRESM** 的统一化、自动化、高性能数据前处理系统。

CPS 是为新一代国产区域地球系统模式 **CRESM** 开发的数据前处理系统，用于统一管理并自动执行模式运行前所需的数据准备任务。系统围绕 **CWRF**、**CoLM2024** 与 **CPL7** 耦合流程组织，可完成静态地理数据、初始/边界场、陆面参数场以及耦合映射文件等关键前处理步骤。

---

## Features

- **统一流程入口**：通过单一主程序管理多阶段前处理流程
- **模块化设计**：支持 `PrepCWRF`、`PrepCoLM`、`PrepCRESM` 三大模块
- **配置驱动运行**：通过 `env.ini` 与 `case.ini` 控制环境与实验流程
- **静态数据可复用**：支持复用 `Geog_[GridName]` 与 `CoLMSrf_[GridName]`
- **适配高性能环境**：面向 Linux + MPI + 科学计算库环境设计
- **便于批量实验**：支持按案例、按年度组织数据处理任务
- **日志清晰**：提供主日志与流程日志，便于调试与故障排查

---

## Changelog

### v1.2.2
- Initial public release

---

## Workflow Overview

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

## Project Structure

```text
CPS/
├── PrepScript/              # 核心前处理脚本
├── SpinUpScript/            # CoLM Spin-Up 脚本
├── CRESM_ToolBox/           # CRESM 工具箱
├── Case/                    # 个例与实验工作目录
└── README.md
```

### Core scripts

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

### Case directory layout

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

## Installation

### 1. Create Conda environments

CPS uses Conda environments for dependency management. According to the user manual, three environments are typically created:

```bash
conda env create -f cresm.yml
conda env create -f cresm_xesmf.yml
conda env create -f Chaomodis.yml
```

Typical responsibilities of these environments:

- `cresm`: main program, FlowDir, plotting and related scripts
- `cresm_xesmf`: mesh generation and remapping
- `Chaomodis`: FVC / IGBP / LAI / SAI related processing

### 2. Install CRESM_ToolBox

After configuring `Makeoptions.ini`, install the toolbox with:

```bash
python SetupAll.py
```

---

## Configuration

CPS is controlled by two configuration files:

- `env.ini`: runtime environment, executable paths, toolbox paths and forcing definitions
- `case.ini`: experiment switches, time range, domain settings and workflow control

### `env.ini`

This file describes **where** the system runs and **what environment** it uses. Typical items include:

- CWRF / CoLM environment paths
- Conda environment names
- script directory paths
- CRESM toolbox paths
- raw and runtime data paths
- external executable paths such as `ncks`, `cdo`, `ncl`
- forcing dataset definitions

### `case.ini`

This file describes **what** to run and **for which experiment**. Typical items include:

- temp file cleanup policy
- time chunking options
- CPU core counts for CWRF / CoLM
- module switches such as `Go_Geogrid`, `Go_MakeSrf`, `Go_Coupler_Prep`
- output collection switches
- domain configuration and simulation period

---

## Quick Start

### Show help

```bash
python CRESM_Preprocessing_System.py -h
```

### List available cases

```bash
python CRESM_Preprocessing_System.py -l
```

### Run a case

```bash
python CRESM_Preprocessing_System.py -n CN_30km
```

### Debug mode

```bash
python CRESM_Preprocessing_System.py -n CN_30km -d
```

### Reuse existing geography data

```bash
python CRESM_Preprocessing_System.py -n CN_30km -g path/to/Geog_[GridName]
```

### Reuse existing CoLM surface data

```bash
python CRESM_Preprocessing_System.py -n CN_30km -s path/to/CoLMSrf_[GridName]
```

### Yearly forcing mode

```bash
python CRESM_Preprocessing_System.py -n CN_30km -y 2000 \
  -g path/to/Geog_[GridName] \
  -s path/to/CoLMSrf_[GridName]
```

### Collect yearly outputs

```bash
python CRESM_Preprocessing_System.py -c CN_30km
```

---

## Command-Line Options

| Option | Description |
|---|---|
| `-h, --help` | Show help message and exit |
| `-v, --version` | Show version information and exit |
| `-d, --debug` | Enable debug mode |
| `-ch, --confighelp` | Show configuration help and exit |
| `-l, --listcases` | List runnable cases defined in `case.ini` |
| `-n, --gridname` | Specify case name |
| `-g, --geogdir` | Reuse existing geography data |
| `-s, --colmsrf` | Reuse existing CoLM surface data |
| `-y, --year` | Override year for yearly forcing workflow |
| `-c, --collectcase` | Collect annual outputs into one case directory |

---

## Recommended Usage Pattern

When preparing long-period datasets, a practical workflow is:

1. Run a short test first to generate static products.
2. Reuse `Geog_[GridName]` and `CoLMSrf_[GridName]` for later yearly or multi-year runs.
3. Run annual jobs with `-y YEAR`.
4. Collect outputs after all years are finished.

This pattern reduces redundant preprocessing and is especially useful for large experiments.

---

## Logging

CPS uses two levels of logging.

### Main log

Default filename:

```text
DataPrepare.[GridName].log
```

Typical content:

- program start and finish status
- configuration parsing results
- environment and path checks
- module start and finish markers
- external commands in debug mode
- global error tracebacks

### Process logs

Default directory:

```text
CaseOutputPath/[GridName]/Log/
```

Naming convention:

```text
log.<ProcessName>[.<TimeTag>]
```

Typical content:

- stdout/stderr of external executables
- MPI runtime messages
- detailed process-level errors
- numerical and file processing details

---

## Troubleshooting

### Recommended checks

- confirm all paths in `env.ini` are valid
- confirm `ForcingDataName` matches forcing definitions in `env.ini`
- confirm required commands such as `ncl`, `cdo`, `ncks` are callable in shell
- retry with `--debug` enabled
- inspect both the main log and the corresponding process log

### Known cautions

- Do **not** enable `CleanTempFiles=True` while debugging, otherwise intermediate files may be removed.
- Internal environment switching may be unstable on some systems; manually `source` the required environment before running CPS.
- Some grid decompositions may conflict with assigned CPU counts; try adjusting core numbers if MPI jobs fail.
- CoLM `define` options can affect the target experiment configuration and should be checked carefully.

---

## License

This project is licensed under the **GNU General Public License (GPL)**.

Please add the corresponding `LICENSE` file to the repository before public release.
