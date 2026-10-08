#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Generate CPL7 weights with ESMF and compare them with NCL outputs."""

import argparse
import os
import shlex
import subprocess
from pathlib import Path

import numpy as np
from netCDF4 import Dataset


REPO_ROOT = Path(__file__).resolve().parents[4]
DEFAULT_NCL_DIR = REPO_ROOT / "cpl7data_ncl"
DEFAULT_DESCRIPTOR_DIR = Path(
    "/hydata01/wumej22/Codex_Works/CPS_hydro/CN_30km/PrepCRESM/CN_30km/cpl7data"
)
DEFAULT_OUTPUT_DIR = REPO_ROOT / "cpl7data_esmf_CN_30km"
DEFAULT_ESMF = Path(
    "/stu01/wumej22/mylibs/GNU_LIBS.HYDRO/build/esmf/8.9.1/bin/"
    "binO/Linux.gfortran.64.mpich.default/ESMF_RegridWeightGen"
)
DEFAULT_MPIRUN = Path(
    "/stu01/wumej22/mylibs/GNU_LIBS.HYDRO/build/mpich/4.2.3/bin/mpirun"
)
DEFAULT_ESMF_ENV = Path(
    "/stu01/wumej22/mylibs/GNU_LIBS.HYDRO/bashrc_gnu_libs.sh"
)


def build_jobs(gridname):
    cwrf = f"cwrf_s_des_{gridname}.nc"
    colm = f"colm_grd_des_{gridname}.nc"
    fvcom_n = f"fvcom_grd_n_des_{gridname}.nc"
    fvcom_e = f"fvcom_grd_e_des_{gridname}.nc"

    return [
        (cwrf, colm, f"cwrf_s2colm_wgts_bl_{gridname}.nc", "neareststod", "SCRIP", "ESMFMESH"),
        (cwrf, colm, f"cwrf_s2colm_wgts_pat_{gridname}.nc", "neareststod", "SCRIP", "ESMFMESH"),
        (colm, cwrf, f"colm_2cwrf_s_wgt_bl_{gridname}.nc", "neareststod", "ESMFMESH", "SCRIP"),
        (colm, cwrf, f"colm_2cwrf_s_wgt_pat_{gridname}.nc", "neareststod", "ESMFMESH", "SCRIP"),
        (cwrf, fvcom_n, f"cwrf_s2fvcom_n_wgts_bl_{gridname}.nc", "bilinear", "SCRIP", "ESMFMESH"),
        (cwrf, fvcom_e, f"cwrf_s2fvcom_e_wgts_bl_{gridname}.nc", "bilinear", "SCRIP", "ESMFMESH"),
        (cwrf, fvcom_n, f"cwrf_s2fvcom_n_wgts_pat_{gridname}.nc", "patch", "SCRIP", "ESMFMESH"),
        (cwrf, fvcom_e, f"cwrf_s2fvcom_e_wgts_pat_{gridname}.nc", "patch", "SCRIP", "ESMFMESH"),
        (fvcom_n, cwrf, f"fvcom_n2cwrf_s_wgts_bl_{gridname}.nc", "bilinear", "ESMFMESH", "SCRIP"),
        (fvcom_e, cwrf, f"fvcom_e2cwrf_s_wgts_bl_{gridname}.nc", "bilinear", "ESMFMESH", "SCRIP"),
        (fvcom_n, cwrf, f"fvcom_n2cwrf_s_wgts_pat_{gridname}.nc", "patch", "ESMFMESH", "SCRIP"),
        (fvcom_e, cwrf, f"fvcom_e2cwrf_s_wgts_pat_{gridname}.nc", "patch", "ESMFMESH", "SCRIP"),
    ]


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gridname", default="CN_30km")
    parser.add_argument("--ncl-dir", type=Path, default=DEFAULT_NCL_DIR)
    parser.add_argument("--descriptor-dir", type=Path, default=DEFAULT_DESCRIPTOR_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--esmf", type=Path, default=DEFAULT_ESMF)
    parser.add_argument("--mpirun", type=Path, default=DEFAULT_MPIRUN)
    parser.add_argument("--esmf-env", type=Path, default=DEFAULT_ESMF_ENV)
    parser.add_argument("--np", type=int, default=1, help="MPI process count, default: 1")
    parser.add_argument("--ucx-tls", default="", help="Optional UCX_TLS value for local MPI tests")
    parser.add_argument("--skip-generate", action="store_true", help="Only compare existing ESMF outputs")
    return parser.parse_args()


def require_file(path):
    if not path.is_file():
        raise FileNotFoundError(f"Required file not found: {path}")


def run_esmf(job, args, log_path):
    source_name, destination_name, output_name, method, source_type, destination_type = job
    source = args.descriptor_dir / source_name
    destination = args.descriptor_dir / destination_name
    output = args.output_dir / output_name
    require_file(source)
    require_file(destination)
    if output.exists():
        output.unlink()

    command = [
        str(args.mpirun),
        "-np",
        str(args.np),
        str(args.esmf),
        "--source",
        str(source),
        "--destination",
        str(destination),
        "--weight",
        str(output),
        "--method",
        method,
        "--src_type",
        source_type,
        "--dst_type",
        destination_type,
        "--ignore_degenerate",
        "--ignore_unmapped",
    ]
    if source_type == "ESMFMESH":
        command.extend(["--src_loc", "corner"])
    if destination_type == "ESMFMESH":
        command.extend(["--dst_loc", "corner"])
    shell_command = f"source {shlex.quote(str(args.esmf_env))} && exec {shlex.join(command)}"
    environment = os.environ.copy()
    if args.ucx_tls:
        environment["UCX_TLS"] = args.ucx_tls
    with log_path.open("w", encoding="utf-8") as log_file:
        result = subprocess.run(
            ["bash", "-lc", shell_command],
            cwd=args.output_dir,
            env=environment,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            check=False,
        )
    if result.returncode != 0:
        raise subprocess.CalledProcessError(result.returncode, command)
    require_file(output)


def compare_values(ncl_var, esmf_var):
    ncl_data = np.asarray(ncl_var[:])
    esmf_data = np.asarray(esmf_var[:])
    if ncl_data.shape != esmf_data.shape:
        return False, int(ncl_data.size), None, None
    exact = np.array_equal(ncl_data, esmf_data, equal_nan=True)
    different = int(np.count_nonzero(ncl_data != esmf_data))
    if np.issubdtype(ncl_data.dtype, np.number):
        difference = np.abs(ncl_data.astype(float) - esmf_data.astype(float))
        max_abs = float(np.nanmax(difference)) if difference.size else 0.0
    else:
        max_abs = None
    return exact, different, max_abs, ncl_data.shape


def compare_file(ncl_path, esmf_path):
    result = {
        "file": ncl_path.name,
        "dimensions_equal": False,
        "variables_equal": False,
        "variables": {},
        "attribute_differences": {},
    }
    with Dataset(ncl_path) as ncl_file, Dataset(esmf_path) as esmf_file:
        ncl_dimensions = {name: len(dim) for name, dim in ncl_file.dimensions.items()}
        esmf_dimensions = {name: len(dim) for name, dim in esmf_file.dimensions.items()}
        result["dimensions_equal"] = ncl_dimensions == esmf_dimensions
        result["variables_equal"] = set(ncl_file.variables) == set(esmf_file.variables)
        if result["variables_equal"]:
            for name in ncl_file.variables:
                exact, different, max_abs, shape = compare_values(
                    ncl_file.variables[name], esmf_file.variables[name]
                )
                result["variables"][name] = {
                    "exact": exact,
                    "different": different,
                    "max_abs": max_abs,
                    "shape": shape,
                }
        attribute_names = set(ncl_file.ncattrs()) | set(esmf_file.ncattrs())
        for name in sorted(attribute_names):
            ncl_value = ncl_file.getncattr(name) if name in ncl_file.ncattrs() else None
            esmf_value = esmf_file.getncattr(name) if name in esmf_file.ncattrs() else None
            if ncl_value != esmf_value:
                result["attribute_differences"][name] = (ncl_value, esmf_value)
    return result


def print_result(result):
    variables = result["variables"]
    exact = all(item["exact"] for item in variables.values())
    print(
        f"{result['file']}: values_exact={exact}, "
        f"dimensions_equal={result['dimensions_equal']}, "
        f"variables_equal={result['variables_equal']}"
    )
    for name, item in variables.items():
        if not item["exact"]:
            print(
                f"  {name}: different={item['different']}, "
                f"max_abs={item['max_abs']}"
            )
    if result["attribute_differences"]:
        print(f"  attribute_differences={sorted(result['attribute_differences'])}")


def main():
    args = parse_args()
    require_file(args.esmf)
    require_file(args.mpirun)
    require_file(args.esmf_env)
    require_file(args.ncl_dir / f"cwrf_s_des_{args.gridname}.nc")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    jobs = build_jobs(args.gridname)
    for job in jobs:
        output_name = job[2]
        if not args.skip_generate:
            log_path = args.output_dir / f"{output_name}.log"
            run_esmf(job, args, log_path)
    report_path = args.output_dir / f"compare_{args.gridname}.txt"
    with report_path.open("w", encoding="utf-8") as report:
        for job in jobs:
            output_name = job[2]
            result = compare_file(args.ncl_dir / output_name, args.output_dir / output_name)
            print_result(result)
            report.write(f"{result}\n")
    print(f"Report: {report_path}")


if __name__ == "__main__":
    main()
