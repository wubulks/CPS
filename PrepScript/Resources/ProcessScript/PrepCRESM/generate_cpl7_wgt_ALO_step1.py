#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Generate CPL7 Step1 ESMF regridding weights from grid descriptors."""

import argparse
import os
import shutil
import subprocess
from pathlib import Path

import numpy as np
from netCDF4 import Dataset

from ncllibs2py import Csstri
from ncllibs2py import NclLibs2PyContext


WEIGHT_JOBS = (
    ("cwrf_s_des_{g}.nc", "colm_grd_des_{g}.nc", "cwrf_s2colm_wgts_bl_{g}.nc", "neareststod", "SCRIP", "ESMFMESH"),
    ("cwrf_s_des_{g}.nc", "colm_grd_des_{g}.nc", "cwrf_s2colm_wgts_pat_{g}.nc", "neareststod", "SCRIP", "ESMFMESH"),
    ("colm_grd_des_{g}.nc", "cwrf_s_des_{g}.nc", "colm_2cwrf_s_wgt_bl_{g}.nc", "neareststod", "ESMFMESH", "SCRIP"),
    ("colm_grd_des_{g}.nc", "cwrf_s_des_{g}.nc", "colm_2cwrf_s_wgt_pat_{g}.nc", "neareststod", "ESMFMESH", "SCRIP"),
    ("cwrf_s_des_{g}.nc", "fvcom_grd_n_des_{g}.nc", "cwrf_s2fvcom_n_wgts_bl_{g}.nc", "bilinear", "SCRIP", "ESMFMESH"),
    ("cwrf_s_des_{g}.nc", "fvcom_grd_e_des_{g}.nc", "cwrf_s2fvcom_e_wgts_bl_{g}.nc", "bilinear", "SCRIP", "ESMFMESH"),
    ("cwrf_s_des_{g}.nc", "fvcom_grd_n_des_{g}.nc", "cwrf_s2fvcom_n_wgts_pat_{g}.nc", "patch", "SCRIP", "ESMFMESH"),
    ("cwrf_s_des_{g}.nc", "fvcom_grd_e_des_{g}.nc", "cwrf_s2fvcom_e_wgts_pat_{g}.nc", "patch", "SCRIP", "ESMFMESH"),
    ("fvcom_grd_n_des_{g}.nc", "cwrf_s_des_{g}.nc", "fvcom_n2cwrf_s_wgts_bl_{g}.nc", "bilinear", "ESMFMESH", "SCRIP"),
    ("fvcom_grd_e_des_{g}.nc", "cwrf_s_des_{g}.nc", "fvcom_e2cwrf_s_wgts_bl_{g}.nc", "bilinear", "ESMFMESH", "SCRIP"),
    ("fvcom_grd_n_des_{g}.nc", "cwrf_s_des_{g}.nc", "fvcom_n2cwrf_s_wgts_pat_{g}.nc", "patch", "ESMFMESH", "SCRIP"),
    ("fvcom_grd_e_des_{g}.nc", "cwrf_s_des_{g}.nc", "fvcom_e2cwrf_s_wgts_pat_{g}.nc", "patch", "ESMFMESH", "SCRIP"),
)


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("gridname", help="CPL7 grid/case name")
    parser.add_argument("--esmf", type=Path, required=True, help="Path to ESMF_RegridWeightGen")
    parser.add_argument("--grid-dir", type=Path, default=None, help="Directory containing the grid files")
    parser.add_argument("--output-dir", type=Path, default=None, help="Directory for descriptors and weights")
    parser.add_argument("--ocean-file", type=Path, default=None, help="FVCOM restart file")
    parser.add_argument(
        "--weights-only",
        action="store_true",
        help="Use existing NCL-generated descriptors and only generate ESMF weights",
    )
    parser.add_argument(
        "--np",
        type=int,
        default=int(os.environ.get("ESMF_NUM_PROC", "1")),
        help="Number of ESMF MPI processes; ESMF_NUM_PROC is used when omitted",
    )
    parser.add_argument("--mpirun", default="mpirun", help="MPI launcher used when --np is greater than one")
    return parser.parse_args()


def require_file(path):
    if not path.is_file():
        raise FileNotFoundError(f"Required file not found: {path}")


def read_variable(path, name):
    require_file(path)
    with Dataset(path) as data:
        if name not in data.variables:
            raise KeyError(f"Variable {name} not found in {path}")
        value = np.asarray(data.variables[name][:])
    if value.ndim == 3 and value.shape[0] == 1:
        value = value[0]
    return value


def read_colm_coordinates(grid_dir, gridname):
    colm_file = grid_dir / "colm_grd.nc"
    reference_file = grid_dir / f"CoLM_ref_{gridname}_vector.nc"
    lat = read_variable(colm_file, "XLAT_M")
    lon = read_variable(colm_file, "XLONG_M")
    require_file(reference_file)
    with Dataset(reference_file) as data:
        if "elmindex" not in data.variables:
            raise KeyError(f"Variable elmindex not found in {reference_file}")
        elmindex = np.asarray(data.variables["elmindex"][:], dtype=np.int64)
    flat_lat = lat.T.reshape(-1)
    flat_lon = lon.T.reshape(-1)
    if np.any(elmindex < 1) or np.any(elmindex > flat_lat.size):
        raise ValueError(f"elmindex in {reference_file} is outside the CoLM grid")
    return flat_lat[elmindex - 1], flat_lon[elmindex - 1]


def calculate_scrip_corners(lat, lon):
    extended_lat = np.empty((lat.shape[0] + 2, lat.shape[1] + 2), dtype=lat.dtype)
    extended_lon = np.empty((lon.shape[0] + 2, lon.shape[1] + 2), dtype=lon.dtype)
    extended_lat[1:-1, 1:-1] = lat
    extended_lon[1:-1, 1:-1] = lon
    extended_lat[0, 1:-1] = 2.0 * lat[0, :] - lat[1, :]
    extended_lon[0, 1:-1] = 2.0 * lon[0, :] - lon[1, :]
    extended_lat[-1, 1:-1] = 2.0 * lat[-1, :] - lat[-2, :]
    extended_lon[-1, 1:-1] = 2.0 * lon[-1, :] - lon[-2, :]
    extended_lat[1:-1, 0] = 2.0 * lat[:, 0] - lat[:, 1]
    extended_lon[1:-1, 0] = 2.0 * lon[:, 0] - lon[:, 1]
    extended_lat[1:-1, -1] = 2.0 * lat[:, -1] - lat[:, -2]
    extended_lon[1:-1, -1] = 2.0 * lon[:, -1] - lon[:, -2]
    extended_lat[0, 0] = 2.0 * lat[0, 0] - lat[1, 1]
    extended_lon[0, 0] = 2.0 * lon[0, 0] - lon[1, 1]
    extended_lat[0, -1] = 2.0 * lat[0, -1] - lat[1, -2]
    extended_lon[0, -1] = 2.0 * lon[0, -1] - lon[1, -2]
    extended_lat[-1, -1] = 2.0 * lat[-1, -1] - lat[-2, -2]
    extended_lon[-1, -1] = 2.0 * lon[-1, -1] - lon[-2, -2]
    extended_lat[-1, 0] = 2.0 * lat[-1, 0] - lat[-2, 1]
    extended_lon[-1, 0] = 2.0 * lon[-1, 0] - lon[-2, 1]
    lat_pair_sum = extended_lat[:, 1:] + extended_lat[:, :-1]
    lon_pair_sum = extended_lon[:, 1:] + extended_lon[:, :-1]
    corner_grid_lat = (lat_pair_sum[1:, :] + lat_pair_sum[:-1, :]) * 0.25
    corner_grid_lon = (lon_pair_sum[1:, :] + lon_pair_sum[:-1, :]) * 0.25
    lat_corners = np.stack(
        (corner_grid_lat[:-1, :-1], corner_grid_lat[:-1, 1:], corner_grid_lat[1:, 1:], corner_grid_lat[1:, :-1]), axis=-1
    )
    lon_corners = np.stack(
        (corner_grid_lon[:-1, :-1], corner_grid_lon[:-1, 1:], corner_grid_lon[1:, 1:], corner_grid_lon[1:, :-1]), axis=-1
    )
    return lat_corners, lon_corners


def write_scrip(path, lat, lon):
    if lat.ndim != 2 or lon.shape != lat.shape:
        raise ValueError("CWRF latitude and longitude must be two-dimensional arrays with equal shapes")
    nlat, nlon = lat.shape
    lat_corners, lon_corners = calculate_scrip_corners(lat, lon)
    with Dataset(path, "w", format="NETCDF3_64BIT_OFFSET") as data:
        data.createDimension("grid_size", nlat * nlon)
        data.createDimension("grid_corners", 4)
        data.createDimension("grid_rank", 2)
        grid_dims = data.createVariable("grid_dims", "i4", ("grid_rank",))
        center_lat = data.createVariable("grid_center_lat", "f8", ("grid_size",))
        center_lon = data.createVariable("grid_center_lon", "f8", ("grid_size",))
        grid_imask = data.createVariable("grid_imask", "i4", ("grid_size",))
        corner_lat = data.createVariable("grid_corner_lat", "f8", ("grid_size", "grid_corners"))
        corner_lon = data.createVariable("grid_corner_lon", "f8", ("grid_size", "grid_corners"))
        center_lat.units = "degrees"
        center_lon.units = "degrees"
        grid_imask.units = "unitless"
        corner_lat.units = "degrees"
        corner_lon.units = "degrees"
        data.Conventions = "SCRIP"
        data.title = f"curvilinear_to_SCRIP ({nlat},{nlon})"
        grid_dims[:] = (nlon, nlat)
        center_lat[:] = lat.reshape(-1)
        center_lon[:] = lon.reshape(-1)
        grid_imask[:] = 1
        corner_lat[:] = lat_corners.reshape((-1, 4))
        corner_lon[:] = lon_corners.reshape((-1, 4))


def spherical_triangles(lat, lon, context=None):
    """Return NCL-compatible zero-based spherical triangle indexes."""

    return Csstri(lat, lon, context=context).astype(np.int32, copy=False)


def spherical_triangle_area(vertices):
    first, second, third = vertices[:, 0], vertices[:, 1], vertices[:, 2]
    cross_product = np.cross(second, third)
    determinant = np.einsum("ij,ij->i", first, cross_product)
    denominator = 1.0 + np.einsum("ij,ij->i", first, second)
    denominator += np.einsum("ij,ij->i", second, third)
    denominator += np.einsum("ij,ij->i", third, first)
    return 2.0 * np.arctan2(np.abs(determinant), denominator)


def write_esmf_mesh(path, lat, lon, context=None):
    if lat.ndim != 1 or lon.shape != lat.shape:
        raise ValueError("Unstructured latitude and longitude must be one-dimensional arrays with equal shapes")
    triangles = spherical_triangles(lat, lon, context=context)
    vertices_lon = lon[triangles]
    vertices_lat = lat[triangles]
    radians_lat = np.deg2rad(vertices_lat)
    radians_lon = np.deg2rad(vertices_lon)
    vertices_xyz = np.stack(
        (
            np.cos(radians_lat) * np.cos(radians_lon),
            np.cos(radians_lat) * np.sin(radians_lon),
            np.sin(radians_lat),
        ),
        axis=-1,
    )
    centers = np.stack((vertices_lon.mean(axis=1), vertices_lat.mean(axis=1)), axis=1)
    areas = spherical_triangle_area(vertices_xyz)
    with Dataset(path, "w", format="NETCDF3_64BIT_OFFSET") as data:
        data.createDimension("nodeCount", lat.size)
        data.createDimension("elementCount", triangles.shape[0])
        data.createDimension("maxNodePElement", 3)
        data.createDimension("coordDim", 2)
        node_coords = data.createVariable("nodeCoords", "f8", ("nodeCount", "coordDim"))
        element_conn = data.createVariable("elementConn", "i4", ("elementCount", "maxNodePElement"), fill_value=-1)
        node_count = data.createVariable("numElementConn", "i1", ("elementCount",))
        center_coords = data.createVariable("centerCoords", "f8", ("elementCount", "coordDim"))
        element_area = data.createVariable("elementArea", "f8", ("elementCount",))
        element_mask = data.createVariable("elementMask", "i4", ("elementCount",))
        node_coords.units = "degrees"
        element_conn.long_name = "Node Indices that define the element connectivity"
        node_count.long_name = "Number of nodes per element"
        center_coords.units = "degrees"
        element_area.long_name = "area weights"
        element_area.units = "radians^2"
        data.Conventions = "ESMF"
        data.gridType = "unstructured"
        node_coords[:] = np.column_stack((lon, lat))
        element_conn[:] = triangles + 1
        node_count[:] = 3
        center_coords[:] = centers
        element_area[:] = areas
        element_mask[:] = 1


def generate_descriptors(args, grid_dir, output_dir, ocean_file):
    cwrf_lat = read_variable(grid_dir / "geo_em.d01.nc", "XLAT_M")
    cwrf_lon = read_variable(grid_dir / "geo_em.d01.nc", "XLONG_M")
    colm_lat, colm_lon = read_colm_coordinates(grid_dir, args.gridname)
    ocean_lat = read_variable(ocean_file, "lat")
    ocean_lon = read_variable(ocean_file, "lon")
    ocean_latc = read_variable(ocean_file, "latc")
    ocean_lonc = read_variable(ocean_file, "lonc")
    context = NclLibs2PyContext()
    write_esmf_mesh(output_dir / f"fvcom_grd_n_des_{args.gridname}.nc", ocean_lat, ocean_lon, context=context)
    write_esmf_mesh(output_dir / f"fvcom_grd_e_des_{args.gridname}.nc", ocean_latc, ocean_lonc, context=context)
    write_esmf_mesh(output_dir / f"colm_grd_des_{args.gridname}.nc", colm_lat, colm_lon, context=context)
    write_scrip(output_dir / f"cwrf_s_des_{args.gridname}.nc", cwrf_lat, cwrf_lon)


def run_weight(source, destination, output, method, source_type, destination_type, args, log_path):
    command = []
    if args.np > 1:
        mpirun = shutil.which(args.mpirun) or args.mpirun
        command.extend([mpirun, "-n", str(args.np)])
    command.extend(
        [
            str(args.esmf),
            "--ignore_degenerate",
            "--ignore_unmapped",
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
        ]
    )
    if source_type == "ESMFMESH":
        command.extend(["--src_loc", "corner"])
    if destination_type == "ESMFMESH":
        command.extend(["--dst_loc", "corner"])
    if output.exists():
        output.unlink()
    with log_path.open("w", encoding="utf-8") as log_file:
        subprocess.run(command, cwd=Path.cwd(), stdout=log_file, stderr=subprocess.STDOUT, check=True)
    require_file(output)


def generate_weights(args, output_dir):
    for source_name, destination_name, output_name, method, source_type, destination_type in WEIGHT_JOBS:
        source = output_dir / source_name.format(g=args.gridname)
        destination = output_dir / destination_name.format(g=args.gridname)
        output = output_dir / output_name.format(g=args.gridname)
        log_path = output_dir / f"{output.name}.log"
        require_file(source)
        require_file(destination)
        run_weight(source, destination, output, method, source_type, destination_type, args, log_path)


def main():
    args = parse_args()
    if args.np < 1:
        raise ValueError("--np must be greater than zero")
    require_file(args.esmf)
    base_dir = Path.cwd()
    grid_dir = args.grid_dir or base_dir / args.gridname
    output_dir = args.output_dir or grid_dir / "cpl7data"
    ocean_file = args.ocean_file or base_dir / "CN_COAST_restart_modified2019.nc"
    output_dir.mkdir(parents=True, exist_ok=True)
    if not args.weights_only:
        generate_descriptors(args, grid_dir, output_dir, ocean_file)
    generate_weights(args, output_dir)
    print(f"CPL7 Step1 ESMF generation completed: {output_dir}")


if __name__ == "__main__":
    main()
