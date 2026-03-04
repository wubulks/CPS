#!/usr/bin/env python3
import os
import time
import argparse
import pandas as pd
import numpy as np
import xarray as xr
from pyproj import CRS, Transformer
from joblib import Parallel, delayed
from scipy.interpolate import griddata
from netCDF4 import Dataset
from numba import njit, prange
from tqdm import tqdm
import warnings

warnings.filterwarnings("ignore", category=UserWarning, module="xarray")

time1 = time.time()


# 解析命令行参数
parser = argparse.ArgumentParser(description='Regrid WGS84 grid to CWRF grid')
parser.add_argument("-dx", type=float, default=5000, help="Grid spacing in X direction")
parser.add_argument("-dy", type=float, default=5000, help="Grid spacing in Y direction")
parser.add_argument("-reflat", type=float, default=29.5, help="Reference Latitude")
parser.add_argument("-reflon", type=float, default=114.3, help="Reference Longitude")
parser.add_argument("-truelat1", type=float, default=27, help="True Latitude 1")
parser.add_argument("-truelat2", type=float, default=33, help="True Latitude 2")
parser.add_argument("-geofile", type=str, default="./wrfinput_d01", help="Path to the CWRF geography file (wrfinput_d01 or geo_em.d01.nc)")
parser.add_argument("-cpu", type=int, default=16, help="Number of cpu cores to use")
parser.add_argument("-nco", type=str, default="ncks", help="NCO command path")

print("[INFO] Regrid CoLM soil parameters to CWRF grid")


SOILDATA = '/shr03/CoLMrawdata/soil/'



# -------------------------------
# 函数定义区
# -------------------------------
def get_latlon_range(clat, clon):
    """Get latitude and longitude range of the target grid points."""
    latmax = np.max(clat)
    latmin = np.min(clat)
    lonmax = np.max(clon)
    lonmin = np.min(clon)
    offset = 2.5
    if latmax < 70:
        latmax = latmax + offset
    if latmin > -70:
        latmin = latmin - offset
    if lonmax < 170:
        lonmax = lonmax + offset
    if lonmin > -170:
        lonmin = lonmin - offset
    latrange = (float(latmin), float(latmax))
    lonrange = (float(lonmin), float(lonmax))
    return latrange, lonrange



def init_transformer(args):
    """Initialize Lambert Conformal projection transformer."""
    crs_wrf = CRS.from_proj4(
        f"+proj=lcc "
        f"+lat_1={args.truelat1} "
        f"+lat_2={args.truelat2} "
        f"+lat_0={args.reflat} "
        f"+lon_0={args.reflon} "
        f"+a=6370000 "
        f"+b=6370000 "
        f"+units=m"
    )
    transformer = Transformer.from_crs(crs_wrf.geodetic_crs, crs_wrf)
    return transformer



@njit(parallel=True, fastmath=False)
def generate_grid_corners_numba(x_center, y_center, dx, dy):
    """Generate grid corners (projected Lambert coordinates) from grid centers."""
    x_center = x_center.astype(np.float64)
    y_center = y_center.astype(np.float64)
    dx = np.float64(dx)
    dy = np.float64(dy)
    half_dx = dx / 2
    half_dy = dy / 2
    
    ny, nx = x_center.shape
    x_corners = np.empty((ny + 1, nx + 1), dtype=x_center.dtype)
    y_corners = np.empty((ny + 1, nx + 1), dtype=y_center.dtype)

    # Parallel loop
    for i in prange(ny):
        for j in prange(nx):
            xc = x_center[i, j]
            yc = y_center[i, j]
            x_corners[i,   j]   = xc - half_dx
            x_corners[i,   j+1] = xc + half_dx
            x_corners[i+1, j]   = xc - half_dx
            x_corners[i+1, j+1] = xc + half_dx

            y_corners[i,   j]   = yc - half_dy
            y_corners[i,   j+1] = yc - half_dy
            y_corners[i+1, j]   = yc + half_dy
            y_corners[i+1, j+1] = yc + half_dy
    return x_corners, y_corners



@njit
def point_in_poly_numba(x, y, poly_lon, poly_lat):
    """
    Ray casting algorithm: check if a point (x, y) lies inside a polygon.
    poly_lon and poly_lat are arrays of polygon vertices (ordered clockwise or counterclockwise).
    """
    inside = False
    num = 4
    j = num - 1
    for i in range(poly_lon.shape[0]):
        xi = poly_lon[i]; yi = poly_lat[i]
        xj = poly_lon[j]; yj = poly_lat[j]
        if ((yi > y) != (yj > y)) and (x < (xj - xi) * (y - yi) / (yj - yi + 1e-12) + xi):
            inside = not inside
        j = i
    return inside



@njit(parallel=True)
def meshgrid_numba(global_lon2d, global_lat2d,
                   lon_corners, lat_corners,
                   global_lon_centers, global_lat_centers,
                   sigma):
    """Parallelized point-in-polygon check for CWRF grid cells, returning global index array."""
    nlat, nlon = global_lon2d.shape
    sn = lat_corners.shape[0] - 1
    we = lon_corners.shape[1] - 1

    grid = np.full((nlat, nlon), -9999, np.int32)
    cwrfnum = np.full((sn, we), -9999, np.int32)

    for i in prange(we):
        for j in range(sn):
            # Build quadrilateral polygon
            poly_lon = np.empty(4, np.float64)
            poly_lat = np.empty(4, np.float64)
            # Counter-clockwise order
            poly_lon[0] = lon_corners[j,   i];   poly_lat[0] = lat_corners[j,   i]       # lower-left
            poly_lon[1] = lon_corners[j,   i+1]; poly_lat[1] = lat_corners[j,   i+1]     # lower-right
            poly_lon[2] = lon_corners[j+1, i+1]; poly_lat[2] = lat_corners[j+1, i+1]     # upper-right
            poly_lon[3] = lon_corners[j+1, i];   poly_lat[3] = lat_corners[j+1, i]       # upper-left

            # Bounding box
            min_lon = poly_lon[0]; max_lon = poly_lon[0]
            min_lat = poly_lat[0]; max_lat = poly_lat[0]
            for k in range(1, 4):
                if poly_lon[k] < min_lon: min_lon = poly_lon[k]
                if poly_lon[k] > max_lon: max_lon = poly_lon[k]
                if poly_lat[k] < min_lat: min_lat = poly_lat[k]
                if poly_lat[k] > max_lat: max_lat = poly_lat[k]
            min_lon -= sigma
            max_lon += sigma
            min_lat -= sigma
            max_lat += sigma

            # Find candidate index ranges
            lon_start = 0
            while lon_start < nlon and global_lon_centers[lon_start] < min_lon:
                lon_start += 1
            lon_end = lon_start
            while lon_end < nlon and global_lon_centers[lon_end] <= max_lon:
                lon_end += 1
            lat_start = 0
            while lat_start < nlat and global_lat_centers[lat_start] > max_lat:
                lat_start += 1
            lat_end = lat_start
            while lat_end < nlat and global_lat_centers[lat_end] >= min_lat:
                lat_end += 1

            # Assign values inside polygon
            cell_value = i * sn + j + 1
            cwrfnum[j, i] = cell_value
            for ii in range(lat_start, lat_end):
                for jj in range(lon_start, lon_end):
                    if point_in_poly_numba(global_lon2d[ii, jj], global_lat2d[ii, jj], poly_lon, poly_lat):
                        grid[ii, jj] = cell_value
                        
    return grid, cwrfnum



@njit
def is_continuous(grid, missing_value=-9999):
    """Check if grid indices are continuous (no gaps in numbering)."""
    n_rows, n_cols = grid.shape

    # Locate bounding box of valid values
    row_min, row_max = n_rows, -1
    col_min, col_max = n_cols, -1
    for i in range(n_rows):
        for j in range(n_cols):
            if grid[i, j] != missing_value:
                if i < row_min: row_min = i
                if i > row_max: row_max = i
                if j < col_min: col_min = j
                if j > col_max: col_max = j

    # No valid values -> considered continuous
    if row_max < 0:
        return True, np.empty(0, np.int64)

    # Find min/max and count values
    vmin = np.iinfo(np.int64).max
    vmax = np.iinfo(np.int64).min
    count = 0
    for i in range(row_min, row_max + 1):
        for j in range(col_min, col_max + 1):
            v = grid[i, j]
            if v != missing_value:
                count += 1
                if v < vmin: vmin = v
                if v > vmax: vmax = v

    if count == 0:
        return True, np.empty(0, np.int64)

    # Track seen values
    length = vmax - vmin + 1
    seen = np.zeros(length, np.bool_)
    for i in range(row_min, row_max + 1):
        for j in range(col_min, col_max + 1):
            v = grid[i, j]
            if v != missing_value:
                seen[v - vmin] = True

    # Collect missing IDs
    missing_list = []
    for k in range(length):
        if not seen[k]:
            missing_list.append(vmin + k)

    if len(missing_list) == 0:
        return True, np.empty(0, np.int64)
    else:
        return False, np.array(missing_list, np.int64)


def interpolate_nan_2d(data: np.ndarray,
                            method = 'linear',
                            invalid_value = None):
    """
    仅对 NaN 像元插值；linear/cubic 的凸包外点再用最近邻兜底。
    invalid_value（如 -9999）保持不变。
    """
    arr = data.astype(float, copy=True)
    nan_mask = np.isnan(arr)

    ny, nx = arr.shape
    yy, xx = np.mgrid[0:ny, 0:nx]  # 比 meshgrid 更省内存

    # 有效观测点（非 NaN 且 ≠ invalid_value）
    if invalid_value is None:
        valid_mask = ~nan_mask
    else:
        valid_mask = (~nan_mask) & (arr != invalid_value)

    if not np.any(nan_mask):
        return arr  # 没有 NaN，直接返回
    if not np.any(valid_mask):
        # 没有可用观测，直接返回（或 raise）
        return arr

    pts = np.column_stack((xx[valid_mask], yy[valid_mask]))
    vals = arr[valid_mask]

    # 目标：仅 NaN 位置
    tgt_idx = np.where(nan_mask)
    tgt_pts = np.column_stack((xx[tgt_idx], yy[tgt_idx]))

    # 1) 首选插值（只算 NaN 位置）
    filled = griddata(pts, vals, tgt_pts, method=method)

    # 2) 兜底：linear/cubic 在凸包外会得到 NaN，用最近邻补齐
    if method in ('linear', 'cubic'):
        miss = np.isnan(filled)
        if np.any(miss):
            tree = cKDTree(pts)
            _, nn_idx = tree.query(tgt_pts[miss], k=1)
            filled[miss] = vals[nn_idx]

    # 写回原数组，仅覆盖原 NaN
    arr[tgt_idx] = filled
    return arr



def cal_median(arr, defval=np.nan):
    """计算非空数组的中位数，若全为空则返回 defval"""
    arr = np.asarray(arr)
    arr_valid = arr[~np.isnan(arr)]
    if arr_valid.size == 0:
        return defval
    return np.median(arr_valid)



def clip_soil_data(args):
    """根据 lat/lon 范围裁剪土壤数据（按行列 index）"""
    var, latrange, lonrange, soildir, nco = args

    file = f"{soildir}/{var}.nc"
    if not os.path.exists(file):
        print(f"    [!] 没有找到 {var} 的土壤数据文件: {file}")
        exit(1)

    # 🌍 以全球15秒分辨率（0.0041666667度）为基础推算索引
    dlat = 0.0041666667
    dlon = 0.0041666667

    # 纬度是从 90 → -90，故行数反着来
    lat_start = int((90.0 - latrange[0]) / dlat)
    lat_end   = int((90.0 - latrange[1]) / dlat)

    # 经度从 -180 → 180
    lon_start = int((lonrange[0] + 180.0) / dlon)
    lon_end   = int((lonrange[1] + 180.0) / dlon)

    # nco 的 -d 是 index 形式（必须 lat_start < lat_end）
    lat_index_start = min(lat_start, lat_end)
    lat_index_end   = max(lat_start, lat_end)
    lon_index_start = min(lon_start, lon_end)
    lon_index_end   = max(lon_start, lon_end)

    # 删除已有的裁剪文件（如果存在）
    if os.path.exists(f"./{var}.nc"):
        os.remove(f"./{var}.nc")

    # 构建 NCO 裁剪命令（按 index）
    command = (
        f'{nco} -O '
        f'-d latitude,{lat_index_start},{lat_index_end} '
        f'-d longitude,{lon_index_start},{lon_index_end} '
        f'{file} ./{var}.nc'
    )

    os.system(command)
    return f"./{var}.nc"




def generate_meshgrid(lon_corners, lat_corners, latrange, lonrange):
    """Wrapper to generate global grid index and save as NetCDF."""
    nlat, nlon = 43200, 86400
    lat_n = 90 - np.arange(nlat) * (180.0 / nlat)
    lat_s = 90 - (np.arange(nlat) + 1) * (180.0 / nlat)
    lon_w = -180 + np.arange(nlon) * (360.0 / nlon)
    lon_e = -180 + (np.arange(nlon) + 1) * (360.0 / nlon)

    global_lon_centers = (lon_w + lon_e) / 2.0
    global_lat_centers = (lat_s + lat_n) / 2.0

    # 🌍 以全球15秒分辨率（0.0041666667度）为基础推算索引
    dlat = 0.0041666667
    dlon = 0.0041666667

    # 纬度是从 90 → -90，故行数反着来
    lat_start = int((90.0 - latrange[0]) / dlat)
    lat_end   = int((90.0 - latrange[1]) / dlat)

    # 经度从 -180 → 180
    lon_start = int((lonrange[0] + 180.0) / dlon)
    lon_end   = int((lonrange[1] + 180.0) / dlon)

    # nco 的 -d 是 index 形式（必须 lat_start < lat_end）
    lat_index_start = min(lat_start, lat_end)
    lat_index_end   = max(lat_start, lat_end)
    lon_index_start = min(lon_start, lon_end)
    lon_index_end   = max(lon_start, lon_end)

    global_lon_centers = global_lon_centers[lon_index_start:lon_index_end+1]
    global_lat_centers = global_lat_centers[lat_index_start:lat_index_end+1]

    global_lon2d, global_lat2d = np.meshgrid(global_lon_centers, global_lat_centers)

    sigma = 0.002
    # Run numba-accelerated routine
    global_grid, cwrf_num = meshgrid_numba(
        global_lon2d, global_lat2d,
        lon_corners, lat_corners,
        global_lon_centers, global_lat_centers,
        sigma
    )
    
    continuous, missing_ids = is_continuous(global_grid)
    if continuous:
        print("[INFO] Element indices are continuous.")
        print(f"[INFO] Total elements: {global_grid.max()}")
    else:
        print("[WARNING] Element indices are NOT continuous.")
        print("[WARNING] Missing element indices:", missing_ids)
        print(f"[INFO] Total elements (adjusted): {global_grid.max() - len(missing_ids)}")

    # Save to NetCDF
    mesh_ds = xr.Dataset(
        {
            "elmindex": (("lat", "lon"), global_grid),
            "longitude": ("lon", global_lon_centers),
            "latitude": ("lat", global_lat_centers),
            "meshnum": (("y", "x"), cwrf_num),
        },
        coords={
            "lat": np.float32(global_lat_centers),
            "lon": np.float32(global_lon_centers),
            "y": np.arange(cwrf_num.shape[0], dtype=np.int32),
            "x": np.arange(cwrf_num.shape[1], dtype=np.int32),
        },
    )
    mesh_ds.attrs["description"] = "CWRF mesh grid"
    mesh_ds.coords["lat"].attrs["units"] = "degrees_north"
    mesh_ds.coords["lon"].attrs["units"] = "degrees_east"
    mesh_ds.coords["lat"].attrs["long_name"] = "latitude"
    mesh_ds.coords["lon"].attrs["long_name"] = "longitude"
    mesh_ds.coords["lat"].attrs["standard_name"] = "latitude"
    mesh_ds.coords["lon"].attrs["standard_name"] = "longitude"
    savename = "CWRF_meshgrid.nc"
    mesh_ds.to_netcdf(savename, mode="w", format="NETCDF4")
    mesh_ds.close()
    print(f"[INFO] Mesh grid saved to {savename}")
    return savename



@njit(parallel=True, fastmath=True)
def aggregate_numba(soil_merge, elmindex_flat, meshnum_flat, ny, nx):
    """
    Fast aggregation using Numba (parallel).
    soil_merge: (8, npoints)
    elmindex_flat: (npoints,) global elmindex
    meshnum_flat: (ny*nx,) mesh grid mapping
    """
    soil = np.full((8, ny, nx), np.nan, dtype=np.float32)
    max_idx = meshnum_flat.max() + 1

    for m in prange(8):  # parallel over months
        sums = np.zeros(max_idx, dtype=np.float64)
        counts = np.zeros(max_idx, dtype=np.int64)

        for i in range(elmindex_flat.size):
            idx = elmindex_flat[i]
            if idx > 0:
                val = soil_merge[m, i]
                if not np.isnan(val):
                    sums[idx] += val
                    counts[idx] += 1

        means = np.full(max_idx, np.nan, dtype=np.float32)
        for i in range(max_idx):
            if counts[i] > 0:
                means[i] = sums[i] / counts[i]

        for i in range(meshnum_flat.size):
            soil[m, i // nx, i % nx] = means[meshnum_flat[i]]

    return soil




def aggregate_data(soildir, mesh_file, oldname, newname):
    """
    Aggregation driver: calls fast Numba kernel and saves result to NetCDF.
    """
    soildatafile = f"./{oldname}.nc"
    soil_ds = xr.open_dataset(soildatafile)

    mesh_ds = xr.open_dataset(mesh_file)
    elmindex = mesh_ds["elmindex"].values.astype(np.int32)   # shape (ny_global, nx_global)
    meshnum  = mesh_ds["meshnum"].values.astype(np.int32)    # shape (ny, nx)
    mesh_ds.close()

    ny, nx = meshnum.shape
    meshnum_flat = meshnum.ravel()
    elmindex_flat = elmindex.ravel()

    varlist = [f'{oldname}_l{layer}' for layer in range(1, 9)]
    soil_merged_da = xr.concat([soil_ds[var] for var in varlist], dim="layer")
    soil_merged_da = soil_merged_da.rename(newname)
    soil_merged_da = soil_merged_da.transpose("layer", "latitude", "longitude")
    soil_merge_flat = soil_merged_da.values.reshape(8, -1)  # shape (8, npoints)
    
    # run numba kernel
    soil = aggregate_numba(soil_merge_flat, elmindex_flat, meshnum_flat, ny, nx)

    # post-process
    soil[soil < 0] = 0.0
    soil[np.isnan(soil)] = 0.0

    soil_da = xr.DataArray(
        soil,
        dims=("layer", "south_north", "west_east"),
        coords={
            "layer": np.arange(8, dtype=np.int32),
            "south_north": np.arange(ny, dtype=np.int32),
            "west_east": np.arange(nx, dtype=np.int32),
        },
        name=newname
    )
    soil_ds = soil_da.to_dataset(name=newname)
    output_file = f"{newname}.nc"
    soil_ds.to_netcdf(output_file, mode="w", format="NETCDF4")

    print(f"[INFO] Aggregated data saved to {output_file}")



def write_to_wrfinput(wrfinppath, soildicts):
    """将处理后的土壤参数写入 wrfinput_d01 文件"""
    new_wrfinppath = f"{wrfinppath}.colm"
    os.system(f"cp {wrfinppath} {new_wrfinppath}")
    infil = Dataset(new_wrfinppath, "r+")
    fr_clay = infil.variables['FR_CLAY']
    scwat = infil.variables['SC_WATER'][0, :, :]
    inland = np.where(scwat !=8, 1, 0)  # inland = 1 if SC_WATER != 8, else 0
    nsoil, nlat, nlon = fr_clay.shape[1:]  # 忽略时间维度
    for oldvar, newname in soildicts.items():
        cwrfvarname = newname.upper()
        soilfil = xr.open_dataset(f"./{newname}.nc")
        if cwrfvarname not in infil.variables:
            infil.createVariable(cwrfvarname, fr_clay.datatype, fr_clay.dimensions)
            #复制属性
            for attr in fr_clay.ncattrs():
                infil.variables[cwrfvarname].setncattr(attr, fr_clay.getncattr(attr))
        
        var = soilfil[newname].data
        var = np.array(var, dtype=float)
        missing_value = soilfil[newname].attrs.get('missing_value', -9999)
        print(f"    Processing variable: {cwrfvarname}")
        for i in range(8):
            varlayer = var[i,:,:]
            varlayer[inland == 0] = -9999
            varlayer[(inland==1) & (varlayer <= 0)] = np.nan  # inland区域小于0的值设为-9999
            newvarlayer= interpolate_nan_2d(varlayer, method='nearest', invalid_value=missing_value)
            newvarlayer[inland == 0] = np.nan  # inland区域设为NaN
            var[i,:,:] = newvarlayer
        new_var = np.zeros((nsoil, nlat, nlon))
        new_var[1:9, :, :] = var[:, :, :]
        new_var[0, :, :] = var[0, :, :]
        new_var[-2, :, :] = var[-1, :, :]
        new_var[-1, :, :] = var[-1, :, :]
        infil.variables[cwrfvarname][0, :, :, :] = new_var
    
    vf_gravels_s = infil.variables['xns_vf_gravels'.upper()][0, :, :, :]
    vf_sand_s = infil.variables['xns_vf_sand'.upper()][0, :, :, :]

    vf_sum = vf_gravels_s + vf_sand_s

    BA_alpha_tmp = np.where(vf_sum > 0.4, 0.38,
                    np.where(vf_sum > 0.25, 0.24, 0.2))
    
    BA_beta_tmp = np.where(vf_sum > 0.4, 35.0,
                    np.where(vf_sum > 0.25, 26.0, 10.0))
    BA_alpha = cal_median(BA_alpha_tmp, np.nan)
    BA_beta  = cal_median(BA_beta_tmp, np.nan)
    cwrfvarname = 'xns_BA_alpha'.upper()
    if cwrfvarname not in infil.variables:
        infil.createVariable(cwrfvarname, fr_clay.datatype, fr_clay.dimensions)
        for attr in fr_clay.ncattrs():
            infil.variables[cwrfvarname].setncattr(attr, fr_clay.getncattr(attr))
    infil.variables[cwrfvarname][0, :, :, :] = BA_alpha
    cwrfvarname = 'xns_BA_beta'.upper()
    if cwrfvarname not in infil.variables:
        infil.createVariable(cwrfvarname, fr_clay.datatype, fr_clay.dimensions)
        for attr in fr_clay.ncattrs():
            infil.variables[cwrfvarname].setncattr(attr, fr_clay.getncattr(attr))
    infil.variables[cwrfvarname][0, :, :, :] = BA_beta
    infil.close()



# -------------------------------
# 主流程
# -------------------------------
def main(soildir, wrfinppath, dx, dy, cpu, nco, CWRF_proj_params):
    soildicts = {
        # old names          : new names
        "vf_quartz_mineral_s": "xns_vf_quartz",
        "vf_gravels_s"       : "xns_vf_gravels",
        "vf_om_s"            : "xns_vf_om",
        "vf_sand_s"          : "xns_vf_sand",
        "wf_gravels_s"       : "xns_wf_gravels",
        "wf_sand_s"          : "xns_wf_sand",
        "theta_s"            : "xns_porsl",
        "csol"               : "xns_csol",
        "k_solids"           : "xns_k_solids",
        "tksatf"             : "xns_dksatf",
        "tkdry"              : "xns_dkdry",
        "tksatu"             : "xns_dksatu",
    }
    soilvars = list(soildicts.keys())

    print("\n正在加载数据...")
    cwrf_ds = xr.open_dataset(wrfinppath)
    print(f"[INFO] CWRF grid size: {cwrf_ds.sizes['south_north']} x {cwrf_ds.sizes['west_east']}")

    print("\n[INFO] Extracting target grid center coordinates...")
    lat_center = cwrf_ds["CLAT"].isel(Time=0).data
    lon_center = cwrf_ds["CLONG"].isel(Time=0).data
    latrange, lonrange = get_latlon_range(lat_center, lon_center)
    print(f"[INFO] Target domain: lat {latrange[0]:.2f} ~ {latrange[1]:.2f}, lon {lonrange[0]:.2f} ~ {lonrange[1]:.2f}")
    
    print("\n[INFO] Initializing Lambert transformer...")
    time1 = time.time()
    transformer = init_transformer(args)
    print("[INFO] Transforming grid centers (WGS84 → Lambert)...")
    x_center, y_center = transformer.transform(lon_center, lat_center)
    print("[INFO] Generating grid corners (Lambert)...")
    x_corners_proj, y_corners_proj = generate_grid_corners_numba(x_center, y_center, args.dx, args.dy)
    print("[INFO] Converting grid corners back to WGS84...")
    lon_corners, lat_corners = transformer.transform(x_corners_proj, y_corners_proj, direction="INVERSE")
    print("[INFO] Generating mesh grid (final)...")
    mesh_file = generate_meshgrid(lon_corners, lat_corners, latrange, lonrange)    
    print(f"[INFO] Total time elapsed: {time.time() - time1:.2f} s")

    print("\nClip soil data to target domain...")
    time2 = time.time()
    clip_args = [(var, latrange, lonrange, soildir, nco) for var in soilvars]
    with Parallel(n_jobs=cpu, backend="loky",return_as="generator") as parallel:
        gen = parallel(delayed(clip_soil_data)(arg) for arg in clip_args)
        for _ in tqdm(gen, total=len(clip_args), desc="Clipping soil data", ncols=80, unit="file"):
            pass
    print(f"[INFO] Total clipping time elapsed: {time.time() - time2:.2f} s")

    print("\nAggregate data with elmindex and save as needed for CWRF...")
    time3 = time.time()
    for oldvar, newvar in soildicts.items():
        aggregate_data(soildir, mesh_file, oldvar, newvar)
    print(f"[INFO] Total aggregation time elapsed: {time.time() - time3:.2f} s")

    print("\nWrite to wrfinput file")
    time4 = time.time()
    write_to_wrfinput(wrfinppath, soildicts)
    print(f"[INFO] Total write time elapsed: {time.time() - time4:.2f} s")

    print("\n[INFO] All tasks completed.")


if __name__ == "__main__":
    args = parser.parse_args()
    dx = args.dx
    dy = args.dy
    cpu = args.cpu
    nco = args.nco
    geofile = args.geofile

    CWRF_proj_params = {
        "proj": "lcc",
        "lat_1": args.truelat1,
        "lat_2": args.truelat2,
        "lat_0": args.reflat,
        "lon_0": args.reflon,
        "a": 6370000,
        "b": 6370000,
        "units": "m"
    }
    
    # 调用主函数
    main(SOILDATA, geofile, dx, dy, cpu, nco, CWRF_proj_params)
    
    
    time2 = time.time()
    print(f"总耗时: {time2 - time1:.2f} 秒")
    
    
