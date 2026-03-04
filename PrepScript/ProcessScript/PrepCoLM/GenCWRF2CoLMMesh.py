
import os
import time
import warnings
import argparse
import numpy as np
import xarray as xr
from pyproj import CRS, Transformer
from numba import njit, prange
warnings.filterwarnings("ignore", category=UserWarning, module="xarray")



# 解析命令行参数
parser = argparse.ArgumentParser(description='Regrid CWRF grid to WGS84 grid',
                                 epilog=("Example: python GenCWRF2CoLMMesh.py -dx 15000 -dy 15000 -reflat 35.1778 -reflon 110.0 -truelat1 30.0 -truelat2 60.0 -savename mesh_15km.nc -meshsize 3\n"))
parser.add_argument("-dx", type=float, required=True, help="Grid spacing in X direction")
parser.add_argument("-dy", type=float, required=True, help="Grid spacing in Y direction")
parser.add_argument("-reflat", type=float, required=True, help="Reference Latitude")
parser.add_argument("-reflon", type=float, required=True, help="Reference Longitude")
parser.add_argument("-truelat1", type=float, required=True, help="True Latitude 1")
parser.add_argument("-truelat2", type=float, required=True, help="True Latitude 2")
parser.add_argument("-meshsize", type=int, default=2, help="Global mesh sizes (1 for 21600*43200; 2 for 43200*86400; 3 for 86400*172800)")
parser.add_argument("-geofile", type=str, default="./geo_em.d01.nc", help="Path to the CWRF geography file (wrfinput_d01 or geo_em.d01.nc)")
parser.add_argument("-savename", type=str, default="mesh.nc", help="Name of the output mesh file")


# -------------------------------
# 函数定义区
# -------------------------------
def get_latlon_range(clat, clon):
    """获取经纬度范围内的网格点"""
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
    """初始化投影转换器"""
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



@njit(parallel=True,fastmath=False)
def generate_grid_corners_numba(x_center, y_center, dx, dy):
    x_center = x_center.astype(np.float64)
    y_center = y_center.astype(np.float64)
    dx = np.float64(dx)
    dy = np.float64(dy)
    half_dx = dx / 2
    half_dy = dy / 2
    
    ny, nx = x_center.shape
    x_corners = np.empty((ny+1, nx+1), dtype=x_center.dtype)
    y_corners = np.empty((ny+1, nx+1), dtype=y_center.dtype)

    # 并行循环
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
    采用射线法判断点 (x, y) 是否在多边形 poly 内。
    poly 是一个由 (lon, lat) 坐标元组组成的列表，
    要求顶点顺序（顺时针或逆时针）正确。
    """
    inside = False
    # j = poly_lon.shape[0] - 1
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
    """在 CWRF 网格多边形上进行并行化点内判定，返回全局网格索引数组。"""
    nlat, nlon = global_lon2d.shape
    sn = lat_corners.shape[0] - 1
    we = lon_corners.shape[1] - 1

    grid = np.full((nlat, nlon), -9999, np.int32)
    cwrfnum = np.full((sn, we), -9999, np.int32)

    for i in prange(we):
        for j in range(sn):
            # 构造四角多边形坐标数组
            poly_lon = np.empty(4, np.float64)
            poly_lat = np.empty(4, np.float64)
            # 逆时针排序
            poly_lon[0] = lon_corners[j,   i];   poly_lat[0] = lat_corners[j,   i]       # 左下角
            poly_lon[1] = lon_corners[j,   i+1]; poly_lat[1] = lat_corners[j,   i+1]     # 右下角
            poly_lon[2] = lon_corners[j+1, i+1]; poly_lat[2] = lat_corners[j+1, i+1]     # 右上角
            poly_lon[3] = lon_corners[j+1, i];   poly_lat[3] = lat_corners[j+1, i]       # 左上角

            # 计算包围盒
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

            # 查找经纬度索引范围（线性扫描）
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

            # 在包围盒内进行点内多边形判定
            cell_value = i * sn + j + 1
            cwrfnum[j, i] = cell_value
            for ii in range(lat_start, lat_end):
                for jj in range(lon_start, lon_end):
                    if point_in_poly_numba(global_lon2d[ii, jj], global_lat2d[ii, jj], poly_lon, poly_lat):
                        grid[ii, jj] = cell_value
                        
    return grid, cwrfnum



@njit
def is_continuous(grid, missing_value=-9999):
    n_rows, n_cols = grid.shape

    # 定位有效值区域的最小外接矩形
    row_min, row_max = n_rows, -1
    col_min, col_max = n_cols, -1
    for i in range(n_rows):
        for j in range(n_cols):
            if grid[i, j] != missing_value:
                if i < row_min: row_min = i
                if i > row_max: row_max = i
                if j < col_min: col_min = j
                if j > col_max: col_max = j

    # 如果无有效值，则视为连续
    if row_max < 0:
        return True, np.empty(0, np.int64)

    # 在 ROI 内找最小值、最大值并统计
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

    # 标记出现过的值
    length = vmax - vmin + 1
    seen = np.zeros(length, np.bool_)
    for i in range(row_min, row_max + 1):
        for j in range(col_min, col_max + 1):
            v = grid[i, j]
            if v != missing_value:
                seen[v - vmin] = True

    # 收集缺失的编号
    missing_list = []
    for k in range(length):
        if not seen[k]:
            missing_list.append(vmin + k)

    if len(missing_list) == 0:
        return True, np.empty(0, np.int64)
    else:
        return False, np.array(missing_list, np.int64)



def generate_meshgrid(lon_corners, lat_corners, savename, meshsize):
    """Wrapper，生成全局网格并保存为 NetCDF。"""
    if meshsize == 1:
        nlat, nlon = 21600, 43200
    elif meshsize == 2:
        nlat, nlon = 43200, 86400
    elif meshsize == 3:
        nlat, nlon = 86400, 172800
        
    lat_n = 90 - np.arange(nlat) * (180.0 / nlat)
    lat_s = 90 - (np.arange(nlat) + 1) * (180.0 / nlat)
    lon_w = -180 + np.arange(nlon) * (360.0 / nlon)
    lon_e = -180 + (np.arange(nlon) + 1) * (360.0 / nlon)

    global_lon_centers = (lon_w + lon_e) / 2.0
    global_lat_centers = (lat_s + lat_n) / 2.0
    global_lon2d, global_lat2d = np.meshgrid(global_lon_centers, global_lat_centers)

    sigma = 0.002
    # 调用 Numba-加速核心
    global_grid, cwrf_num = meshgrid_numba(
        global_lon2d, global_lat2d,
        lon_corners, lat_corners,
        global_lon_centers, global_lat_centers,
        sigma
    )
    
    continuous, missing_ids = is_continuous(global_grid)
    if continuous:
        print("Element indices are continuous.")
        print("Total elements:", global_grid.max())
    else:
        print("Element indices are NOT continuous.")
        print("Missing element indices:", missing_ids)
        print("Total elements:", global_grid.max() - len(missing_ids))

    # 保存为 NetCDF
    mesh_ds = xr.Dataset(
        {
            "lat_n": ("lat", lat_n),
            "lat_s": ("lat", lat_s),
            "lon_w": ("lon", lon_w),
            "lon_e": ("lon", lon_e),
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
    mesh_ds.to_netcdf(savename, mode="w", format="NETCDF4")
    mesh_ds.close()
    



def check_arguments(args):
    """
    检查命令行参数
    """
    if args.dx <= 0 or args.dy <= 0:
        raise ValueError("Grid spacing (dx, dy) must be positive.")
    if args.meshsize not in [1, 2, 3]:
        raise ValueError("Mesh size must be 1, 2 or 3.")
    if not os.path.exists(args.geofile):
        raise FileNotFoundError(f"File {args.geofile} does not exist.")
    if (args.dx <=1500. or args.dy <= 1500.) and args.meshsize == 1:
        print("")
        print("    meshsize=1 : mesh size is 21600*43200")
        print("    meshsize=2 : mesh size is 43200*86400")
        print("    meshsize=3 : mesh size is 86400*172800")
        print("")
        raise ValueError("For dx or dy < 1500, mesh size must be 2 or 3.")



def main(args):
    """
    Main function to regrid CWRF data to WGS84 grid
    """
    # 读取wrfinput_d01文件
    time1 = time.time()
    cwrf_ds = xr.open_dataset(args.geofile)
    print("\nGetting target grid center coordinates...")
    lat_center = cwrf_ds["CLAT"].isel(Time=0).data
    lon_center = cwrf_ds["CLONG"].isel(Time=0).data
    latrange, lonrange = get_latlon_range(lat_center, lon_center)
    print("\nInitializing transformer...")
    transformer = init_transformer(args)
    x_center, y_center = transformer.transform(lon_center, lat_center) # lat, lon(degree) -> x, y(meters)
    print("\nGenerating grid center coordinates (Lambert)...")
    x_corners_proj, y_corners_proj = generate_grid_corners_numba(x_center, y_center, args.dx, args.dy) # x, y (meters)
    print("\nGenerating grid corner coordinates (WGS84)...")
    lon_corners, lat_corners = transformer.transform(x_corners_proj, y_corners_proj, direction="INVERSE") # x, y(meters) -> lon, lat(degree)
    print("\nGenerating mesh grid...")
    generate_meshgrid(lon_corners, lat_corners, args.savename, args.meshsize)    
    print(f"\nMesh grid saved to {args.savename}")
    print(f"\nTotal time: {time.time() - time1:.2f} seconds")
    
    
    
if __name__ == "__main__":
    args = parser.parse_args()
    check_arguments(args)
    main(args)
