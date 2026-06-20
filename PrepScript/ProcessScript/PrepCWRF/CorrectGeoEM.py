import os
import time
import argparse
import numpy as np
from netCDF4 import Dataset
import xarray as xr
from numba import njit, prange
from pyproj import CRS, Transformer

###################### - 函数定义区 - ######################
def refine_ocean_with_landuse(mask: np.ndarray,
                              landusef: np.ndarray,
                              land_threshold: float = 0.5,
                              water_index: int = 15) -> np.ndarray:
    """
    使用 `LANDUSEF` 对海洋掩膜做一次保守修正。

    本函数只做一件事：对于当前已经判定为海洋的格点，检查 `LANDUSEF`
    中“非水体类别”的总比例是否过高。如果陆地类别总和超过给定阈值，
    则认为该格点更像陆地，将其从海洋掩膜中移除。

    参数
    ----------
    mask : np.ndarray
        二维海洋掩膜，约定 `1=海洋`，`0=非海洋`。
    landusef : np.ndarray
        三维土地利用比例数组，形状必须为 `(n_types, n_lat, n_lon)`。
    land_threshold : float, default=0.5
        陆地比例阈值。仅当非水体类别总和严格大于该阈值时，
        海洋格点才会被翻转为陆地。
    water_index : int, default=15
        水体类别在 `LANDUSEF` 第一维中的索引。

    返回
    ----------
    np.ndarray
        修正后的二维海洋掩膜，类型为 `uint8`，仍满足 `1=海洋`、`0=陆地/湖泊`。
    """
    assert landusef.ndim == 3, "landusef 应为 (n_types, n_lat, n_lon)"
    assert landusef.shape[1:] == mask.shape, "landusef 和 mask 空间维度需一致"
    assert landusef.shape[0] > water_index, "landusef 第一维长度必须 > water_index"

    # 陆地比例总和 = 所有类型之和 - 水体比例（第 water_index 层）
    land_sum = landusef.sum(axis=0) - landusef[water_index]
    mask_out = mask.astype(np.uint8).copy()
    flip_to_land = (mask_out == 1) & (land_sum > land_threshold)
    mask_out[flip_to_land] = 0
    return mask_out



def get_latlon_range(clat: np.ndarray,
                     clon: np.ndarray,
                     offset: float = 0.25) -> tuple[tuple[float, float], tuple[float, float]]:
    """
    计算二维经纬度场的包络范围，并在四周增加一个小的安全边界。

    该函数主要用于从高分辨率全球经纬网格中裁剪出 CWRF 域对应的局部窗口，
    减少后续点落区匹配时的内存与计算开销。

    参数
    ----------
    clat, clon : np.ndarray
        二维纬度、经度数组，通常为 CWRF 网格中心点或角点坐标。
    offset : float, default=0.25
        向外扩展的经纬度安全边界，单位为度。

    返回
    ----------
    tuple[tuple[float, float], tuple[float, float]]
        `(latrange, lonrange)`，其中
        `latrange = (lat_min, lat_max)`，
        `lonrange = (lon_min, lon_max)`。
    """
    latmax = float(np.max(clat))
    latmin = float(np.min(clat))
    lonmax = float(np.max(clon))
    lonmin = float(np.min(clon))
    latrange = (max(-90.0, latmin - offset), min(90.0, latmax + offset))
    lonrange = (max(-180.0, lonmin - offset), min(180.0, lonmax + offset))
    return latrange, lonrange



def init_transformer_from_geoem(infil: Dataset) -> Transformer:
    """
    根据 `geo_em` 文件的投影属性构造 WGS84 与 Lambert Conformal 间的转换器。

    当前脚本的高分辨率网格匹配逻辑优先使用真实的 CWRF 网格角点。
    为此需要先将 `XLAT_M/XLONG_M` 中心点投影到 Lambert 平面，再结合 `DX/DY`
    反推角点位置。本函数负责从 `geo_em` 全局属性中提取所需投影参数。

    参数
    ----------
    infil : netCDF4.Dataset
        已打开的 `geo_em` 文件句柄。

    返回
    ----------
    pyproj.Transformer
        支持 `WGS84 -> Lambert` 与 `Lambert -> WGS84` 双向转换的投影器。

    异常
    ----------
    ValueError
        当 `MAP_PROJ` 不是 Lambert Conformal (`MAP_PROJ=1`) 时抛出。
    """
    map_proj = int(getattr(infil, "MAP_PROJ", 1))
    if map_proj != 1:
        raise ValueError(f"当前仅支持 Lambert Conformal (MAP_PROJ=1)，实际为 {map_proj}")

    truelat1 = float(getattr(infil, "TRUELAT1"))
    truelat2 = float(getattr(infil, "TRUELAT2"))
    reflat = float(getattr(infil, "MOAD_CEN_LAT", getattr(infil, "CEN_LAT")))
    reflon = float(getattr(infil, "STAND_LON"))
    earth_radius = float(getattr(infil, "RADIUS_EARTH", 6370000.0))

    crs_wrf = CRS.from_proj4(
        f"+proj=lcc "
        f"+lat_1={truelat1} "
        f"+lat_2={truelat2} "
        f"+lat_0={reflat} "
        f"+lon_0={reflon} "
        f"+a={earth_radius} "
        f"+b={earth_radius} "
        f"+units=m"
    )
    return Transformer.from_crs(crs_wrf.geodetic_crs, crs_wrf, always_xy=True)



@njit(fastmath=False)
def generate_grid_corners_numba(x_center, y_center, dx, dy):
    """
    根据投影平面上的格点中心坐标与网格距，构造规则四边形角点。

    这里假设 CWRF 网格在投影平面上是规则矩形网格，即每个格点
    都可以由 `(center_x ± dx/2, center_y ± dy/2)` 唯一确定四个角点。
    该假设与 WRF/CWRF 在 Lambert 平面中的离散方式一致。

    参数
    ----------
    x_center, y_center : np.ndarray
        二维中心点投影坐标，形状相同。
    dx, dy : float
        投影平面上的网格距，单位通常为米。

    返回
    ----------
    tuple[np.ndarray, np.ndarray]
        `(x_corners, y_corners)`，形状均为 `(ny+1, nx+1)`。

    说明
    ----------
    这里不使用 `parallel=True`。原因是相邻格点会写入共享角点，
    并行写同一位置虽然理论上值应一致，但仍可能产生不必要的竞争风险。
    """
    x_center = x_center.astype(np.float64)
    y_center = y_center.astype(np.float64)
    dx = np.float64(dx)
    dy = np.float64(dy)
    half_dx = dx / 2.0
    half_dy = dy / 2.0

    ny, nx = x_center.shape
    x_corners = np.empty((ny + 1, nx + 1), dtype=x_center.dtype)
    y_corners = np.empty((ny + 1, nx + 1), dtype=y_center.dtype)

    for i in prange(ny):
        for j in range(nx):
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



def approximate_corners_from_centers(lons: np.ndarray,
                                     lats: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """
    在无法获取投影参数时，根据中心点经纬度场近似外推角点经纬度。

    该方法不是严格几何意义上的真实角点，只是一个降级回退方案。
    它通过相邻中心点中点先估算边界，再沿南北方向外推角点。
    当 `geo_em` 缺少必要投影元数据、或投影转换失败时，脚本仍可继续运行。

    参数
    ----------
    lons, lats : np.ndarray
        二维中心点经纬度数组，形状必须完全一致。

    返回
    ----------
    tuple[np.ndarray, np.ndarray]
        近似的 `(lon_corners, lat_corners)`，形状均为 `(ny+1, nx+1)`。
    """
    ny, nx = lons.shape
    lon_edges_x = np.empty((ny, nx + 1), dtype=np.float64)
    lat_edges_x = np.empty((ny, nx + 1), dtype=np.float64)

    lon_edges_x[:, 1:nx] = 0.5 * (lons[:, :-1] + lons[:, 1:])
    lat_edges_x[:, 1:nx] = 0.5 * (lats[:, :-1] + lats[:, 1:])
    lon_edges_x[:, 0] = lons[:, 0] - 0.5 * (lons[:, 1] - lons[:, 0])
    lon_edges_x[:, nx] = lons[:, -1] + 0.5 * (lons[:, -1] - lons[:, -2])
    lat_edges_x[:, 0] = lats[:, 0] - 0.5 * (lats[:, 1] - lats[:, 0])
    lat_edges_x[:, nx] = lats[:, -1] + 0.5 * (lats[:, -1] - lats[:, -2])

    lon_corners = np.empty((ny + 1, nx + 1), dtype=np.float64)
    lat_corners = np.empty((ny + 1, nx + 1), dtype=np.float64)
    lon_corners[1:ny, :] = 0.5 * (lon_edges_x[:-1, :] + lon_edges_x[1:, :])
    lat_corners[1:ny, :] = 0.5 * (lat_edges_x[:-1, :] + lat_edges_x[1:, :])
    lon_corners[0, :] = lon_edges_x[0, :] - 0.5 * (lon_edges_x[1, :] - lon_edges_x[0, :])
    lon_corners[ny, :] = lon_edges_x[-1, :] + 0.5 * (lon_edges_x[-1, :] - lon_edges_x[-2, :])
    lat_corners[0, :] = lat_edges_x[0, :] - 0.5 * (lat_edges_x[1, :] - lat_edges_x[0, :])
    lat_corners[ny, :] = lat_edges_x[-1, :] + 0.5 * (lat_edges_x[-1, :] - lat_edges_x[-2, :])
    return lon_corners, lat_corners



@njit
def point_in_poly_numba(x, y, poly_lon, poly_lat):
    """
    使用射线法判断单个点是否位于四边形内部。

    参数
    ----------
    x, y : float
        待判定点的经纬度坐标。
    poly_lon, poly_lat : np.ndarray
        四边形四个顶点的经度、纬度数组，顶点顺序需保持一致
        （顺时针或逆时针均可）。

    返回
    ----------
    bool
        若点位于四边形内部，则返回 `True`，否则返回 `False`。
    """
    inside = False
    j = poly_lon.shape[0] - 1
    for i in range(poly_lon.shape[0]):
        xi = poly_lon[i]
        yi = poly_lat[i]
        xj = poly_lon[j]
        yj = poly_lat[j]
        if ((yi > y) != (yj > y)) and (x < (xj - xi) * (y - yi) / (yj - yi + 1e-12) + xi):
            inside = not inside
        j = i
    return inside



@njit(parallel=True)
def meshgrid_index_numba(global_lon2d, global_lat2d,
                         lon_corners, lat_corners,
                         global_lon_centers, global_lat_centers,
                         sigma):
    """
    将高分辨率规则经纬网格的中心点映射到 CWRF 网格编号。

    算法流程为：
    1. 遍历每个 CWRF 格点四边形；
    2. 先用经纬度包围盒缩小候选高分像元范围；
    3. 再对候选像元中心执行点在多边形内测试；
    4. 将命中的高分像元写入对应的 CWRF 线性编号。

    参数
    ----------
    global_lon2d, global_lat2d : np.ndarray
        局部高分辨率子区域的二维中心点经纬度网格。
    lon_corners, lat_corners : np.ndarray
        CWRF 网格角点经纬度，形状为 `(ny+1, nx+1)`。
    global_lon_centers, global_lat_centers : np.ndarray
        高分辨率子区域的一维经度、纬度中心坐标。
    sigma : float
        包围盒缓冲量，单位为度，用于避免边界浮点误差漏采样。

    返回
    ----------
    np.ndarray
        形状为 `(nlat, nlon)` 的二维整型数组。值为 `-1` 表示未命中任何
        CWRF 格点，其余值为对应 CWRF 格点的线性编号 `j * nx + i`。
    """
    nlat, nlon = global_lon2d.shape
    sn = lat_corners.shape[0] - 1
    we = lon_corners.shape[1] - 1

    grid = np.full((nlat, nlon), -1, np.int32)
    for j in prange(sn):
        for i in range(we):
            poly_lon = np.empty(4, np.float64)
            poly_lat = np.empty(4, np.float64)
            poly_lon[0] = lon_corners[j,   i]
            poly_lat[0] = lat_corners[j,   i]
            poly_lon[1] = lon_corners[j,   i+1]
            poly_lat[1] = lat_corners[j,   i+1]
            poly_lon[2] = lon_corners[j+1, i+1]
            poly_lat[2] = lat_corners[j+1, i+1]
            poly_lon[3] = lon_corners[j+1, i]
            poly_lat[3] = lat_corners[j+1, i]

            min_lon = poly_lon[0]
            max_lon = poly_lon[0]
            min_lat = poly_lat[0]
            max_lat = poly_lat[0]
            for k in range(1, 4):
                if poly_lon[k] < min_lon: min_lon = poly_lon[k]
                if poly_lon[k] > max_lon: max_lon = poly_lon[k]
                if poly_lat[k] < min_lat: min_lat = poly_lat[k]
                if poly_lat[k] > max_lat: max_lat = poly_lat[k]
            min_lon -= sigma
            max_lon += sigma
            min_lat -= sigma
            max_lat += sigma

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

            cell_value = j * we + i
            for ii in range(lat_start, lat_end):
                for jj in range(lon_start, lon_end):
                    if point_in_poly_numba(global_lon2d[ii, jj], global_lat2d[ii, jj], poly_lon, poly_lat):
                        grid[ii, jj] = cell_value
    return grid



def build_cwrf_corners(infil: Dataset,
                       lons: np.ndarray,
                       lats: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """
    构造 CWRF 网格角点坐标。

    本函数优先采用“投影中心点 + DX/DY”方式恢复真实角点；如果投影属性
    缺失、转换器初始化失败或其他异常发生，则自动退回到中心点近似角点方案。

    参数
    ----------
    infil : netCDF4.Dataset
        已打开的 `geo_em` 文件句柄。
    lons, lats : np.ndarray
        二维 CWRF 中心点经纬度数组。

    返回
    ----------
    tuple[np.ndarray, np.ndarray]
        `(lon_corners, lat_corners)`，形状均为 `(ny+1, nx+1)`。
    """
    try:
        dx = float(getattr(infil, "DX"))
        dy = float(getattr(infil, "DY"))
        transformer = init_transformer_from_geoem(infil)
        # netCDF4 读出的变量有时是 MaskedArray；pyproj 不接受该类型，
        # 这里统一转为普通 ndarray 后再做投影转换。
        lons_in = np.ma.filled(np.asanyarray(lons), np.nan).astype(np.float64, copy=False)
        lats_in = np.ma.filled(np.asanyarray(lats), np.nan).astype(np.float64, copy=False)
        x_center, y_center = transformer.transform(lons_in, lats_in)
        x_corners, y_corners = generate_grid_corners_numba(x_center, y_center, dx, dy)
        lon_corners, lat_corners = transformer.transform(x_corners, y_corners, direction="INVERSE")
        return lon_corners, lat_corners
    except Exception as e:
        print(f"投影角点生成失败（{e}），回退为中心点近似角点。")
        return approximate_corners_from_centers(lons, lats)



def ocean_mask_from_highres_nc(infil: Dataset,
                               lons: np.ndarray,
                               lats: np.ndarray,
                               land_sea_mask_nc: str,
                               ocean_threshold: float = 0.5) -> np.ndarray:
    """
    使用高分辨率 `land_ocean_mask` 数据聚合生成 CWRF 海洋掩膜。

    该函数是脚本当前的主海陆判定方法。它不再依赖旧的感染算法，而是直接：
    1. 计算 CWRF 网格角点；
    2. 从全球高分辨率海陆掩膜中裁剪出覆盖当前域的子区域；
    3. 将高分像元中心映射到 CWRF 四边形网格；
    4. 统计每个 CWRF 格点内的海洋像元占比；
    5. 按 `ocean_threshold` 阈值将粗网格判定为海洋或陆地。

    高分辨率掩膜数据约定：
    - `0 = ocean`
    - `1 = land`

    参数
    ----------
    infil : netCDF4.Dataset
        已打开的 `geo_em` 文件句柄，用于读取投影元数据。
    lons, lats : np.ndarray
        二维 CWRF 中心点经纬度数组。
    land_sea_mask_nc : str
        高分辨率海陆掩膜文件路径。
    ocean_threshold : float, default=0.5
        海洋比例阈值。若某个 CWRF 格点内海洋像元比例大于等于该值，
        则判定为海洋。

    返回
    ----------
    np.ndarray
        二维 `uint8` 海洋掩膜，满足 `1=海洋`、`0=非海洋`。

    异常
    ----------
    FileNotFoundError
        当输入的高分辨率掩膜文件不存在时抛出。
    ValueError
        当裁剪后的高分辨率子区域为空时抛出。
    """
    if not os.path.isfile(land_sea_mask_nc):
        raise FileNotFoundError(f"高分辨率海陆掩膜不存在：{land_sea_mask_nc}")

    lon_corners, lat_corners = build_cwrf_corners(infil, lons, lats)
    latrange, lonrange = get_latlon_range(lat_corners, lon_corners, offset=0.05)

    with xr.open_dataset(land_sea_mask_nc) as ds_mask:
        if "land_ocean_mask" not in ds_mask:
            raise KeyError("高分辨率海陆掩膜文件中缺少变量 `land_ocean_mask`。")

        da_mask = ds_mask["land_ocean_mask"]
        if "lat" not in da_mask.dims or "lon" not in da_mask.dims:
            raise ValueError("`land_ocean_mask` 必须包含 `lat` 和 `lon` 两个维度。")

        da_mask = da_mask.transpose("lat", "lon")
        lat_values = da_mask["lat"].values
        lon_values = da_mask["lon"].values

        lat_ascending = bool(lat_values[0] < lat_values[-1])
        lon_ascending = bool(lon_values[0] < lon_values[-1])
        lat_slice = slice(latrange[0], latrange[1]) if lat_ascending else slice(latrange[1], latrange[0])
        lon_slice = slice(lonrange[0], lonrange[1]) if lon_ascending else slice(lonrange[1], lonrange[0])

        subset = da_mask.sel(lat=lat_slice, lon=lon_slice)
        hr_mask = subset.values
        hr_lats = subset["lat"].values
        hr_lons = subset["lon"].values

    if hr_mask.size == 0:
        raise ValueError("高分辨率海陆掩膜裁剪后为空，无法进行网格匹配。")

    global_lon2d, global_lat2d = np.meshgrid(hr_lons, hr_lats)
    elmindex = meshgrid_index_numba(
        global_lon2d.astype(np.float64),
        global_lat2d.astype(np.float64),
        lon_corners.astype(np.float64),
        lat_corners.astype(np.float64),
        hr_lons.astype(np.float64),
        hr_lats.astype(np.float64),
        0.002,
    )

    ny, nx = lons.shape
    ncell = ny * nx
    valid = elmindex >= 0
    flat_idx = elmindex[valid].ravel()
    ocean_hits = (hr_mask[valid] == 0).astype(np.float64).ravel()
    total_counts = np.bincount(flat_idx, minlength=ncell).astype(np.float64)
    ocean_counts = np.bincount(flat_idx, weights=ocean_hits, minlength=ncell).astype(np.float64)

    ocean_fraction = np.full(ncell, np.nan, dtype=np.float64)
    sampled = total_counts > 0
    ocean_fraction[sampled] = ocean_counts[sampled] / total_counts[sampled]

    if np.any(~sampled):
        print(f"警告：有 {np.sum(~sampled)} 个 CWRF 格点未命中高分像元，回退为中心点取样。")
        lat_idx = np.abs(hr_lats[:, None, None] - lats[None, :, :]).argmin(axis=0)
        lon_idx = np.abs(hr_lons[:, None, None] - lons[None, :, :]).argmin(axis=0)
        center_land = hr_mask[lat_idx, lon_idx]
        ocean_fraction[~sampled] = (center_land.ravel()[~sampled] == 0).astype(np.float64)

    return (ocean_fraction.reshape(ny, nx) >= ocean_threshold).astype(np.uint8)



def ocean_mask_from_shapefile(lons: np.ndarray,
                              lats: np.ndarray,
                              land_shp_path: str) -> np.ndarray:
    """
    使用陆地边界矢量文件按“点是否落在陆地多边形内”判定海陆。

    这是一个可选的优先方法，适用于用户提供了可信海陆边界矢量的情形。
    若矢量判定失败，主流程会自动回退到高分辨率 `nc` 掩膜方法。

    约定如下：
    - 点在陆地多边形内或边界上：判定为陆地；
    - 点不在陆地多边形内：判定为海洋。

    参数
    ----------
    lons, lats : np.ndarray
        二维 CWRF 中心点经纬度数组。
    land_shp_path : str
        陆地边界矢量文件路径，例如 `.gpkg` 或 `.shp`。

    返回
    ----------
    np.ndarray
        二维 `uint8` 海洋掩膜，满足 `1=海洋`、`0=陆地`。
    """
    import geopandas as gpd
    from shapely.geometry import Point
    from shapely.prepared import prep

    # 读取 + 统一到 WGS84
    gdf = gpd.read_file(land_shp_path, engine="fiona")
    if gdf.crs is None:
        gdf = gdf.set_crs(epsg=4326)
    elif gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)

    # 取唯一多边形，必要时做一次轻量修补
    geom = gdf.geometry.iloc[0]
    if not geom.is_valid:
        geom = geom.buffer(0)

    # prepared geometry 加速 covers（含边界）
    G = prep(geom)
    it = (G.covers(Point(float(x), float(y))) for x, y in zip(lons.ravel(), lats.ravel()))
    is_land = np.fromiter(it, dtype=bool, count=lons.size).reshape(lons.shape)
    mask = (~is_land).astype(np.uint8)  # 海洋=1，陆地=0

    return mask



@njit(parallel=True, fastmath=True)
def _fill_land_cells_numba(result, land_mask, n_types, water_idx, land_window):
    """
    对非水体格点补齐 `LANDUSEF` 比例，使每个格点最终仍能归一化到 1。

    处理策略分两类：
    1. 若当前格点已有非水体类别占比，则把剩余比例补到最大类别上；
    2. 若当前格点所有非水体类别均为 0，则用邻域均值重建类别分布；
       若邻域也全为 0，则在非水体类别之间均分。

    参数
    ----------
    result : np.ndarray
        待修改的 `LANDUSEF` 三维数组。
    land_mask : np.ndarray
        二维布尔掩膜，`True` 表示需要按陆地逻辑修补的格点。
    n_types : int
        土地利用类型总数。
    water_idx : int
        水体类别索引。
    land_window : int
        邻域均值窗口大小，必须为奇数。
    """
    n_lat, n_lon = result.shape[1], result.shape[2]
    k = land_window // 2

    for i in prange(n_lat):
        for j in range(n_lon):
            if not land_mask[i, j]:
                continue

            # 计算除水层外的总和 + 找最大类别
            total_others = 0.0
            max_idx = 0
            max_val = -1.0e308
            for t in range(n_types):
                v = result[t, i, j]
                if t == water_idx:
                    v = 0.0
                if v > max_val:
                    max_val = v
                    max_idx = t
                total_others += v

            if total_others > 0.0:
                # 有其它类型：把最大比例的类型补到和为 1
                add = 1.0 - total_others
                result[max_idx, i, j] += add
                # clip 到 [0,1]
                for t in range(n_types):
                    v = result[t, i, j]
                    if v < 0.0: v = 0.0
                    elif v > 1.0: v = 1.0
                    result[t, i, j] = v
            else:
                # 无其它类型：用邻域均值
                i0 = 0 if i - k < 0 else i - k
                i1 = n_lat if i + k + 1 > n_lat else i + k + 1
                j0 = 0 if j - k < 0 else j - k
                j1 = n_lon if j + k + 1 > n_lon else j + k + 1
                area = (i1 - i0) * (j1 - j0)

                total_mean = 0.0
                # 暂存每一类的邻域均值
                mean_props = np.empty(n_types, dtype=result.dtype)
                for t in range(n_types):
                    s = 0.0
                    for ii in range(i0, i1):
                        for jj in range(j0, j1):
                            s += result[t, ii, jj]
                    m = s / area
                    if t == water_idx:
                        m = 0.0
                    mean_props[t] = m
                    total_mean += m

                if total_mean > 0.0:
                    inv = 1.0 / total_mean
                    for t in range(n_types):
                        result[t, i, j] = mean_props[t] * inv
                else:
                    # 邻域全 0：均分到“非水层”
                    val = 1.0 / (n_types - 1)
                    for t in range(n_types):
                        result[t, i, j] = 0.0
                    for t in range(n_types):
                        if t != water_idx:
                            result[t, i, j] = val



def classify_lakes_3d(landusef: np.ndarray,
                      lake_mask: np.ndarray,
                      ocean_mask: np.ndarray,
                      land_window: int = 3) -> np.ndarray:
    """
    根据湖泊掩膜与海洋掩膜重建 `LANDUSEF` 的水体层，并保证每个格点比例和为 1。

    本函数负责把新的海洋/湖泊判定同步回三维土地利用比例数组：
    - 海洋格点：水体层设为 1，其余类别清零；
    - 湖泊格点：水体层设为 1，其余类别清零；
    - 其余陆地格点：重新调整非水体类别比例，保证总和为 1。

    参数
    ----------
    landusef : np.ndarray
        原始三维土地利用比例数组，形状为 `(n_types, n_lat, n_lon)`。
        其中索引 15 为水体层。
    lake_mask : np.ndarray
        二维湖泊掩膜，非零表示湖泊。
    ocean_mask : np.ndarray
        二维海洋掩膜，非零表示海洋。
    land_window : int, default=3
        修补陆地类别时使用的邻域窗口大小，必须为奇数。

    返回
    ----------
    np.ndarray
        调整后的三维土地利用比例数组，形状与 `landusef` 相同，
        且每个格点沿类型维的和严格为 1。
    """
    # 检查窗口大小是否为奇数
    if land_window % 2 == 0:
        raise ValueError("land_window 必须为奇数")

    # 转换海洋掩膜为布尔类型
    ocean_mask = ocean_mask.astype(bool)
    lake_mask = lake_mask.astype(bool)

    n_types, n_lat, n_lon = landusef.shape
    if n_types < 16:
        raise ValueError("landusef 至少需要16层，第16层为水体层，索引15")

    # 复制数组，作为结果数组
    result = landusef.copy()

    # 将原有水体层（索引15）清零
    result[15, :, :] = 0.0


    # 生成湖泊掩膜（排除海洋）
    land_mask = (~ocean_mask) & (~lake_mask)

    # 赋值海洋格点：水体比例=1, 其他类型=0
    result[:, ocean_mask] = 0.0
    result[15, ocean_mask] = 1.0

    # 赋值湖泊格点：水体比例=1, 其他类型=0
    result[:, lake_mask] = 0.0
    result[15, lake_mask] = 1.0

    # 确保连续内存，避免 Numba 因 stride 导致降速
    result = np.ascontiguousarray(result)
    land_mask = np.ascontiguousarray(land_mask.astype(np.bool_))
    _fill_land_cells_numba(result, land_mask, n_types, water_idx=15, land_window=land_window)

    # 全局归一化，确保每个格点和为1
    sums = result.sum(axis=0)
    result = result / sums
    # 检查归一化结果
    sums = result.sum(axis=0)
    if not np.allclose(sums, 1.0):
        raise ValueError("归一化后，某些格点的和不为1")
    
    # 强制要求数据范围在 [0,1]
    result = np.clip(result, 0.0, 1.0)
    result = np.where(result < 0, 0.0, result)
    result = np.where(result > 1, 1.0, result)
    return result



if __name__ == "__main__":
    argparser = argparse.ArgumentParser()
    argparser.add_argument('-lk', '--lake_threshold', type=float, help='lake threshold', required=True)
    argparser.add_argument('-lsbdy', '--land_and_sea', type=str, help='land and sea boundary (vector file like .gpkg/.shp, or netcdf [0=sea,1=land])', default='land_ocean_mask_igbp_2020.nc')
    argparser.add_argument('-thres', '--land_threshold', type=float, help='land threshold', default=0.5)
    args = argparser.parse_args()
    lake_threshold = args.lake_threshold
    land_and_sea = args.land_and_sea
    land_threshold = args.land_threshold

    land_and_sea_shp = None
    land_sea_mask_nc = None
    land_and_sea_suffix = os.path.splitext(land_and_sea)[1].lower()

    if land_and_sea_suffix in {'.shp', '.gpkg'}:
        print(f"使用矢量边界文件进行海陆判定：{land_and_sea}")
        land_and_sea_shp = land_and_sea
        land_sea_mask_nc = 'land_ocean_mask_igbp_2020.nc'
    else:
        print(f"使用高分辨率海陆掩膜进行海陆判定：{land_and_sea}")
        land_sea_mask_nc = land_and_sea
    time0 = time.time()

    # 打开 geo_em 文件
    infil = Dataset("./geo_em.d01.nc", mode="r+")

    dplake = infil.variables["DPLAKE"][:].squeeze()
    flake = infil.variables["FLAKE"][:].squeeze()
    scw = infil.variables["SC_WATER"][:].squeeze()
    lu = infil.variables["LU_INDEX"][:].squeeze()
    slopecat = infil.variables["SLOPECAT"][:].squeeze()
    snoalb = infil.variables["SNOALB"][:].squeeze()
    soiltemp = infil.variables["SOILTEMP"][:].squeeze()
    xoro = infil.variables["XORO"][:].squeeze()
    xfsky = infil.variables["XFSKY"][:].squeeze()
    xhsdv = infil.variables["XHSDV"][:].squeeze()
    xmoang = infil.variables["XMOANG"][:].squeeze()
    xmoani = infil.variables["XMOANI"][:].squeeze()
    xmostd = infil.variables["XMOSTD"][:].squeeze()
    xorasp = infil.variables["XORASP"][:].squeeze()
    xorslo = infil.variables["XORSLO"][:].squeeze()
    xsgasp = infil.variables["XSGASP"][:].squeeze()
    xslpys = infil.variables["XSLPYS"][:].squeeze()
    xsoslo = infil.variables["XSOSLO"][:].squeeze()
    xsostd = infil.variables["XSOSTD"][:].squeeze()
    slpxgrid = infil.variables["slpxgrid"][:].squeeze()
    slpygrid = infil.variables["slpygrid"][:].squeeze()
    landusef = infil.variables["LANDUSEF"][:].squeeze()
    lons = infil.variables["XLONG_M"][0, :, :].squeeze()  # CLONG
    lats = infil.variables["XLAT_M"][0, :, :].squeeze()   # CLAT
    print(f"数据读取完毕，耗时 {time.time()-time0:.1f} 秒")
    time1 = time.time()

    # 修正湖泊深度
    dplake_gt0_lt15 = np.where((dplake > 0) & (dplake < 1.5), 1, 0)
    newdplake = np.where(dplake_gt0_lt15 == 1, 1.5, dplake)

    # 浅湖掩膜
    dplake_gt0_lt20 = np.where((newdplake > 0) & (newdplake < 20), 1, 0)
    # 深湖掩膜
    dplake_gt20 = np.where(newdplake > 20, 1, 0)

    # 湖泊比例调整
    newflake = np.where(flake > lake_threshold*100, flake + 1, flake - 1)
    newflake = np.where(newflake >= 100., 100., newflake)
    newflake = np.where(newflake < 0., 0., newflake)

    # 可用湖泊掩模
    flake_gt_thres = np.where(newflake > lake_threshold*100, 1, 0)

    # 更新 SC_WATER
    scwcopy = np.where((scw == 5) | (scw == 6), 2, scw)
    # 深湖
    scwcopy = np.where((dplake_gt20 == 1) & (flake_gt_thres == 1), 6, scwcopy)
    # 浅湖
    scwcopy = np.where((dplake_gt0_lt20 == 1) & (flake_gt_thres == 1), 5, scwcopy)
    print(f"湖泊深度和比例调整完毕，耗时 {time.time()-time1:.1f} 秒")
    time2 = time.time()

    # 修正海陆边界：优先矢量边界文件，失败或未提供则使用高分辨率海陆掩膜
    if land_and_sea_shp is not None and os.path.isfile(land_and_sea_shp):
        print(f"[Method-1] 使用矢量边界文件进行海陆判定（点在陆地面外 => 海洋）")
        print(f"           边界文件：{land_and_sea_shp}")
        try:
            mask = ocean_mask_from_shapefile(lons, lats, land_and_sea_shp)  # 1/0 掩膜
        except Exception as e:
            print(f"矢量边界文件判定失败（{e}），回退到高分辨率海陆掩膜 [Method-2].")
            print(f"           掩膜文件：{land_sea_mask_nc}")
            mask = ocean_mask_from_highres_nc(infil, lons, lats, land_sea_mask_nc, ocean_threshold=land_threshold)
    else:
        print(f"[Method-2] 未提供矢量边界文件或文件不存在，使用高分辨率海陆掩膜（海洋比例 >= {land_threshold} 判海）")
        print(f"           掩膜文件：{land_sea_mask_nc}")
        mask = ocean_mask_from_highres_nc(infil, lons, lats, land_sea_mask_nc, ocean_threshold=land_threshold)
    # 外部 shp/nc 先给出海陆主判定；这里再用 LANDUSEF 做一次保守回修。
    # 若某格点虽然已被外部边界判为海洋，但现有 LANDUSEF 的非水体比例仍显著偏高，
    # 则允许 LANDUSEF 否决该海洋判定，并把该格点翻回陆地。
    newocemask = refine_ocean_with_landuse(mask, landusef, land_threshold=land_threshold, water_index=15)
    
    # 更新 SC_WATER
    newscw = scwcopy.copy()
    lakemask = np.where((newscw == 5) | (newscw == 6), 1, 0)
    oldocemask = np.where(newscw == 8, 1, 0)
    lakeocemask = np.where((lakemask == 1) & (newocemask == 1), 1, 0)
    newscw[oldocemask == 1] = 2   # 先把旧海洋改为陆地
    # 判断湖泊和海洋是否重叠，且湖泊深度是否合理
    if np.sum(lakeocemask) > 0:
        print(f"警告：出现湖泊和海洋重叠区域！")
        # 如果重叠区域的湖泊深度大于1.5m，且flake大于阈值，则改为湖泊
        overlap_dplake = newdplake * lakeocemask
        overlap_flake = newflake * lakeocemask
        condition = (overlap_dplake >= 1.5) & (overlap_flake > lake_threshold*100)
        if np.sum(condition) > 0:
            print(f"重叠区域中，湖泊深度 > 1.5m 且 flake > {lake_threshold*100} 的格点数：{np.sum(condition)}，改为湖泊")
            newscw[condition] = np.where(newdplake[condition] > 20, 6, 5)
            newocemask[condition] = 0   # 把这些格点从海洋中去掉
            lakemask[condition] = 1    # 把这些格点保留为湖泊
        # 其余重叠区域改为海洋
        condition_else = (lakeocemask == 1) & (~condition)
        if np.sum(condition_else) > 0:
            print(f"重叠区域中，其余格点数：{np.sum(condition_else)}，改为海洋")
            newscw[condition_else] = 8
            newocemask[condition_else] = 1   # 把这些格点保留为海洋
            lakemask[condition_else] = 0    # 把这些格点从湖泊中去掉
    newscw[newocemask == 1] = 8   # 再把新海洋改为海洋
    diffoce = oldocemask - newocemask   # 1表示错误的海洋区域，-1表示缺失的海洋区域
    print(f"错误的海洋区域：{np.sum(diffoce == 1)}")
    print(f"缺失的海洋区域：{np.sum(diffoce == -1)}")
    time3 = time.time()

    # 更新土地利用比例
    newlandusef = classify_lakes_3d(landusef, lakemask, newocemask)
    water_grid = np.where(newlandusef[15] > 0.5, 1, 0)
    # 检查水陆格点一致性
    if np.sum(water_grid-lakemask-newocemask) != 0:
        print(f"水体格点：{np.sum(water_grid)}")
        print(f"湖泊格点：{np.sum(lakemask)}")
        print(f"海洋格点：{np.sum(newocemask)}")
        overlap_mask = (lakemask == 1) & (newocemask == 1)
        print("重叠格点数：", int(overlap_mask.sum()))
        raise ValueError(f"湖泊网格不一致，检查数据。湖泊网格差异：{np.sum(water_grid-lakemask-newocemask)}")

    # 更新 LU_INDEX 和 SC_WATER
    lu[diffoce == 1] = 6
    lu = np.where(lakemask == 1, 16, lu)

    # 去除非湖泊区域
    lu_water = np.where(lu == 16, 1, 0)
    water_grid = np.where((newscw == 5) | (newscw == 6) | (newscw == 8), 1, 0)
    nonwater_grid = lu_water - water_grid

    # newscw = np.where(nonwater_grid == 1, 5, newscw)
    scw_5_6 = np.where((newscw == 5) | (newscw == 6), 1, 0)

    #将非湖泊区域更新6
    lu = np.where(nonwater_grid == 1, 6, lu)

    # 更新 XLANDMASK
    lu_16 = np.where(lu == 16, 1, 0)
    newxlandmask = np.where(lu_16 == 1, 0, 1)
    slopecat[lu_16 == 1] = 0
    snoalb[lu_16 == 1] = 0
    soiltemp[lu_16 == 1] = 0
    xfsky[lu_16 == 1] = -9999
    xhsdv[lu_16 == 1] = -9999
    xmoang[lu_16 == 1] = -9999
    xmoani[lu_16 == 1] = -9999
    xmostd[lu_16 == 1] = -9999
    xorasp[lu_16 == 1] = -9999
    xoro[newocemask == 1] = 0
    xoro[scw_5_6 == 1] = 3
    xorslo[lu_16 == 1] = -9999
    xsgasp[lu_16 == 1] = -9999
    xslpys[lu_16 == 1] = -9999
    xsoslo[lu_16 == 1] = -9999
    xsostd[lu_16 == 1] = -9999
    slpxgrid[lu_16 == 1] = -9999
    slpygrid[lu_16 == 1] = -9999

    # 写回 geo_em 文件
    infil.variables["LU_INDEX"][0,:,:] = lu
    infil.variables["LANDMASK"][0,:,:] = newxlandmask
    infil.variables["SC_WATER"][0,:,:] = newscw
    infil.variables["DPLAKE"][0,:,:] = newdplake
    infil.variables["SLOPECAT"][0,:,:] = slopecat
    infil.variables["SNOALB"][0,:,:] = snoalb
    infil.variables["SOILTEMP"][0,:,:] = soiltemp
    infil.variables["XORO"][0,:,:] = xoro
    infil.variables["XFSKY"][0,:,:] = xfsky
    infil.variables["XHSDV"][0,:,:] = xhsdv
    infil.variables["XMOANG"][0,:,:] = xmoang
    infil.variables["XMOANI"][0,:,:] = xmoani
    infil.variables["XMOSTD"][0,:,:] = xmostd
    infil.variables["XORASP"][0,:,:] = xorasp
    infil.variables["XORSLO"][0,:,:] = xorslo
    infil.variables["XSGASP"][0,:,:] = xsgasp
    infil.variables["XSLPYS"][0,:,:] = xslpys
    infil.variables["XSOSLO"][0,:,:] = xsoslo
    infil.variables["XSOSTD"][0,:,:] = xsostd
    infil.variables["slpxgrid"][0,:,:] = slpxgrid
    infil.variables["slpygrid"][0,:,:] = slpygrid
    infil.variables["FLAKE"][0,:,:] = newflake
    infil.variables["LANDUSEF"][0,:,:,:] = newlandusef

    # 关闭文件
    infil.close()



    # 保存海洋掩膜
    infil = xr.open_dataset("./geo_em.d01.nc", mode="r")
    landmask = infil["LANDMASK"]

    diffoce = np.expand_dims(diffoce, axis=0)
    diffoce = xr.DataArray(diffoce, dims=landmask.dims, coords=landmask.coords)
    diffoce.attrs = landmask.attrs
    diffoce.name = "OCEANMASK_DIFF"
    ocean = np.expand_dims(newocemask, axis=0)
    ocean = xr.DataArray(ocean, dims=landmask.dims, coords=landmask.coords)
    ocean.attrs = landmask.attrs
    ocean.name = "OCEANMASK"

    # 保存到文件
    outfil = xr.Dataset()
    outfil["OCEANMASK_DIFF"] = diffoce
    outfil["OCEANMASK"] = ocean
    outfil.to_netcdf("./ocean_mask.nc")

    infil.close()
    outfil.close()
    print(f"海洋掩膜保存完毕，耗时 {time.time()-time0:.1f} 秒")
    print("\n\n===================================")
    print("========== 全部处理完毕！ ==========")
    print("===================================")
    
