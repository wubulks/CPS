import os
import time
import argparse
import numpy as np
from netCDF4 import Dataset
from collections import deque
import xarray as xr
from numba import njit, prange

###################### - 函数定义区 - ######################
def refine_ocean_with_landuse(mask: np.ndarray,
                              landusef: np.ndarray,
                              land_threshold: float = 0.5,
                              water_index: int = 15) -> np.ndarray:
    """
    用 landusef 修正海洋掩膜：
      若某海洋格点（mask==1）处的“陆地比例总和”（=除 water_index 外的所有层之和）> land_threshold，
      则改为陆地（置 0）。
    """
    import numpy as np
    assert landusef.ndim == 3, "landusef 应为 (n_types, n_lat, n_lon)"
    assert landusef.shape[1:] == mask.shape, "landusef 和 mask 空间维度需一致"
    assert landusef.shape[0] > water_index, "landusef 第一维长度必须 > water_index"

    # 陆地比例总和 = 所有类型之和 - 水体比例（第 water_index 层）
    land_sum = landusef.sum(axis=0) - landusef[water_index]
    mask_out = mask.astype(np.uint8).copy()
    flip_to_land = (mask_out == 1) & (land_sum > land_threshold)
    mask_out[flip_to_land] = 0
    return mask_out



def ocean_mask_from_shapefile(lons: np.ndarray,
                              lats: np.ndarray,
                              land_shp_path: str) -> np.ndarray:
    """
    用“陆地多边形”shapefile 判定海陆（1=海, 0=陆）。
    假设 shapefile 只有一个面要素；尽量走最快的简单路径。
    """
    import numpy as np
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



def infection_algorithm(scw: np.ndarray,
                        hgt_m: np.ndarray,
                        mask: np.ndarray | None = None,
                        sea_level: float = 0.0,
                        tol: float = 1e-6,
                        merge_lakes: bool = True) -> np.ndarray:
    """
    从边界所有“海洋候选”(scw==8 且 |hgt_m-sea_level|<=tol)作为多起点做 8 邻域 flood-fill，
    得到与边界连通的海洋掩膜。若边界上无候选，则返回全 0（本域无海洋）。
    若 merge_lakes=True，则仅在湖格点同时满足 |hgt_m-sea_level|<=tol 时才并入海洋。

    参数
    ----
    scw : 2D SC_WATER 数组
    hgt_m : 2D 地形高程（米），与 scw 同形状
    mask : 可选的 0/1 掩膜；缺省则内部创建
    sea_level : 认为海平面的高程值（默认 0.0）
    tol : 海平面容差（默认 1e-6；若你的 HGT_M 海面为 0±0.5，可把 tol 设为 0.5）
    merge_lakes : 是否把满足海平面条件的湖(5/6)并入海

    返回
    ----
    mask : 0/1 海洋掩膜（只包含“与边界连通且海拔≈海平面”的海）
    """
    scw = np.asarray(scw)
    hgt_m = np.asarray(hgt_m)
    assert scw.shape == hgt_m.shape, "scw 与 hgt_m 形状必须一致"

    rows, cols = scw.shape
    if mask is None:
        mask = np.zeros((rows, cols), dtype=np.uint8)

    # 海平面判定（数值稳定：允许容差 tol）
    sea_ok = np.isfinite(hgt_m) & (np.abs(hgt_m - sea_level) <= tol)

    # 1) 收集边界上的“海洋候选”作为种子：scw==8 且 sea_ok
    seeds = []
    for j in range(cols):
        if scw[0, j] == 8 and sea_ok[0, j]:           seeds.append((0, j))
        if scw[rows-1, j] == 8 and sea_ok[rows-1, j]: seeds.append((rows-1, j))
    for i in range(rows):
        if scw[i, 0] == 8 and sea_ok[i, 0]:           seeds.append((i, 0))
        if scw[i, cols-1] == 8 and sea_ok[i, cols-1]: seeds.append((i, cols-1))

    # 2) 边界无海洋候选：直接返回全 0（本域无海洋）
    if not seeds:
        return mask

    # 3) 多源 BFS 泛洪（8 邻域）
    q = deque(seeds)
    while q:
        r, c = q.popleft()
        if r < 0 or r >= rows or c < 0 or c >= cols or mask[r, c] == 1:
            continue

        # 只能在“边界海洋候选连通域”内扩张
        if scw[r, c] == 8 and sea_ok[r, c]:
            mask[r, c] = 1
            for dr in (-1, 0, 1):
                for dc in (-1, 0, 1):
                    if dr == 0 and dc == 0:
                        continue
                    nr, nc = r + dr, c + dc
                    if 0 <= nr < rows and 0 <= nc < cols:
                        # 海格点：必须满足 sea_ok 才能扩展
                        if scw[nr, nc] == 8 and sea_ok[nr, nc] and mask[nr, nc] == 0:
                            q.append((nr, nc))
                        # 湖格点：只有在 sea_ok 条件满足且允许并湖时才并入为海
                        elif merge_lakes and scw[nr, nc] in (5, 6) and sea_ok[nr, nc] and mask[nr, nc] == 0:
                            scw[nr, nc] = 8  # 就地改为海
                            q.append((nr, nc))
    return mask



@njit(parallel=True, fastmath=True)
def _fill_land_cells_numba(result, land_mask, n_types, water_idx, land_window):
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
    对三维土地利用比例数据进行湖泊分类和海洋掩膜处理，并归一化各类比例。

    参数：
    :param landusef: 三维数组，形状为 (n_types, n_lat, n_lon)，表示各土地利用类型的比例。
                     第16层（索引15）原为水体比例，将重新计算。
    :param ocean_mask: 二维数组，形状为 (n_lat, n_lon)，海洋格点标记（可为 0/1 或布尔）。
    :param threshold: 湖泊比例阈值，超过该值的格点视为湖泊（0-1）。
    :param land_window: 用于邻域均值计算的窗口大小，必须为奇数。

    返回：
    :return: 经调整后并归一化的三维土地利用比例数组。
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

    # # 处理剩余陆地格点
    # for i in range(n_lat):
    #     for j in range(n_lon):
    #         if not land_mask[i, j]:
    #             continue
    #         # 排除水体层，计算其他类型总和
    #         others = result[:, i, j].copy()
    #         others[15] = 0.0
    #         total_others = others.sum()
    #         if total_others > 0:
    #             # 存在其他类型，则找到最大比例的类型，补充至总和=1
    #             max_idx = np.argmax(others)
    #             result[max_idx, i, j] = 1.0 - others.sum() + others[max_idx]
    #             # 修正范围
    #             result[:, i, j] = np.clip(result[:, i, j], 0.0, 1.0)
    #         else:
    #             # 无其他类型：用邻域均值填充
    #             k = land_window // 2
    #             i0, i1 = max(0, i-k), min(n_lat, i+k+1)
    #             j0, j1 = max(0, j-k), min(n_lon, j+k+1)
    #             local = result[:, i0:i1, j0:j1]
    #             mean_props = local.reshape(n_types, -1).mean(axis=1)
    #             mean_props[15] = 0.0
    #             total_mean = mean_props.sum()
    #             if total_mean > 0:
    #                 result[:, i, j] = mean_props / total_mean
    #             else:
    #                 result[:-1, i, j] = 1.0 / (n_types - 1)

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
    argparser.add_argument('-lsbdy', '--land_and_sea_bdy', type=str, help='land and sea boundary shapefile', default=None)
    args = argparser.parse_args()
    lake_threshold = args.lake_threshold
    land_and_sea_bdy = args.land_and_sea_bdy
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
    hgt_m = infil.variables["HGT_M"][:].squeeze()
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

    # 修正内陆出现的海洋区域（两种方法二选一）
    rows, cols = scwcopy.shape
    if land_and_sea_bdy is not None and os.path.isfile(land_and_sea_bdy):
        print(f"[Method-1] 使用边界文件进行海陆判定（点在陆地面外 => 海洋）")
        print(f"           边界文件：{land_and_sea_bdy}")
        try:
            mask = ocean_mask_from_shapefile(lons, lats, land_and_sea_bdy)  # 1/0 掩膜
        except Exception as e:
            print(f"边界文件判定失败（{e}），回退到感染算法 [Method-2].")
            mask = infection_algorithm(scwcopy.squeeze(), hgt_m=hgt_m, sea_level=0.0, tol=0.01, merge_lakes=True)
    else:
        print("[Method-2] 未提供边界文件或文件不存在，使用感染算法（边界连通 + HGT≈0）")
        mask = infection_algorithm(scwcopy.squeeze(), hgt_m=hgt_m, sea_level=0.0, tol=0.01, merge_lakes=True)
    newocemask = refine_ocean_with_landuse(mask, landusef, land_threshold=0.5, water_index=15)
    
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
    
