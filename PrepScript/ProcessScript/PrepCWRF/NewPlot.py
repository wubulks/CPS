import argparse
import time
from pathlib import Path
from typing import Dict, List
import os
import shapely.geometry as sgeom
import numpy as np
import xarray as xr
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from cnmaps import get_adm_maps
import geopandas as gpd
import cmaps

# ------------------- 1. 默认配置 -------------------
DefaultDict = dict(
    dem_coarsen=3,
    draw_lake=True,
    draw_river=True,
    draw_province=True,
    draw_country=True,
    draw_city=False,
    shapefile='',
)

# ------------------- 2. 参数解析 -------------------
def parse_args():
    p = argparse.ArgumentParser(description="生成带地形的 WRF 网格示意图", formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument("--casename", default="Yangtze")
    p.add_argument("--proj", default="LAMBERT", choices=["LAMBERT"])
    p.add_argument("--RefLat", type=float, default=29.5)
    p.add_argument("--RefLon", type=float, default=113.5)
    p.add_argument("--True_Lat1", type=float, default=27)
    p.add_argument("--True_Lat2", type=float, default=33)
    p.add_argument("--dx_WE", type=float, default=6000.0)
    p.add_argument("--dy_SN", type=float, default=6000.0)
    p.add_argument("--EdgeNum_WE", type=int, default=288)
    p.add_argument("--EdgeNum_SN", type=int, default=208)
    p.add_argument("--savepath", type=str, default="")
    p.add_argument("--BdyWidth", type=int, default=15)
    p.add_argument("--topodir", type=str, default="/stu01/wumej22/CWRF/CWPS_GEOG/geog_new/geog_wm_modified_lake/topo_30s/")
    p.add_argument("--plotcfg", nargs="*", default=[])
    return p.parse_args()

def parse_plot_cfg(DefaultDict, items):
    newDict = DefaultDict.copy()
    for item in items:
        if "=" not in item: continue
        k, v = item.split("=", 1)
        k_str = k.strip().lower()
        v_str = v.strip()
        if k_str in ("draw_lake", "draw_river", "draw_province", "draw_country", "draw_city"):
            newDict[k_str] = v_str.lower() == "true"
        elif k_str == "dem_coarsen":
            newDict[k_str] = int(v_str)
        elif k_str == "shapefile":
            newDict[k_str] = v_str
    return newDict

# ------------------- 3. 几何计算 -------------------
def cal_corner_coords(g, proj):
    x0 = -((g["e_we"] - 1) / 2) * g["dx"]
    y0 = -((g["e_sn"] - 1) / 2) * g["dy"]
    x1 = x0 + g["e_we"] * g["dx"]
    y1 = y0 + g["e_sn"] * g["dy"]
    bdx, bdy = g["BdyWidth"] * g["dx"], g["BdyWidth"] * g["dy"]
    lam = dict(left=x0, right=x1, bottom=y0, top=y1)
    inner = dict(left=x0+bdx, right=x1-bdx, bottom=y0+bdy, top=y1-bdy)
    return lam, inner

def calc_bbox_wgs_from_lambert(corner_lam, proj, n_samples=400, expand_deg=0.2):
    x = np.linspace(corner_lam["left"], corner_lam["right"], n_samples)
    y = np.linspace(corner_lam["bottom"], corner_lam["top"], n_samples)
    xx, yy = np.meshgrid(x, y)
    pts = ccrs.PlateCarree().transform_points(proj, xx.flatten(), yy.flatten())
    lons, lats = pts[:, 0], pts[:, 1]
    ok = ~np.isnan(lons) & ~np.isnan(lats)
    return lons[ok].min()-expand_deg, lons[ok].max()+expand_deg, lats[ok].min()-expand_deg, lats[ok].max()+expand_deg

# ------------------- 4. DEM 读取 -------------------
def read_elevation(topodir, bbox_wgs, fac):
    minlon, maxlon, minlat, maxlat = bbox_wgs
    known_lat, known_lon, ddeg = -89.99583, -179.99583, 0.00833333
    tile_x = 1200
    tx_min, tx_max = (minlon - known_lon) / ddeg, (maxlon - known_lon) / ddeg
    ty_min, ty_max = (minlat - known_lat) / ddeg, (maxlat - known_lat) / ddeg
    xstarts = list(range(1, 43200, tile_x))
    ystarts = list(range(1, 21600, tile_x))
    xid = [i for i, xs in enumerate(xstarts) if xs <= tx_max and (xs+tile_x-1) >= tx_min]
    yid = [j for j, ys in enumerate(ystarts) if ys <= ty_max and (ys+tile_x-1) >= ty_min]
    if not xid or not yid: return xr.DataArray(np.zeros((10,10)), dims=("lat","lon"), coords={"lat":np.linspace(minlat,maxlat,10),"lon":np.linspace(minlon,maxlon,10)})
    
    big = np.zeros((len(yid)*tile_x, len(xid)*tile_x), np.int16)
    for ix, i in enumerate(xid):
        for iy, j in enumerate(yid):
            path = Path(topodir) / f"{xstarts[i]:05d}-{xstarts[i]+tile_x-1:05d}.{ystarts[j]:05d}-{ystarts[j]+tile_x-1:05d}"
            if path.exists():
                tile = np.fromfile(path, dtype=">i2").reshape(tile_x+6, tile_x+6)[3:-3, 3:-3]
                big[iy*tile_x:(iy+1)*tile_x, ix*tile_x:(ix+1)*tile_x] = tile
    lons = known_lon + (xstarts[xid[0]]-1 + np.arange(big.shape[1])) * ddeg
    lats = known_lat + (ystarts[yid[0]]-1 + np.arange(big.shape[0])) * ddeg
    da = xr.DataArray(big, coords=[("lat", lats), ("lon", lons)], name="topo")
    if fac > 1:
        da = da.coarsen(lat=fac, lon=fac, boundary="trim").mean().astype("float32")
    return da

# ------------------- 5. 刻度修正逻辑 -------------------
def _lambert_ticks(ax, ticks, tick_location, line_constructor, n_samples=1000):
    p_xlim = ax.get_xlim()
    p_ylim = ax.get_ylim()
    trans = ax.transData + ax.transAxes.inverted()
    
    tick_positions = []
    tick_labels = []

    for t in ticks:
        # 获取采样点并转换
        lonlat = line_constructor(t, n_samples, ax.get_extent(ccrs.PlateCarree()))
        proj_pts = ax.projection.transform_points(ccrs.Geodetic(), lonlat[:, 0], lonlat[:, 1])[..., :2]
        valid = ~np.isnan(proj_pts).any(axis=1)
        if not np.any(valid): continue
        
        ax_pts = trans.transform(proj_pts[valid])
        
        if tick_location in ['bottom', 'top']:
            target_y = 0.0 if tick_location == 'bottom' else 1.0
            # 只有当经纬线与轴交点在 0.001-0.999 范围内时才保留
            mask = (ax_pts[:, 0] >= 0.001) & (ax_pts[:, 0] <= 0.999) & (np.abs(ax_pts[:, 1] - target_y) < 0.01)
            if np.any(mask):
                idx = np.argmin(np.abs(ax_pts[mask, 1] - target_y))
                tick_positions.append(proj_pts[valid][mask][idx, 0])
                tick_labels.append(t)
        else:
            target_x = 0.0 if tick_location == 'left' else 1.0
            mask = (ax_pts[:, 1] >= 0.001) & (ax_pts[:, 1] <= 0.999) & (np.abs(ax_pts[:, 0] - target_x) < 0.01)
            if np.any(mask):
                idx = np.argmin(np.abs(ax_pts[mask, 0] - target_x))
                tick_positions.append(proj_pts[valid][mask][idx, 1])
                tick_labels.append(t)
    return tick_positions, tick_labels

# ------------------- 6. 绘图 -------------------
def create_map(grid, proj, lam, inner, elev, cfg, args):
    fig = plt.figure(figsize=(12, 10))
    ax = plt.axes(projection=proj)
    ax.set_extent([lam['left'], lam['right'], lam['bottom'], lam['top']], crs=proj)

    # 地形
    elev_land = elev.where(elev > 0)
    pcm = ax.pcolormesh(elev_land.lon, elev_land.lat, elev_land, cmap=cmaps.WhiteBlueGreenYellowRed, 
                        shading="auto", vmax=2000, vmin=0, transform=ccrs.PlateCarree(), zorder=0)
    plt.colorbar(pcm, ax=ax, shrink=0.6, pad=0.04, label="Elevation (m)")

    # 行政区划 - 修复 AttributeError: 'list' object has no attribute 'geometry'
    def add_china_map(level, ec, lw):
        try:
            # 显式指定 engine="geopandas" 确保返回 GeoDataFrame
            gdf = get_adm_maps(level=level, engine="geopandas")
            # 如果依然返回了 list，将其转换为 GeoDataFrame 或直接合并几何
            if isinstance(gdf, list):
                # 这种情况下通常 list 里是 shapely 对象
                ax.add_geometries(gdf, ccrs.PlateCarree(), fc="none", ec=ec, lw=lw)
            else:
                ax.add_geometries(gdf.geometry, ccrs.PlateCarree(), fc="none", ec=ec, lw=lw)
        except Exception as e:
            print(f"Warning: Failed to draw {level} boundaries: {e}")

    if cfg["draw_province"]: add_china_map("省", "k", 0.6)
    if cfg["draw_country"]: add_china_map("国", "k", 1.2)
    if cfg["draw_city"]: add_china_map("市", "gray", 0.3)

    # 基础要素
    ax.add_feature(cfeature.OCEAN.with_scale("50m"), facecolor="#49E0E1", zorder=1)
    if cfg["draw_lake"]:
        ax.add_feature(cfeature.LAKES.with_scale("10m"), facecolor="blue", alpha=0.3, zorder=2)

    # 内边框
    bx, by = [inner[k] for k in ["left","right","right","left","left"]], [inner[k] for k in ["bottom","bottom","top","top","bottom"]]
    ax.plot(bx, by, transform=proj, color="black", linewidth=2.5, zorder=10)

    # 刻度计算
    lon_min, lon_max, lat_min, lat_max = calc_bbox_wgs_from_lambert(lam, proj)
    lon_ticks = np.arange(np.floor(lon_min), np.ceil(lon_max)+1, max(1, round((lon_max-lon_min)/6)))
    lat_ticks = np.arange(np.floor(lat_min), np.ceil(lat_max)+1, max(1, round((lat_max-lat_min)/6)))

    # 网格线
    gl = ax.gridlines(crs=ccrs.PlateCarree(), draw_labels=False, linewidth=0.6, color="gray", alpha=0.5, linestyle="--")
    gl.xlocator = mticker.FixedLocator(lon_ticks)
    gl.ylocator = mticker.FixedLocator(lat_ticks)

    # 应用严格边缘刻度
    xticks, xl = _lambert_ticks(ax, lon_ticks, 'bottom', lambda t, n, b: np.vstack((np.zeros(n)+t, np.linspace(b[2], b[3], n))).T)
    ax.set_xticks(xticks)
    ax.set_xticklabels([f"{l:.1f}°E" for l in xl])
    
    yticks, yl = _lambert_ticks(ax, lat_ticks, 'left', lambda t, n, b: np.vstack((np.linspace(b[0], b[1], n), np.zeros(n)+t)).T)
    ax.set_yticks(yticks)
    ax.set_yticklabels([f"{l:.1f}°N" for l in yl])

    title = f"{args.casename}\n"
    title+= f"RefLat={args.RefLat} RefLon={args.RefLon} True_Lat1={args.True_Lat1} True_Lat2={args.True_Lat2}\n"
    title+= f"dx={args.dx_WE} dy={args.dy_SN} EdgeNum_WE={args.EdgeNum_WE} EdgeNum_SN={args.EdgeNum_SN} BdyWidth={args.BdyWidth}\n"
    plt.title(title)
    
    out = args.savepath if args.savepath else f"./{args.casename}_map.png"
    plt.savefig(out, dpi=300, bbox_inches="tight")
    plt.close()

if __name__ == "__main__":
    args = parse_args()
    cfg = parse_plot_cfg(DefaultDict, args.plotcfg)
    grid = dict(lat_0=args.RefLat, lon_0=args.RefLon, lat_1=args.True_Lat1, lat_2=args.True_Lat2,
                dx=args.dx_WE, dy=args.dy_SN, BdyWidth=args.BdyWidth, e_we=args.EdgeNum_WE, e_sn=args.EdgeNum_SN)
    proj = ccrs.LambertConformal(central_longitude=grid["lon_0"], central_latitude=grid["lat_0"],
                                 standard_parallels=(grid["lat_1"], grid["lat_2"]))
    lam, inner = cal_corner_coords(grid, proj)
    bbox = calc_bbox_wgs_from_lambert(lam, proj)
    elev = read_elevation(args.topodir, bbox, cfg["dem_coarsen"])
    create_map(grid, proj, lam, inner, elev, cfg, args)



