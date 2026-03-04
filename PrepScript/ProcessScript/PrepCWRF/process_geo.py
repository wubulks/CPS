import numpy as np
import xarray as xr
from scipy import stats
from scipy.ndimage import binary_dilation
from scipy.interpolate import griddata
from scipy.spatial import cKDTree
from joblib import Parallel, delayed
import argparse
import os
import time

np.set_printoptions(threshold=np.inf)

argparser = argparse.ArgumentParser()
argparser.add_argument('-res', '--resolution', type=float, help='resolution of the input data', required=True)
argparser.add_argument('-lk', '--lake_threshold', type=float, help='lake threshold', required=True)
argparser.add_argument('-cpu', '--ncpu', type=int, help="cpu num for parallel", default=1)
args = argparser.parse_args()
resolution = args.resolution    # dx = dy = resolution
ncpu = args.ncpu
lake_threshold = args.lake_threshold
land_threshold = 1 - lake_threshold
land_threshold = 0.0 if land_threshold < 0.0 else land_threshold


os.system("rm -f a.d01.nc")
os.system("rm -f b.d01.nc")
os.system("rm -f c.d01.nc")
os.system("rm -f geo_em.d01_veg.nc")

timestart = time.time()

# 首先读取原始文件的Times变量
original_fw = xr.open_dataset("geo_em.d01.nc")
original_times = original_fw['Times'].copy(deep=True)
original_dims = dict(original_fw.dims)  # 保存原始维度信息
original_fw.close()


##############################################################
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

def build_encoding(ds):
    enc = {}
    for v in ds.data_vars:
        if np.issubdtype(ds[v].dtype, np.floating):
            enc[v] = {'zlib': False, 'complevel': 0, 'dtype': 'f4'}
        else:
            enc[v] = {'zlib': False, 'complevel': 0}
    for c in ds.coords:
        # 坐标一般不改 dtype；若是浮点坐标且很大可不强制
        enc[c] = {'zlib': False, 'complevel': 0}
    return enc

##############################################################



# 打开 NetCDF 文件
fw = xr.open_dataset("geo_em.d01.nc")

# 读取 LANDMASK
landmask = fw['LANDMASK']

#----------------------------
# 处理 XMOANI
#----------------------------
print("#-----------------------------#")
time0 = time.time()
name = "XMOANI"
var = fw[name].values
var = np.where(var <= 0.3, 0.3, var)  # 处理小于 0.3 的值
fw[name].values = var
print("[a] XMOANI is done.")
print(f"Time used: {time.time() - time0:.2f} seconds")
print("#-----------------------------#\n")


#----------------------------
# 处理 XFSKY
#----------------------------
print("#-----------------------------#")
time0 = time.time()
name = "XFSKY"
var = fw[name].values
var = np.where(var <= 0.0, 1.0, var)  # 处理小于 0 的值
fw[name].values = var
print("[a] XFSKY is done.")
print(f"Time used: {time.time() - time0:.2f} seconds")
print("#-----------------------------#\n")


#----------------------------
# 处理 BEDROCK (不知道为什么要处理这个)
#----------------------------
print("#-----------------------------#")
time0 = time.time()
name = "BEDROCK"
# sf = fw['sea_floor'].values
var = fw[name].values 
fw[name].values = var
print("[a] BEDROCK is done.")
print(f"Time used: {time.time() - time0:.2f} seconds")
print("#-----------------------------#\n")


#----------------------------
# 读取 fdir 文件并赋值 FLOWDIR
#----------------------------
print("#-----------------------------#")
time0 = time.time()
fdir_fil = xr.open_dataset("./fdir.nc")
fdir = fdir_fil['fdir'].values
geoflowdir = fw['FLOWDIR'].values
geoflowdir[0, 0, :, :] = fdir
geoflowdir[0, 1, :, :] = fdir
fw['FLOWDIR'].values = geoflowdir 
fdir_fil.close()
print("[a] FLOWDIR is done.")
print(f"Time used: {time.time() - time0:.2f} seconds")
print("#-----------------------------#\n")


#----------------------------
# 读取 acc 文件并赋值 XFLOWACN
#----------------------------
print("#-----------------------------#")
time0 = time.time()
acc_fil = xr.open_dataset("./acc.nc")
acc = acc_fil['acc'].values
xflowacn = np.zeros_like(landmask.values)
xflowacn[0, :, :] = acc
# 复制landmask的属性
xflowacn = xr.DataArray(xflowacn, dims=landmask.dims, coords=landmask.coords)
xflowacn.attrs = landmask.attrs
xflowacn.attrs["description"] = "FlowACN"
xflowacn.attrs["MemoryOrder"] = "XY"
xflowacn.attrs["units"] = ""
xflowacn.attrs["stagger"] = "M"
fw['XFLOWACN'] = xflowacn
acc_fil.close()
print("[a] XFLOWACN is done.")
print(f"Time used: {time.time() - time0:.2f} seconds")
print("#-----------------------------#\n")


#----------------------------
# 处理 FR_SAND
#----------------------------
print("#-----------------------------#")
time0 = time.time()
# 参数配置
name = "FR_SAND"
INVALID_VALUES = [-999, 1e19]  # 原始数据中的无效值标记
SPECIAL_VALUE = -1.0           # 需要特殊处理的值
SPECIAL_REPLACEMENT = 0.5      # 特殊值的替换值
# 获取原始数据
raw_data = fw[name].values
# 1. 识别无效值（在缩放前进行，确保准确识别）
invalid_mask = np.isin(raw_data, INVALID_VALUES) | (raw_data >= 1e19)
# 2. 转换为浮点数并缩放
scaled_data = raw_data.astype(np.float32) / 10000.0  # 转换单位
# 3. 获取陆面掩码（仅使用第一个时间步）
# landmask 可能是 DataArray，这里转成 ndarray 再扩维
land_mask = (landmask[0].values == 1)          # (y, x) -> ndarray
land_mask_expanded = land_mask[None, None, :, :]   # (1,1,y,x)
# 4. 识别需要替换的负值（仅限于陆面且非无效值）
negative_mask = (scaled_data < 0) & land_mask_expanded & ~invalid_mask
# 5. 用表层值替换负值（避免重复创建大广播数组）
surface_values = scaled_data[:, 0:1, :, :]                 # (t,1,y,x)
surface_broadcast = np.broadcast_to(surface_values, scaled_data.shape)
scaled_data[negative_mask] = surface_broadcast[negative_mask]
# 6. 处理无效值和特殊值
# 将无效值设置为标准无效标记 (-999)
scaled_data[invalid_mask] = -999.0
# 处理特殊值 (-1.0 替换为 0.5)
special_value_mask = np.isclose(scaled_data, SPECIAL_VALUE) & ~invalid_mask
scaled_data[special_value_mask] = SPECIAL_REPLACEMENT
# 7. 回写处理后的数据
fw[name].values = scaled_data
# 输出处理信息
print(f"[a] FR_SAND is done.")
print(f"Time used: {time.time() - time0:.2f} seconds")
print("#-----------------------------#\n")



#----------------------------
# 处理 FR_CLAY
#----------------------------
print("#-----------------------------#")
time0 = time.time()
name = "FR_CLAY"
var = fw[name].values
var = var / 10000.0  # 转换单位
# 替换无效值
var = np.where((var == -999) | (var == 1e20), -999, var)
var = np.where(var == -1.0, 0.5, var)
fw[name].values = var
print("[a] FR_CLAY is done.")
print(f"Time used: {time.time() - time0:.2f} seconds")
print("#-----------------------------#\n")


#----------------------------
# 修正过小的 FR_SAND 和 FR_CLAY
#----------------------------
print("#-----------------------------#")
time0 = time.time()
tsc = fw['FR_SAND'].values + fw['FR_CLAY'].values
frs = fw['FR_SAND'].values
frc = fw['FR_CLAY'].values
frs = np.where(tsc < 0.1, 0.5, frs)
frc = np.where(tsc < 0.1, 0.5, frc)
fw['FR_SAND'].values = frs
fw['FR_CLAY'].values = frc
print("[a] FR_SAND and FR_CLAY are merged.")
print(f"Time used: {time.time() - time0:.2f} seconds")
print("#-----------------------------#\n")


#----------------------------
# 处理 CHWD
#----------------------------
print("#-----------------------------#")
time0 = time.time()
var = fw['FLOWDIR'].values
var = var * resolution
var = xr.DataArray(var, dims=fw['FLOWDIR'].dims, coords=fw['FLOWDIR'].coords)
var.attrs = fw['FLOWDIR'].attrs
var.attrs["units"] = "m"
var.attrs["description"] = "channel widths across flowdir"
fw["CHWD"] = var
print("[a] CHWD is done.")
print(f"Time used: {time.time() - time0:.2f} seconds")
print("#-----------------------------#\n")

#----------------------------
# 处理 BEDROCK
#----------------------------
print("#-----------------------------#")
time0 = time.time()
name = "BEDROCK"
var = fw[name].values
var = np.where(var <= 0.2, 0.2, var) # 处理小于 0.2 的值
fw[name].values = var
print("[a] BEDROCK is done.")
print(f"Time used: {time.time() - time0:.2f} seconds")
print("#-----------------------------#\n")


#----------------------------
# 处理 XALBSOL
#----------------------------
print("#-----------------------------#")
time0 = time.time()
name = "XALBSOL"
solour_table = np.array([0.12, 0.11, 0.10, 0.09, 0.08, 0.07, 0.06, 0.05],
                        dtype=np.float32)
scw = fw["SC_WATER"]  # xarray.DataArray，形状通常是 (time, y, x)
# 把分类码转换为查表索引：1..8 -> 0..7
idx = np.rint(scw.values).astype(np.int64) - 1
# 对越界做裁剪（若存在异常值，不至于报错）
idx = np.clip(idx, 0, solour_table.size - 1)
# 查表：同形状直接得到目标值（广播/向量化）
mapped = np.take(solour_table, idx)
# 组回 xarray，并保留/更新属性
var = xr.DataArray(mapped, dims=scw.dims, coords=scw.coords)
var.attrs = fw['SC_WATER'].attrs
var.attrs["description"] = "soil albedo (visible beam) when wet"
var.attrs["MemoryOrder"] = "XY"
var.attrs["units"] = ""
var.attrs["stagger"] = "M"
fw[name] = var 
print("[a] XALBSOL is done.")
print(f"Time used: {time.time() - time0:.2f} seconds")
print("#-----------------------------#\n")

#----------------------------
# 处理 LANDUSEF 和 LANDMASK 一致性
#----------------------------
#  0  Ocean
#  1  Urban and Built-Up Land
#  2  Dryland Cropland and Pasture
#  3  Irrigated Cropland and Pasture
#  4  Mixed Dryland/Irrigated Cropland and Pasture
#  5  Cropland/Grassland Mosaic
#  6  Cropland/Woodland Mosaic
#  7  Grassland
#  8  Shrubland
#  9  Mixed Shrubland/Grassland
# 10  Savanna
# 11  Deciduous Broadleaf Forest 
# 12  Deciduous Needleleaf Forest 
# 13  Evergreen Broadleaf Forest
# 14  Evergreen Needleleaf Forest
# 15  Mixed Forest
# 16  Water Bodies
# 17  Herbaceous Wetland
# 18  Wooded Wetland
# 19  Barren or Sparsely Vegetated
# 20  Herbaceous Tundra
# 21  Wooded Tundra
# 22  Mixed Tundra
# 23  Bare Ground Tundra
# 24  Snow or Ice
print("#-----------------------------#")
time0 = time.time()
name = "LANDF, LANDUSEF CONSISTENCY"
var = np.zeros_like(fw['LANDMASK'].values, dtype='float32')
landusef = fw['LANDUSEF'].values
landmask = fw['LANDMASK'].values
landusef = np.where(landusef == -9999, -999, landusef)
landusef = np.where(landusef > 1.0e10, 1.0e20, landusef)
landmask = np.where(landmask == -9999, -999, landmask)
landmask = np.where(landmask > 1.0e10, 1.0e20, landmask)

# 修正 landusef 的值
scwater = fw['SC_WATER'].values
dplake = fw['DPLAKE'].values
oceanfile = xr.open_dataset("ocean_mask.nc")
ocean = oceanfile['OCEANMASK'].values
oceandiff = oceanfile['OCEANMASK_DIFF'].values

landusef = np.where(landusef < 0, 0, landusef)

# 校准数据(重新归一化)
# 检查归一化结果
sums = landusef.sum(axis=1)
if not np.allclose(sums, 1.0):
    raise ValueError("LANDUSEF does not sum to 1.0 across the first axis.")

dim = landusef.shape
print(dim)
# 处理 landusef 的值
m = 0
n = 0

'''确认网格中主流的土地利用类型'''
index_lf = np.zeros_like(var, dtype='int')
for i in range(dim[2]):
    for j in range(dim[3]):
        index_lf[0, i, j] = np.argmax(landusef[0, :, i, j]) + 1  # 获取主流地表类型
        if landusef[0, 15, i, j] > lake_threshold:
            index_lf[0, i, j] = 16
        if int(index_lf[0, i, j]) == 16: # 如果是水体
            if (landusef[0, 15, i, j] < lake_threshold) & (lake_threshold <= 1.0) :  # 如果水体比例小于阈值
                rxum = landusef[0, 15, i, j] # 保存原始值
                landusef[0, 15, i, j] = -1.0 # 设置为无效值
                index_lf[0, i, j] = np.argmax(landusef[0, :, i, j]) + 1 # 重新获取主流地表类型
                m += 1 # 计数
                landusef[0, 15, i, j] = rxum # 恢复原始值
                print(f"water but landusef < {lake_threshold} at {i}, {j} with landusef = {landusef[0, 15, i, j]}")
            if (oceandiff[0, i, j] == 1): # 如果湖泊深度小于阈值
                rxum = landusef[0, 15, i, j] # 保存原始值
                landusef[0, 15, i, j] = -1.0 # 设置为无效值
                index_lf[0, i, j] = np.argmax(landusef[0, :, i, j]) + 1 # 重新获取主流地表类型
                n += 1 # 计数
                landusef[0, 15, i, j] = rxum # 恢复原始值
                print(f"ocean but dplake <= 0 at {i}, {j} with landusef = {landusef[0, 15, i, j]}")
            
print("LANDUSEF consistency is done.")
print(f"water but landusef less than {lake_threshold} in {m} points") # 主流地表类型为水体，但水体比例小于阈值的点数
print(f"ocean but dplake <= 0 in {n} points") # 主流地表类型为水体，但湖泊深度小于阈值的点数

xoro = fw['XORO'].values  # 读取 XORO： 0-海洋，1-陆地，2-海冰，3-湖泊
index_lf = np.where((index_lf == 24) & (xoro == 0), 14, index_lf)   
index_lf = np.where(index_lf == 24, 22, index_lf)
fw['LU_INDEX'].values = index_lf.astype('float32')

# 重新计算 XORO
newxoro = np.zeros_like(xoro)
newxoro = np.nan
newxoro = np.where((index_lf == 16) & (ocean == 1), 0, newxoro)
newxoro = np.where((index_lf == 16) & (ocean != 1), 3, newxoro)
newxoro = np.where((index_lf != 16), 1, newxoro)
newxoro = np.where((newxoro == np.nan), 1, newxoro)
newxoro = np.where((xoro == 2), 2, newxoro)
xoro = newxoro
print(f"XORO: Ocean: {np.sum(xoro == 0)}, Land: {np.sum(xoro == 1)}, Sea Ice: {np.sum(xoro == 2)}, Lake: {np.sum(xoro == 3)}")
xorocopy = xoro.copy().squeeze()
zero_mask = (xorocopy == 0) # 创建一个掩码，标记 xorocopy == 0 的区域
dilated_zero_mask = binary_dilation(zero_mask, structure=np.ones((3, 3))) # 扩展邻域（这里用5x5区域）
fix_mask = (xorocopy == 3) & dilated_zero_mask # 找到 xorocopy == 3 且在扩展的 zero_mask 区域内的点
xorocopy[fix_mask] = 0 # 将这些点的值修正为 0
xoro[0, :, :] = xorocopy


var2 =[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16, 17, 18, 19, 20, 21, 22, 23] 
landf = np.zeros_like(landmask, dtype='float32')
for i in range(len(var2)):
    landf = landf + landusef[0, var2[i], :, :]
var = xr.DataArray(landf, dims=fw['LANDMASK'].dims, coords=fw['LANDMASK'].coords)
var.attrs = fw['LANDMASK'].attrs
var.attrs["description"] = "fraction of land cover in a grid"
for i in range(dim[2]):
    for j in range(dim[3]):
        # 条件 1: 水体网格 (index_lf == 16)，但 landf > 阈值 或 xoro == 1
        if (index_lf[0, i, j] == 16) & ((landf[0, i, j] > land_threshold) | (xoro[0, i, j] == 1)):
            landusef[0,:,i,j] = landusef[0,:,i,j] * (land_threshold-0.001) / landf[0,i,j]
            landusef[0,15,i,j] = 1 - np.sum(landusef[0,:,i,j]) + landusef[0,15,i,j]
            landf[0,i,j] = land_threshold - 0.001
            print(f"bad point, water but landf > {land_threshold} at {i}, {j} with landf = {landf[0, i, j]} and landusef = {landusef[0,15,i,j]} correted to {landf[0,i,j]}")
        
        # 条件 2: 非水体网格 (index_lf != 16)，但 landf < 阈值 或 xoro 不符合条件
        if (index_lf[0, i, j] != 16) & ((landf[0, i, j] < land_threshold) | ((xoro[0, i, j] != 1) & (xoro[0, i, j] != 2))):
            # 检查 landf 是否为零或无效值
            if np.isnan(landf[0, i, j]) or landf[0, i, j] == 0:
                print(f"Warning: landf[0, {i}, {j}] is invalid ({landf[0, i, j]}). Skipping...")
                continue  # 跳过无效点
            landusef[0,:,i,j] = landusef[0,:,i,j] * (land_threshold+0.001) / landf[0,i,j]
            tpv = 1 - np.sum(landusef[0,:,i,j])
            for k in range(24):
                if k != 15:
                    landusef[0,k,i,j] = landusef[0,k,i,j] + tpv/23
            landf[0,i,j] = land_threshold + 0.001
            print(f"bad point, land but landf < {land_threshold} at {i}, {j} with landf = {landf[0,i,j]} and xoro = {xoro[0,i,j]} and index_lf = {index_lf[0,i,j]}, correted to {landf[0,i,j]}")
        
# 水体网格，陆地占比不能大于陆地阈值
landf = np.where(((index_lf == 16) & (landf >= land_threshold)), land_threshold-0.001, landf) 
# 非水体网格，陆地占比不能小于陆地阈值
landf = np.where(((index_lf != 16) & (landf <= land_threshold)), land_threshold+0.001, landf)

var = np.where( ((index_lf == 16) & ((landf > land_threshold) | (xoro == 1))) |
                ((index_lf != 16) & ((landf < land_threshold) | (~np.isin(xoro, [1, 2])))), 1, 0)

oooo = np.where((index_lf == 16) & ((landf > land_threshold) | (xoro == 1)), 1, 0)
print(f"water but landf >= {land_threshold} | xoro == 1: {np.sum(oooo)}")

oooo = np.where((index_lf == 16) & (landf > land_threshold), 1, 0)
print(f"water but landf > {land_threshold}: {np.sum(oooo)}")

oooo = np.where((index_lf == 16) & (xoro == 0), 1, 0)
print(f"water but xoro == 0: {np.sum(oooo)}")

oooo = np.where((index_lf == 16) & (xoro == 1), 1, 0)
print(f"water but xoro == 1: {np.sum(oooo)}")

oooo = np.where((index_lf == 16) & (xoro == 2), 1, 0)
print(f"water but xoro == 2: {np.sum(oooo)}")

oooo = np.where((index_lf == 16) & (xoro == 3), 1, 0)
print(f"water but xoro == 3: {np.sum(oooo)}")    

oooo = np.where((index_lf == 16) & (landf > land_threshold), 1, 0)
print(f"water but landf > {land_threshold}: {np.sum(oooo)}")

if (np.sum(var) > 0):
    print(f"bad points: {np.sum(var)}")
    print(f" Error points LU_INDEX is 16 while landf bigger than {land_threshold} or XORO is 1")
    print("Scipt is terminated.")
    # exit()

# 修正 xoro
for i in range(dim[2]):
    for j in range(dim[3]):
        condition1 = (index_lf[0, i, j] == 16) & ((landf[0, i, j] > land_threshold) | (xoro[0, i, j] == 1))
        condition2 = (index_lf[0, i, j] != 16) & ((landf[0, i, j] < land_threshold) | (xoro[0, i, j] != 1) & (xoro[0, i, j] != 2))
        if condition1 | condition2:
            print(index_lf[0, i, j], landf[0, i, j], xoro[0, i, j])
            xoro[0, i, j] = 1

fw['XORO'].values = xoro

# 处理 SC_WATER
newscwater = np.zeros_like(scwater)
newscwater = np.where(xoro == 1, scwater, newscwater)
newscwater = np.where(xoro == 2, scwater, newscwater)
newscwater = np.where((xoro == 3) & (dplake > 20), 6, newscwater)
newscwater = np.where((xoro == 3) & (dplake <= 20) & (dplake > 0), 5, newscwater)
newscwater = np.where((xoro == 0), 8, newscwater)

# 处理landf
landf = np.where(xoro == 3, 0.0, landf)  # 湖泊地表类型的 landf 为 0 

landf = xr.DataArray(landf, dims=fw['LANDMASK'].dims, coords=fw['LANDMASK'].coords)
landf.attrs = fw['LANDMASK'].attrs
landf.attrs["description"] = "fraction of land cover in a grid"
landf.attrs["MemoryOrder"] = "XY"
landf.attrs["units"] = ""
landf.attrs["stagger"] = "M"
fw['LANDF'] = landf
fw['LANDUSEF'].values = landusef
fw['SC_WATER'].values = newscwater


print("[a] LANDF, LANDUSEF and SC_WATER are done.")
print(f"Time used: {time.time() - time0:.2f} seconds")
print("#-----------------------------#\n")


#----------------------------
# 处理 LANDMASK
#----------------------------
print("#-----------------------------#")
time0 = time.time()
name = "XLANDMASK"
lnd = fw['LANDMASK']
lnd_new = np.zeros_like(lnd.values)
lnd_new = np.where(index_lf == 16, 0.0, 1.0)
lnd_new = xr.DataArray(lnd_new, dims=lnd.dims, coords=lnd.coords)
lnd_new.attrs = lnd.attrs
fw[name] = lnd_new
fw['LANDMASK'] = lnd_new
print("[a] LANDMASK is done.")
print(f"Time used: {time.time() - time0:.2f} seconds")
print("#-----------------------------#\n")


#----------------------------
# 处理 BEDROCK
#----------------------------
print("#-----------------------------#")
time0 = time.time()
name = "BEDROCK"
bedrock = fw[name].values
name = "DPLAKE"
dplake = fw[name].values
name = "sea_floor"
sea_floor = fw[name].values
for i in range(dim[2]):
    for j in range(dim[3]):
        # 读取 XORO： 0-海洋，1-陆地，2-海冰，3-湖泊
        if xoro[0, i, j] == 0:  # 海洋
            if ( (sea_floor[0, i, j] < 0) & (sea_floor[0, i, j] > -500)):
                bedrock[0, i, j] = abs(sea_floor[0, i, j])*100
            else :
                bedrock[0, i, j] = 10
                
        if xoro[0, i, j] == 2:  # 海冰
            if ( (sea_floor[0, i, j] < 0) & (sea_floor[0, i, j] > -500)):
                bedrock[0, i, j] = abs(sea_floor[0, i, j])*100 
            else :
                bedrock[0, i, j] = 10
                
        if xoro[0, i, j] == 3:  # 湖泊
            if ( (dplake[0, i, j] > 0) & (dplake[0, i, j] < 300)):
                bedrock[0, i, j] = dplake[0, i, j]       
            else :
                bedrock[0, i, j] = 1
                
fw['BEDROCK'].values = bedrock
print("[a] BEDROCK is done.")
print(f"Time used: {time.time() - time0:.2f} seconds")
print("#-----------------------------#\n")
        
# 保存数据,禁用压缩以获取最快的IO速度
encoding = build_encoding(fw)
fw.to_netcdf("a.d01.nc", format="NETCDF4", encoding=encoding)
fw.close()

print("======================================================")
print("XUMCHECK  Add Variables Over.")
print("======================================================\n\n")


#----------------------------
# 处理一些非湖泊但无值的格点
#----------------------------
print("#-----------------------------#")
time0 = time.time()
names_2d = {
    "OA1"       : 0,
    "OA2"       : 0,
    "OA3"       : 0,
    "OA4"       : 0,
    "OC1"       : 0,
    "OSD"       : 0,
    "SNOALB"    : 0,
    "SOILTEMP"  : 0,
    "VAR_SSO"   : 0,
    "XFSKY"     : 1,
    "XHSDV"     : -9999,
    "XMOANG"    : -9999,
    "XMOANI"    : 0.3,
    "XMOSTD"    : -9999,
    "XORASP"    : -9999,
    "XORSLO"    : -9999,
    "XSGASP"    : -9999,
    "XSLPX"     : 0,
    "XSLPXS"    : -9999,
    "XSLPY"     : 0,
    "XSLPYS"    : -9999,
    "XSOSLO"    : -9999,
    "XSOSTD"    : -9999,
    "slpxgrid"  : -9999,
    "slpygrid"  : -9999,
}   
names_3d = {
    "ALBEDO12M" : 8,
    "CEC"       : -9999,
    "FR_CLAY"   : 0,
    "FR_PH"     : -9999,
    "GRAVEL"    : -9999,
    "GREENFRAC" : 0,
    "OC"        : -9999,
    "RBD"       : -9999,
}
infile = xr.open_dataset("a.d01.nc")
landmask = infile['LANDMASK'].values.squeeze()
landmask_1d = (landmask == 1)   # 陆地掩码

# 优化后的插值函数
def interpolate_2d_var(name, var, invalid_value, landmask_1d):
    """处理2D变量的插值"""
    var = var.copy()
    # 使用布尔索引而不是np.where，效率更高
    mask = landmask_1d & (var == invalid_value)
    var[mask] = np.nan
    result = interpolate_nan_2d(data=var, method='linear', invalid_value=invalid_value)
    return name, np.expand_dims(result, axis=0)


def interpolate_3d_var(name, var, invalid_value, landmask_1d):
    """处理3D变量的插值"""
    var = var.copy()
    result = np.empty_like(var)
    # 预计算掩码，避免在循环中重复计算
    mask_3d = np.zeros_like(var, dtype=bool)
    for ilayer in range(var.shape[0]):
        mask_3d[ilayer] = landmask_1d & (var[ilayer] == invalid_value)
    # 使用向量化操作处理所有层
    var[mask_3d] = np.nan
    # 逐层插值
    for ilayer in range(var.shape[0]):
        result[ilayer] = interpolate_nan_2d(
            data=var[ilayer], method='linear', invalid_value=invalid_value
        )
    return name, np.expand_dims(result, axis=0)

if ncpu == 1:
    # 单线程处理
    results_2d = {}
    for name, value in names_2d.items():
        var = infile[name].values.squeeze()
        _, result = interpolate_2d_var(name, var, value, landmask_1d)
        results_2d[name] = result

    results_3d = {}
    for name, value in names_3d.items():
        var = infile[name].values.squeeze()
        _, result = interpolate_3d_var(name, var, value, landmask_1d)
        results_3d[name] = result
    # 批量更新变量值
    for name, result in results_2d.items():
        print(f"Interpolating [2D] {name}")
        infile[name].values = result
    for name, result in results_3d.items():
        print(f"Interpolating [3D] {name}")
        infile[name].values = result
else:
    # 进程并行（稳定），注意进程数不要超过磁盘能力（一般 8 比较稳）
    ncpu = min(ncpu, 6)
    # 处理2D变量
    tasks_2d = [(name, infile[name].values.squeeze().copy(), value, landmask_1d) for name, value in names_2d.items()]
    with Parallel(n_jobs=ncpu, backend="loky", timeout=100000, return_as="generator") as parallel:
        gen = parallel(
            delayed(interpolate_2d_var)(*task)
            for task in tasks_2d
        )
        for name, res in gen:
            print(f"Interpolating [2D] {name}")
            infile[name].values = res
    # 处理3D变量
    tasks_3d = [(name, infile[name].values.squeeze().copy(), value, landmask_1d) for name, value in names_3d.items()]
    with Parallel(n_jobs=ncpu, backend="loky",timeout=100000,  return_as="generator") as parallel:
        gen = parallel(
            delayed(interpolate_3d_var)(*task)
            for task in tasks_3d
        )
        for name, res in gen:
            print(f"Interpolating [3D] {name}")
            infile[name].values = res
# 保存数据,禁用压缩以获取最快的IO速度
encoding = build_encoding(infile)
infile.to_netcdf("a0.d01.nc", encoding=encoding)
infile.close()
print("[a0] Water bodies attributes check is done.")
print(f"Time used: {time.time() - time0:.2f} seconds")
print("#-----------------------------#\n")




#----------------------------
# 处理 植被数据
#----------------------------
print("#-----------------------------#")
time0 = time.time()
infile = xr.open_dataset("a0.d01.nc")

lu_index = infile['LU_INDEX']
infile['SC_LAND'] = lu_index
sc_water = infile['SC_WATER'].values
xlandmask = infile['XLANDMASK'].values
sc_water = np.where((sc_water == 8) & (xlandmask == 1), 6, sc_water)
infile['SC_WATER'].values = sc_water

inveg  = xr.open_dataset("MODIS2CWRF_SBC_d01.nc")
hi_res_var = inveg['FVC'].values[:, 0, :, :]*1.0
xlandmask = infile['XLANDMASK'].values
hi_res_var = np.where(xlandmask == 0, 0, hi_res_var)
hi_res_var = np.where((hi_res_var == -999) |  (hi_res_var == 1e20), 0, hi_res_var)
hi_res_var = xr.DataArray(hi_res_var, dims=infile['SC_WATER'].dims, coords=infile['SC_WATER'].coords)
hi_res_var.attrs = infile['SC_WATER'].attrs
hi_res_var.attrs["description"] = "vegetation fraction"
hi_res_var.attrs["units"] = "fraction"
hi_res_var.attrs["stagger"] = "M"
hi_res_var.attrs["MemoryOrder"] = "XY"
infile['XFVEG'] = hi_res_var
print("[b] XFVEG is done.")
print(f"Time used: {time.time() - time0:.2f} seconds")
print("#-----------------------------#\n")

# 保存数据,禁用压缩以获取最快的IO速度
encoding = build_encoding(infile)
infile.to_netcdf("b.d01.nc", encoding=encoding)
infile.close()

print("======================================================")
print("add XFVEG Over.")
print("======================================================\n\n")


#----------------------------
# 处理 SOIL 数据
#----------------------------
print("#-----------------------------#")
time0 = time.time()
fw = xr.open_dataset("b.d01.nc")
upoint = 0
landmask = fw['LANDMASK'].values
fsand = fw['FR_SAND'].values
fclay = fw['FR_CLAY'].values
dim = fsand.shape
for i in range(dim[2]):
    for j in range(dim[3]):
        if landmask[0, i, j] == -1:
            for k in range(dim[1]):
                if (fsand[0, k, i, j] < 0) | (fsand[0, k, i, j]) > 100:
                    print(f"fsand[{k}, {i}, {j}] = {fsand[0, k, i, j]}, landmask[{k}, {i}, {j}] = {landmask[0, i, j]}")
                    upoint += 1
                if (fclay[0, k, i, j] < 0) | (fclay[0, k, i, j]) > 100:
                    print(f"fclay[{k}, {i}, {j}] = {fclay[0, k, i, j]}, landmask[{k}, {i}, {j}] = {landmask[0, i, j]}")
                    upoint += 1

print(f"undefied points= {upoint}")

print("[c] Soil data check is done.")
print(f"Time used: {time.time() - time0:.2f} seconds")
print("#-----------------------------#\n")

#----------------------------
# 检查并修正2D数据
#----------------------------
print("#-----------------------------#")
time0 = time.time()
names_2d        = ["BEDROCK","XFSKY","XFVEG","XHSDV","XMOANG","XMOANI","XMOSTD","XORASP","XORO","XORSLO","XSGASP","XSLPXS","XSLPYS","XSOSLO","XSOSTD"]
upbd_2d         = [10       ,1      ,1      ,1000   ,20      ,1       ,8000    ,10      ,1     ,1       ,5       ,1       ,1       ,1       ,500     ]
lwbd_2d         = [0.3      ,0.5    ,0.0    ,0.001  ,0.0001  ,0       ,0.000   ,0.001   ,0     ,0       ,0.01    ,0       ,0       ,0       ,0       ]
typicalvalue_2d = [1.2      ,0.7    ,0.1    ,0.01   ,0.1     ,0.7     ,0       ,0.1     ,1     ,0       ,0.1     ,0       ,0       ,0       ,0       ]
landmask = fw['LANDMASK'].values

for idx, name in enumerate(names_2d):
    field_2d = fw[name].values
    print(f"Checking {name}")
    # 创建与 field_2d 相同形状的 mask2d
    mask2d = (landmask[0, :, :] > 0.5)
    # 替换超出上下界的值或缺失值
    field_2d = np.where(
        mask2d & ((np.isnan(field_2d)) | (field_2d > upbd_2d[idx]) | (field_2d < lwbd_2d[idx])),
        typicalvalue_2d[idx], field_2d
    )
    field_2d = np.where(
        ~mask2d & ((np.isnan(field_2d)) | (field_2d > upbd_2d[idx]) | (field_2d < lwbd_2d[idx])),
        0, field_2d
    )
    fw[name].values = field_2d

print("[c] 2D data check is done.")
print(f"Time used: {time.time() - time0:.2f} seconds")
print("#-----------------------------#\n")

#----------------------------
# 检查并修正3D数据
#----------------------------
print("#-----------------------------#")
time0 = time.time()

names_3d        = ["XSALF","LANDUSEF","SOILCBOT","SOILCTOP","XSGSRF"]
upbd_3d         = [3.0    ,1.0       ,1.0       ,1.0       ,5.0     ]
lwbd_3d         = [0.01   ,0.0       ,0.0       ,0.0       ,0.0     ]
typicalvalue_3d = [1.0    ,0.1       ,0.0       ,0.0       ,0.1     ]

# 2D 陆面掩码（y,x）
landmask_2d = (fw["LANDMASK"].values[0, :, :] > 0.5)
def fix_one_var(fw, varname, lo, hi, typval, landmask_2d):
    arr = fw[varname].values  # 可能是 (k,y,x) 或 (t,k,y,x)
    # 确保浮点（典型值大多为浮点）
    if not np.issubdtype(arr.dtype, np.floating):
        arr = arr.astype(np.float32, copy=True)
    # 构造与 arr 同形状的 3D/4D 掩码：仅在 (y,x) 上应用 landmask
    if arr.ndim == 3:          # (k, y, x)
        mask3d = landmask_2d[None, :, :]          # (1,y,x) 广播到 (k,y,x)
    elif arr.ndim == 4:        # (t, k, y, x)
        mask3d = landmask_2d[None, None, :, :]    # (1,1,y,x) 广播到 (t,k,y,x)
    else:
        raise ValueError(f"{varname}: unexpected ndim={arr.ndim}, expected 3 or 4")
    # 需要修正的位置：陆面 且 (NaN 或 >上界 或 <下界)
    bad = mask3d & (np.isnan(arr) | (arr > hi) | (arr < lo))
    # 一次性替换
    arr[bad] = typval
    # 回写（就地）
    fw[varname].values[...] = arr

# 执行
for name, lo, hi, tv in zip(names_3d, lwbd_3d, upbd_3d, typicalvalue_3d):
    print(f"Checking {name}")
    fix_one_var(fw, name, lo, hi, tv, landmask_2d)

# 可选：无压缩加快写盘
encoding = build_encoding(fw)
fw.to_netcdf("c.d01.nc", engine="netcdf4", format="NETCDF4", encoding=encoding)
fw.close()

print("[c] 3D fields check is done.")
print(f"Time used: {time.time() - time0:.2f} seconds")
print("#-----------------------------#\n")

print("======================================================")
print("[c] 3D data check is done.")
print("======================================================\n\n")


#----------------------------
# 处理 一些额外的3D数据
#----------------------------
print("#-----------------------------#")
time0 = time.time()
fw = xr.open_dataset("c.d01.nc", engine="netcdf4")
landmask = fw['LANDMASK'].values
landmask_2d = (landmask.squeeze() > 0.5)
water_mask_2d = ~landmask_2d

names_3d         = ["ALBEDO12M","CEC"  ,"FR_PH", "GRAVEL", "GREENFRAC", "OC"  , "RBD", "XSALF"]
typicalvalue_3d  = [8          ,-9999  ,-9999  , -9999   , 0          , -9999 , -9999, -9999  ]

def fill_water_with_typical(arr:np.ndarray, varname: str, typval: float):
    # 确保浮点，避免 -9999 在整型里溢出
    if not np.issubdtype(arr.dtype, np.floating):
        arr = arr.astype(np.float32, copy=True)

    if arr.shape[-2:] != landmask_2d.shape:
        raise ValueError(f"{varname}: last two dims {arr.shape[-2:]} != LANDMASK {landmask_2d.shape}")
    # 广播掩码
    if arr.ndim == 3:      # (k,y,x) 或 (time,y,x)
        mask = water_mask_2d[None, :, :]
    elif arr.ndim == 4:    # (t,k,y,x)
        mask = water_mask_2d[None, None, :, :]
    else:
        raise ValueError(f"{varname}: unexpected ndim {arr.ndim}")
    # 使用 where（支持广播），得到更新后的数组
    arr_new = np.where(mask, typval, arr)
    # 关键：把结果放回 Dataset（而不是 .values[...]）
    return arr_new
    
for name, tv in zip(names_3d, typicalvalue_3d):
    print(f"Checking {name}")
    arr  = fw[name].values
    arr_new = fill_water_with_typical(arr, name, tv)
    fw[name][:] = arr_new  # 或者：fw[varname][:] = arr_new

# 不压缩写盘以获得最快 I/O（顺序访问可加 contiguous）
encoding = build_encoding(fw)
fw.to_netcdf("d.d01.nc", engine="netcdf4", format="NETCDF4", encoding=encoding)
fw.close()

print("[d] Extra 3D data check is done.")
print(f"Time used: {time.time() - time0:.2f} seconds")
print("#-----------------------------#\n")
print("======================================================")
print("[d] Extra 3D data check is done.")
print("======================================================\n\n")

#----------------------------
# 修正时间维度
#----------------------------
print("#-----------------------------#")
time0 = time.time()
ds = xr.open_dataset("d.d01.nc")
# 确保Times变量存在且正确
if 'Times' in ds.variables:
    # 删除可能被修改的Times变量
    ds = ds.drop_vars('Times', errors='ignore')
# 重新添加原始Times变量
ds['Times'] = original_times
# 确保维度名称正确
if 'string19' in ds.dims:
    ds = ds.rename({'string19': 'DateStrLen'})
# 在保存文件之前，为每个变量设置编码
encoding = {}
for var_name in fw.data_vars:
    # 获取变量的原始数据类型
    original_dtype = fw[var_name].dtype
    encoding[var_name] = {
        'zlib': False,
        'complevel': 0,
        'dtype': original_dtype  # 强制以原始类型写入磁盘
    }
# 特别确保 Times 变量正确存储为字符型
if 'Times' in fw.variables:
    encoding['Times'] = {'dtype': 'S1'} 

ds.to_netcdf("geo_em.d01_veg.nc", encoding=encoding)
print("[e] Fix time dimension is done.")
print(f"Time used: {time.time() - time0:.2f} seconds")
print("#-----------------------------#\n")

print("======================================================")
print("      All Over, Time used: {:.2f} seconds".format(time.time() - timestart))
print("======================================================\n\n")


