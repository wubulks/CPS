import numpy as np
from netCDF4 import Dataset
from scipy.ndimage import generic_filter
# 使用周围5*5的网格进行插值


def interpolate_nonzero(data):
    """
    对输入数据进行插值，使用周围不为0的值的平均值。
    """
    def nonzero_mean(values):
        nonzero_values = values[values > 0]
        return nonzero_values.mean() if nonzero_values.size > 0 else 0

    return generic_filter(data, nonzero_mean, size=3, mode='constant', cval=0)







# ###################################################
# #                  修正水体数据
# ###################################################
print("\nStart to correct water data...")
# 打开 wrfinput_d01 文件
wrfinp = Dataset("./wrfinput_d01", mode="r+")
oceanmask = Dataset("./ocean_mask.nc", mode="r")['OCEANMASK'][:].squeeze()


scwater = wrfinp.variables["SC_WATER"][0, :, :]
lu_index = wrfinp.variables["LU_INDEX"][0, :, :]
landmask = wrfinp.variables["LANDMASK"][0, :, :]
xlandmask = wrfinp.variables["XLANDMASK"][0, :, :]
xland = wrfinp.variables["XLAND"][0, :, :]
landf = wrfinp.variables["LANDF"][0, :, :]
xoro = wrfinp.variables["XORO"][0, :, :]
sc_landu = wrfinp.variables["SC_LANDU"][0, :, :]
vegfra = wrfinp.variables["VEGFRA"][0, :, :]
xfveg = wrfinp.variables["XFVEG"][0, :, :]
dplake = wrfinp.variables["DPLAKE"][0, :, :]

ind_lu_index = np.where(lu_index == 16, 1, 0)
ind_scwater = np.where((scwater == 5) | (scwater == 6) | (scwater == 8), 1, 0)
ind_landmask = np.where(landmask == 0, 1, 0)
ind_xlandmask = np.where(xlandmask == 0, 1, 0)
ind_xland = np.where(xland == 2, 1, 0)
ind_landf = np.where(landf == 0, 1, 0)
ind_xoro = np.where(xoro == 0, 1, 0)
ind_sc_landu = np.where(sc_landu == 16, 1, 0)


scw_lu = ind_scwater - ind_lu_index
print(f"scw_lu: {np.sum(scw_lu)}")
lm_lu = ind_landmask - ind_lu_index
print(f"lm_lu: {np.sum(lm_lu)}")
xlm_lu = ind_xlandmask - ind_lu_index
print(f"xlm_lu: {np.sum(xlm_lu)}")
xl_lu = ind_xland - ind_lu_index
print(f"xl_lu: {np.sum(xl_lu)}")
lf_lu = ind_landf - ind_lu_index
print(f"lf_lu: {np.sum(lf_lu)}")
xo_lu = ind_xoro - ind_lu_index
print(f"xo_lu: {np.sum(xo_lu)}")
sl_lu = ind_sc_landu - ind_lu_index
print(f"sl_lu: {np.sum(sl_lu)}")



# # 修正 SC_WATER
# newluindex = lu_index.copy()
# newscwater = np.zeros_like(scwater)
# newscwater = np.where(newluindex != 16, 2, newscwater)
# newscwater = np.where((newluindex == 16) & (dplake > 20), 6, newscwater)
# newscwater = np.where((newluindex == 16) & (dplake <= 20) & (dplake > 0), 5, newscwater)
# newscwater = np.where((newluindex == 16) & (dplake == 0), 8, newscwater)
# newocemask = np.where(newscwater == 8, 1, 0)
# diff = newocemask - oceanmask
# addocean = np.where(diff == 1, 1, 0)
# if np.sum(addocean) > 0:
#     print(f"新增海洋区域：{np.sum(addocean)}")
#     print("新增海洋区域不合理！")
#     newscwater[addocean == 1] = 2
#     newluindex = np.where(addocean == 1, 6, newluindex)

# newxoro = np.where(newluindex == 16, 0, 1)
# newlandmask = np.where(newluindex == 16, 0, 1)
# newxlandmask = np.where(newluindex == 16, 0, 1)
# newxland = np.where(newluindex == 16, 2, 1)
# newlandf = np.where(newluindex == 16, 0, landf)
# newlandf = np.where((newluindex != 16) & (landf == 0), 0.99, newlandf)
# newsc_landu = newluindex
# newvegfra = np.where(newluindex == 16, 0, vegfra)
# newvegfra = np.where((newluindex != 16) & (vegfra == 0), 0.5, newvegfra)
# newxfveg = np.where(newluindex == 16, 0, xfveg)
# newxfveg = np.where((newluindex != 16) & (xfveg == 0), 0.5, newxfveg)


# 修正 SC_WATER
newluindex = lu_index.copy()
print((xlandmask == 0) & (newluindex != 16))
print("-----------------------------------")
newluindex = np.where((xlandmask == 0) & (newluindex != 16), 16, newluindex)  # 有些点存在问题，暂定
newscwater = np.zeros_like(scwater)
newscwater = np.where(newluindex != 16, 2, newscwater)
newscwater = np.where((newluindex == 16) & (dplake > 20), 6, newscwater)
newscwater = np.where((newluindex == 16) & (dplake <= 20) & (dplake > 0), 5, newscwater)
newscwater = np.where((newluindex == 16) & (dplake == 0), 8, newscwater)
newocemask = np.where(newscwater == 8, 1, 0)
diff = newocemask - oceanmask
addocean = np.where(diff == 1, 1, 0)
if np.sum(addocean) > 0:
    print(f"新增海洋区域：{np.sum(addocean)}")
    print("新增海洋区域不合理！")
    newscwater[addocean == 1] = 2
    newluindex = np.where(addocean == 1, 6, newluindex)

newxoro = np.where(newluindex == 16, 0, 1)
newlandmask = np.where(newluindex == 16, 0, 1)
newxlandmask = np.where(newluindex == 16, 0, 1)
newxland = np.where(newluindex == 16, 2, 1)
newlandf = np.where(newluindex == 16, 0, landf)
newlandf = np.where((newluindex != 16) & (landf == 0), 0.99, newlandf)
newsc_landu = newluindex
newvegfra = np.where(newluindex == 16, 0, vegfra)
mask = (newluindex != 16) & (vegfra >= 0)
newvegfra[mask] = interpolate_nonzero(newvegfra)[mask]
# newvegfra = np.where((newluindex != 16) & (vegfra == 0), 0.5, newvegfra)
newxfveg = np.where(newluindex == 16, 0, xfveg)
mask = (newluindex != 16) & (xfveg >= 0.1)
newxfveg[mask] = interpolate_nonzero(newxfveg)[mask]
# newxfveg = np.where((newluindex != 16) & (xfveg == 0), 0.5, newxfveg)








# # 保存一场数据到一个检查nc文件
# with Dataset("./check.nc", mode="w") as check:
#     check.createDimension("south_north", scwater.shape[0])
#     check.createDimension("west_east", scwater.shape[1])
#     check.createVariable("scw_lu", "f4", ("south_north", "west_east"))
#     check.createVariable("lm_lu", "f4", ("south_north", "west_east"))
#     check.createVariable("xlm_lu", "f4", ("south_north", "west_east"))
#     check.createVariable("xl_lu", "f4", ("south_north", "west_east"))
#     check.createVariable("lf_lu", "f4", ("south_north", "west_east"))
#     check.createVariable("xo_lu", "f4", ("south_north", "west_east"))
#     check.createVariable("sl_lu", "f4", ("south_north", "west_east"))
#     check.createVariable("scwater", "f4", ("south_north", "west_east"))
#     check.createVariable("lu_index", "f4", ("south_north", "west_east"))
#     check.createVariable("landmask", "f4", ("south_north", "west_east"))
#     check.createVariable("xlandmask", "f4", ("south_north", "west_east"))
#     check.createVariable("xland", "f4", ("south_north", "west_east"))
#     check.createVariable("landf", "f4", ("south_north", "west_east"))
#     check.createVariable("xoro", "f4", ("south_north", "west_east"))
#     check.createVariable("sc_landu", "f4", ("south_north", "west_east"))
#     check.createVariable("addocean", "f4", ("south_north", "west_east"))
#     check.variables["scw_lu"][:, :] = scw_lu
#     check.variables["lm_lu"][:, :] = lm_lu
#     check.variables["xlm_lu"][:, :] = xlm_lu
#     check.variables["xl_lu"][:, :] = xl_lu
#     check.variables["lf_lu"][:, :] = lf_lu
#     check.variables["xo_lu"][:, :] = xo_lu
#     check.variables["sl_lu"][:, :] = sl_lu
#     check.variables["scwater"][:, :] = newscwater
#     check.variables["lu_index"][:, :] = ind_lu_index
#     check.variables["landmask"][:, :] = ind_landmask
#     check.variables["xlandmask"][:, :] = ind_xlandmask
#     check.variables["xland"][:, :] = ind_xland
#     check.variables["landf"][:, :] = ind_landf
#     check.variables["xoro"][:, :] = ind_xoro
#     check.variables["sc_landu"][:, :] = ind_sc_landu
#     check.variables["addocean"][:, :] = addocean



# 更新数据
wrfinp.variables["SC_WATER"][0, :, :] = newscwater
wrfinp.variables["LU_INDEX"][0, :, :] = newluindex
wrfinp.variables["LANDMASK"][0, :, :] = newlandmask
wrfinp.variables["XLANDMASK"][0, :, :] = newxlandmask
wrfinp.variables["XLAND"][0, :, :] = newxland
wrfinp.variables["LANDF"][0, :, :] = newlandf
wrfinp.variables["SC_LANDU"][0, :, :] = newsc_landu
wrfinp.variables["VEGFRA"][0, :, :] = newvegfra
wrfinp.variables["XORO"][0, :, :] = newxoro
wrfinp.variables["XFVEG"][0, :, :] = newxfveg



# 关闭文件
wrfinp.close()

print("Water data has been successfully updated!")



# ###################################################
# #                  修正土壤数据
# ###################################################

# # 打开 wrfinput_d01 文件
# with Dataset("./wrfinput_d01", mode="r+") as wrfinp:
#     # 读取变量，抛弃时间轴
#     scw = wrfinp.variables["SC_WATER"][0, :, :]
#     clay = wrfinp.variables["FR_CLAY"][0, :, :, :]
#     sand = wrfinp.variables["FR_SAND"][0, :, :, :]
#     lu = wrfinp.variables["LU_INDEX"][0, :, :]

#     # 确定维度
#     nx, ny, nz = clay.shape[2], clay.shape[1], clay.shape[0]
#     print(f"nx: {nx}, ny: {ny}, nz: {nz}")

#     # 遍历每个网格点，逐点检查和修正
#     num_corrected = 0
#     for j in range(ny):
#         for i in range(nx):
#             # 如果 SC_WATER 值是 5、6、8，则跳过
#             if scw[j, i] in [5, 6, 8]:
#                 continue

#             # 遍历每一层，检查 clay 和 sand 是否需要修正
#             for k in range(nz):
#                 if clay[k, j, i] <= 0 or sand[k, j, i] <= 0:
#                     # 获取当前网格的 LU_INDEX
#                     lu_value = lu[j, i]

#                     # 使用向量化操作查找同样 LU_INDEX 的有效点
#                     valid_points = np.argwhere(
#                         (lu == lu_value) & (clay[k, :, :] > 0) & (sand[k, :, :] > 0)
#                     )

#                     if valid_points.size > 0:
#                         # 选择第一个有效点进行修正
#                         ref_j, ref_i = valid_points[0]
#                         clay[k, j, i] = clay[k, ref_j, ref_i]
#                         sand[k, j, i] = sand[k, ref_j, ref_i]
#                         num_corrected += 1
#                     else:
#                         print(f"No valid reference point found for LU_INDEX={lu_value} at ({j}, {i}, {k})")


#     print(f"Total corrected points: {num_corrected}")

#     # 写回文件
#     wrfinp.variables["FR_CLAY"][0, :, :, :] = clay
#     wrfinp.variables["FR_SAND"][0, :, :, :] = sand

# print("Soil data has been successfully updated!")

