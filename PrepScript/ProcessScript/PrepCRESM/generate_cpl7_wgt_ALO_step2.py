r'''
Author: ChenHJ
Date: 2025-01-14 15:55:43
LastEditors: ChenHJ
LastEditTime: 2025-03-07 00:30:36
FilePath: /CRESM_0306test/SCRIPTS/ICBCScripts/PrepCRESM/generate_cpl7_wgt_cwrf_colm_only.py
Description: 
'''
import numpy as np
import xarray as xr
import xesmf as xe
import esmpy as ESMF
from netCDF4 import Dataset
import os
import sys

argv = sys.argv
geogName = argv[1]
# geogName = "NECN_15km"
cwrffilname = geogName+"/geo_em.d01.nc"
colmfilname = geogName+"/colm_grd.nc"  #colm can have the same or a different grid as cwrf
pathdir = geogName+"/cpl7data/"
colmrefdata = geogName+"/CoLM_ref_"+geogName+"_vector.nc"
#colmrefdata = "/share/home/dq130/CRESM_0306test/SCRIPTS/ICBCScripts/PrepCRESM/CN_30km/unstructured_cwrf_CN30_LCT_hist_2000-05.nc"



cwrffile = xr.open_dataset(cwrffilname)
cwrf_rho_lon = cwrffile['XLONG_M'][0, :, :].values
cwrf_rho_lat = cwrffile['XLAT_M'][0, :, :].values

# cwrf_u_lon = cwrffile['XLONG_U'][0, :, :].values
# cwrf_u_lat = cwrffile['XLAT_U'][0, :, :].values

# cwrf_v_lon = cwrffile['XLONG_V'][0, :, :].values
# cwrf_v_lat = cwrffile['XLAT_V'][0, :, :].values

cwrf_rho_grid = xe.backend.Grid.from_xarray(cwrf_rho_lon.T, cwrf_rho_lat.T)
# cwrf_u_grid = xe.backend.Grid.from_xarray(cwrf_u_lon.T, cwrf_u_lat.T)
# cwrf_v_grid = xe.backend.Grid.from_xarray(cwrf_v_lon.T, cwrf_v_lat.T)

colmfile = xr.open_dataset(colmfilname)
colm_lon = colmfile['XLONG_M'][0, :, :].values
colm_lat = colmfile['XLAT_M'][0, :, :].values

fcolmdata = xr.open_dataset(colmrefdata)
elmindex = fcolmdata["elmindex"]

colm_mask = np.zeros((colm_lon.shape[0], colm_lon.shape[1]), dtype=bool)
colm_mask_re = colm_mask.reshape(colm_lon.shape[0]*colm_lon.shape[1], order='F')
colm_mask_re[elmindex.values-1] = True
colm_mask = colm_mask_re.reshape(colm_lon.shape[0], colm_lon.shape[1], order='F')
#colm_mask_da = xr.DataArray(colm_mask)
#colm_mask_da.to_netcdf("./colm_mask.nc")


# colm_data = fcolmdata["f_fgrnd"][0].values  # 假设形状为 (ny, nx)，若需要 .T 则调整
# colm_mask = ~np.isnan(colm_data)  # True 表示数据有效（非 NaN）
# fcolmdata.close()

colm_esmfgrid = xe.backend.Grid.from_xarray(colm_lon, colm_lat, mask=colm_mask)

# rho grid
cwrf_s_rho2colm_wgts_bl = pathdir + f"cwrf_s_py2colm_wgts_bl_{geogName}.nc"
cwrf_s_rho2colm_wgts_pat = pathdir + f"cwrf_s_py2colm_wgts_pat_{geogName}.nc"
colm_2cwrf_s_rho_wgt_bl = pathdir + f"colm_2cwrf_s_py_wgt_bl_{geogName}.nc"
colm_2cwrf_s_rho_wgt_pat = pathdir + f"colm_2cwrf_s_py_wgt_pat_{geogName}.nc"


regridder_cwrf_rho_2colm_bl = xe.frontend.BaseRegridder(cwrf_rho_grid, colm_esmfgrid, method="nearest_s2d", extrap_method="nearest_s2d", filename=cwrf_s_rho2colm_wgts_bl) #method="bilinear"

regridder_cwrf_rho_2colm_pat = xe.frontend.BaseRegridder(cwrf_rho_grid, colm_esmfgrid, method="nearest_s2d", extrap_method="nearest_s2d", filename=cwrf_s_rho2colm_wgts_pat) #method="patch"

regridder_colm2cwrf_rho_bl = xe.frontend.BaseRegridder(colm_esmfgrid, cwrf_rho_grid, method="nearest_s2d", extrap_method="nearest_s2d", filename=colm_2cwrf_s_rho_wgt_bl) #method="bilinear"

regridder_colm2cwrf_rho_pat = xe.frontend.BaseRegridder(colm_esmfgrid, cwrf_rho_grid, method="nearest_s2d", extrap_method="nearest_s2d", filename=colm_2cwrf_s_rho_wgt_pat) #method="patch"

print("elmindex min max:", min(elmindex), max(elmindex))

py_weights_list = [colm_2cwrf_s_rho_wgt_bl, 
                colm_2cwrf_s_rho_wgt_pat]

ncl_weights_list =["colm_2cwrf_s_wgt_bl_"+geogName+".nc", 
                "colm_2cwrf_s_wgt_pat_"+geogName+".nc"]

for pyweights, nclweights in zip(py_weights_list, ncl_weights_list):

# 打开源文件A并读取数据
    with Dataset(pyweights, 'r') as src:
        n_s = src.dimensions['n_s'].size  # 获取文件A中n_s维度的大小
        col_data = src.variables['col'][:]  # 读取col数据
        row_data = src.variables['row'][:]  # 读取row数据
        s_data = src.variables['S'][:]      # 读取S数据
    
    # print("col before:", min(col_data), max(col_data))
    # col_new = np.zeros(n_s, dtype=int)  # 创建一个新的col数组
    # for ipos in np.arange(1, elmindex.shape[0]+1):
    #     value = elmindex[ipos-1].values
    #     col_new[col_data == ipos] = value
    
    # col_data = col_new
    # print("col after:", min(col_data), max(col_data))

    with Dataset(pathdir+nclweights, 'r') as original_dataset:
        # 创建一个新的 NetCDF 文件
        with Dataset(pathdir + nclweights[:-3] + "_final.nc", 'w') as new_dataset:
            # 复制原始文件的维度（排除要删除的维度）
            for dim_name, dim in original_dataset.dimensions.items():
                if dim_name != 'n_s':  # 排除 'n_s' 维度
                    new_dataset.createDimension(dim_name, len(dim))

            # 复制原始文件的变量（排除要删除的变量）
            for var_name, var in original_dataset.variables.items():
                if var_name not in ['col', 'row', 'S']:  # 排除 'col'、'row' 和 'S' 变量
                    new_var = new_dataset.createVariable(var_name, var.dtype, var.dimensions)
                    new_var[:] = var[:]
                    for attr_name in var.ncattrs():
                        new_var.setncattr(attr_name, var.getncattr(attr_name))
        # # 创建新的col、row、S变量，使用与文件A一致的n_s维度
            new_dataset.createDimension('n_s', n_s)
            col = new_dataset.createVariable('col', 'i4', ('n_s',))  # 32位整数类型
            row = new_dataset.createVariable('row', 'i4', ('n_s',))  # 32位整数类型
            S = new_dataset.createVariable('S', 'f8', ('n_s',))      # 64位浮点类型
        # # S._FillValue = float('nan')  # 设置S的填充值

        # # 将文件A中的数据写入文件B
            col[:] = col_data
            row[:] = row_data
            S[:] = s_data

os.remove(py_weights_list[0])
os.remove(py_weights_list[1])


py_weights_list = [cwrf_s_rho2colm_wgts_bl, 
                cwrf_s_rho2colm_wgts_pat]
ncl_weights_list =["cwrf_s2colm_wgts_bl_"+geogName+".nc", 
                "cwrf_s2colm_wgts_pat_"+geogName+".nc"]

for pyweights, nclweights in zip(py_weights_list, ncl_weights_list):

# cwrf2colm，改row的值
# 打开源文件A并读取数据
    with Dataset(pyweights, 'r') as src:
        n_s = src.dimensions['n_s'].size  # 获取文件A中n_s维度的大小
        col_data = src.variables['col'][:]  # 读取col数据
        row_data = src.variables['row'][:]  # 读取row数据
        s_data = src.variables['S'][:]      # 读取S数据
    
    row_mask = np.isin(row_data,elmindex.values)
    new_n_s = np.sum(row_mask)
    print("n_s should be:", new_n_s)
    new_row_data = row_data[row_mask]
    new_col_data = col_data[row_mask]
    new_S_data = s_data[row_mask]
    print("row before:", min(row_data), max(row_data))
    
    # row_new = np.zeros(n_s, dtype=int)  # 创建一个新的row数组
    # for ipos in np.arange(1, elmindex.shape[0]+1):
    #     elmvalue = elmindex[ipos-1].values
        
    #     value = elmindex[ipos-1].values
    #     row_new[row_data == ipos] = value
    
    # row_data = row_new
    print("row after:", min(new_row_data), max(new_row_data))

    with Dataset(pathdir+nclweights, 'r') as original_dataset:
        # 创建一个新的 NetCDF 文件
        with Dataset(pathdir + nclweights[:-3] + "_final.nc", 'w') as new_dataset:
            # 复制原始文件的维度（排除要删除的维度）
            for dim_name, dim in original_dataset.dimensions.items():
                if dim_name != 'n_s':  # 排除 'n_s' 维度
                    new_dataset.createDimension(dim_name, len(dim))

            # 复制原始文件的变量（排除要删除的变量）
            for var_name, var in original_dataset.variables.items():
                if var_name not in ['col', 'row', 'S']:  # 排除 'col'、'row' 和 'S' 变量
                    new_var = new_dataset.createVariable(var_name, var.dtype, var.dimensions)
                    new_var[:] = var[:]
                    for attr_name in var.ncattrs():
                        new_var.setncattr(attr_name, var.getncattr(attr_name))
        # # 创建新的col、row、S变量，使用与文件A一致的n_s维度
            new_dataset.createDimension('n_s', new_n_s)
            col = new_dataset.createVariable('col', 'i4', ('n_s',))  # 32位整数类型
            row = new_dataset.createVariable('row', 'i4', ('n_s',))  # 32位整数类型
            S = new_dataset.createVariable('S', 'f8', ('n_s',))      # 64位浮点类型
        # # S._FillValue = float('nan')  # 设置S的填充值

        # # 将文件A中的数据写入文件B
            col[:] = new_col_data
            row[:] = new_row_data
            S[:] = new_S_data


os.remove(py_weights_list[0])
os.remove(py_weights_list[1])
#========================================================
# 直接用python生成完整的权重文件
# regridder_cwrf_rho_2colm_bl = xe.frontend.BaseRegridder(cwrf_rho_grid, colm_esmfgrid, method="bilinear", extrap_method="nearest_s2d", filename=None)

# regridder_cwrf_rho_2colm_pat = xe.frontend.BaseRegridder(cwrf_rho_grid, colm_esmfgrid, method="patch", extrap_method="nearest_s2d", filename=None)

# regridder_colm2cwrf_rho_bl = xe.frontend.BaseRegridder(colm_esmfgrid, cwrf_rho_grid, method="bilinear", extrap_method="nearest_s2d", filename=None)

# regridder_colm2cwrf_rho_pat = xe.frontend.BaseRegridder(colm_esmfgrid, cwrf_rho_grid, method="patch", extrap_method="nearest_s2d", filename=None)

# lon_cwrf_center = regridder_cwrf_rho_2colm_bl._grid_in.get_coords(0, ESMF.StaggerLoc.CENTER)
# lat_cwrf_center = regridder_cwrf_rho_2colm_bl._grid_in.get_coords(1, ESMF.StaggerLoc.CENTER)

# lon_colm_center = regridder_cwrf_rho_2colm_bl._grid_out.get_coords(0, ESMF.StaggerLoc.CENTER)
# lat_colm_center = regridder_cwrf_rho_2colm_bl._grid_out.get_coords(1, ESMF.StaggerLoc.CENTER)

# lon_cwrf_corner = regridder_cwrf_rho_2colm_bl._grid_in.get_coords(0, ESMF.StaggerLoc.CORNER)
# lat_cwrf_corner = regridder_cwrf_rho_2colm_bl._grid_in.get_coords(1, ESMF.StaggerLoc.CORNER)

# lon_colm_corner = regridder_cwrf_rho_2colm_bl._grid_out.get_coords(0, ESMF.StaggerLoc.CORNER)
# lat_colm_corner = regridder_cwrf_rho_2colm_bl._grid_out.get_coords(1, ESMF.StaggerLoc.CORNER)

# src_mask = regridder_cwrf_rho_2colm_bl._grid_in.mask[0] if regridder_cwrf_rho_2colm_bl._grid_in.has_mask else np.ones(lon_cwrf_center.shape, dtype=int)
# dst_mask = regridder_cwrf_rho_2colm_bl._grid_out.mask[0] if regridder_cwrf_rho_2colm_bl._grid_out.has_mask else np.ones(lon_colm_center.shape, dtype=int)

# # 源网格面积
# src_field = ESMF.Field(regridder._grid_in)
# src_field.get_area()
# src_area = src_field.data.flatten(order='F')

# # 目标网格面积
# dst_field = ESMF.Field(regridder._grid_out)
# dst_field.get_area()
# dst_area = dst_field.data.flatten(order='F')

# # 定义维度
# n_a = src_lon.size  # 6720
# n_b = dst_lon.size  # 241920
# n_s = len(weightdict['col'])  # 949144
# nv_a = 4
# nv_b = 4

# # 重塑顶点坐标（假设矩形网格，每个单元 4 个顶点）
# src_lon_corner_reshaped = src_lon_corner[:-1, :-1].reshape(n_a, nv_a, order='F')  # 示例重塑
# src_lat_corner_reshaped = src_lat_corner[:-1, :-1].reshape(n_a, nv_a, order='F')
# dst_lon_corner_reshaped = dst_lon_corner[:-1, :-1].reshape(n_b, nv_b, order='F')
# dst_lat_corner_reshaped = dst_lat_corner[:-1, :-1].reshape(n_b, nv_b, order='F')

# # 创建数据集
# weight_ds = xr.Dataset(
#     {
#         'src_grid_dims': ('src_grid_rank', [src_lon.shape[1], src_lon.shape[0]]),
#         'dst_grid_dims': ('dst_grid_rank', [dst_lon.shape[1], dst_lon.shape[0]]),
#         'yc_a': ('n_a', src_lat.flatten(order='F')),
#         'xc_a': ('n_a', src_lon.flatten(order='F')),
#         'yc_b': ('n_b', dst_lat.flatten(order='F')),
#         'xc_b': ('n_b', dst_lon.flatten(order='F')),
#         'yv_a': (('n_a', 'nv_a'), src_lat_corner_reshaped),
#         'xv_a': (('n_a', 'nv_a'), src_lon_corner_reshaped),
#         'yv_b': (('n_b', 'nv_b'), dst_lat_corner_reshaped),
#         'xv_b': (('n_b', 'nv_b'), dst_lon_corner_reshaped),
#         'mask_a': ('n_a', src_mask.flatten(order='F')),
#         'mask_b': ('n_b', dst_mask.flatten(order='F')),
#         'area_a': ('n_a', src_area),
#         'area_b': ('n_b', dst_area),
#         'frac_a': ('n_a', src_frac),
#         'frac_b': ('n_b', dst_frac),
#         'col': ('n_s', weightdict['col']),
#         'row': ('n_s', weightdict['row']),
#         'S': ('n_s', weightdict['S']),
#     },
#     coords={
#         'n_a': np.arange(1, n_a + 1),
#         'n_b': np.arange(1, n_b + 1),
#         'n_s': np.arange(1, n_s + 1),
#         'nv_a': np.arange(1, nv_a + 1),
#         'nv_b': np.arange(1, nv_b + 1),
#         'src_grid_rank': np.arange(1, 3),
#         'dst_grid_rank': np.arange(1, 3),
#     }
# )

# # 添加单位属性
# weight_ds['yc_a'].attrs['units'] = 'degrees'
# weight_ds['xc_a'].attrs['units'] = 'degrees'
# weight_ds['yc_b'].attrs['units'] = 'degrees'
# weight_ds['xc_b'].attrs['units'] = 'degrees'
# weight_ds['yv_a'].attrs['units'] = 'degrees'
# weight_ds['xv_a'].attrs['units'] = 'degrees'
# weight_ds['yv_b'].attrs['units'] = 'degrees'
# weight_ds['xv_b'].attrs['units'] = 'degrees'
# weight_ds['mask_a'].attrs['units'] = 'unitless'
# weight_ds['mask_b'].attrs['units'] = 'unitless'
# weight_ds['area_a'].attrs['units'] = 'square radians'
# weight_ds['area_b'].attrs['units'] = 'square radians'
# weight_ds['frac_a'].attrs['units'] = 'unitless'
# weight_ds['frac_b'].attrs['units'] = 'unitless'

# # 保存文件
# weight_ds.to_netcdf('cwrf_s2colm_wgts_bl_SC_15km_25km.nc')

#=====================================================
# u grid
#cwrf_s_u2colm_wgts_bl = pathdir + f"cwrf_s_u2colm_wgts_bl_{geogName}.nc"
#cwrf_s_u2colm_wgts_pat = pathdir + f"cwrf_s_u2colm_wgts_pat_{geogName}.nc"
#colm_2cwrf_s_u_wgt_bl = pathdir + f"colm_2cwrf_s_u_wgt_bl_{geogName}.nc"
#colm_2cwrf_s_u_wgt_pat = pathdir + f"colm_2cwrf_s_u_wgt_pat_{geogName}.nc"

#regridder_cwrf_u_2colm_bl = xe.frontend.BaseRegridder(cwrf_u_grid, colm_esmfgrid, method="bilinear", extrap_method="nearest_s2d", filename=cwrf_s_u2colm_wgts_bl)

#regridder_cwrf_u_2colm_pat = xe.frontend.BaseRegridder(cwrf_u_grid, colm_esmfgrid, method="patch", extrap_method="nearest_s2d", filename=cwrf_s_u2colm_wgts_pat)

#regridder_colm2cwrf_u_bl = xe.frontend.BaseRegridder(colm_esmfgrid, cwrf_u_grid, method="bilinear", extrap_method="nearest_s2d", filename=colm_2cwrf_s_u_wgt_bl)

#regridder_colm2cwrf_u_pat = xe.frontend.BaseRegridder(colm_esmfgrid, cwrf_u_grid, method="patch", extrap_method="nearest_s2d", filename=colm_2cwrf_s_u_wgt_pat)

# v grid
#cwrf_s_v2colm_wgts_bl = pathdir + f"cwrf_s_v2colm_wgts_bl_{geogName}.nc"
#cwrf_s_v2colm_wgts_pat = pathdir + f"cwrf_s_v2colm_wgts_pat_{geogName}.nc"
#colm_2cwrf_s_v_wgt_bl = pathdir + f"colm_2cwrf_s_v_wgt_bl_{geogName}.nc"
#colm_2cwrf_s_v_wgt_pat = pathdir + f"colm_2cwrf_s_v_wgt_pat_{geogName}.nc"

#regridder_cwrf_v_2colm_bl = xe.frontend.BaseRegridder(cwrf_v_grid, colm_esmfgrid, method="bilinear", extrap_method="nearest_s2d", filename=cwrf_s_v2colm_wgts_bl)

#regridder_cwrf_v_2colm_pat = xe.frontend.BaseRegridder(cwrf_v_grid, colm_esmfgrid, method="patch", extrap_method="nearest_s2d", filename=cwrf_s_v2colm_wgts_pat)

#regridder_colm2cwrf_v_bl = xe.frontend.BaseRegridder(colm_esmfgrid, cwrf_v_grid, method="bilinear", extrap_method="nearest_s2d", filename=colm_2cwrf_s_v_wgt_bl)

#regridder_colm2cwrf_v_pat = xe.frontend.BaseRegridder(colm_esmfgrid, cwrf_v_grid, method="patch", extrap_method="nearest_s2d", filename=colm_2cwrf_s_v_wgt_pat)
