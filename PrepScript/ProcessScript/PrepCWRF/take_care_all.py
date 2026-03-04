import os
import time
from netCDF4 import Dataset
import numpy as np
import xarray as xr


time0 = time.time()
print("\n\n=====> Start take_care_all.py <=====")

with Dataset("geo_em.d01_veg.nc", "r+") as fw: 
    namelist = ["CHWD", "XALBSOL", "XLANDMASK", "SC_LAND", "XFVEG", "XFLOWACN"]
    MO = ["XYZ", "XY ", "XY ", "XY ", "XY ", "XY "]
    des = ["channel widths across flowdir", "soil albedo (visible beam) when wet", 
           "Landmask : 1=land, 0=water", "LAND USE INDEX", "vegetation fraction", "FlowACN"]
    UNI = ["m", "", "none", "", "fraction", ""]
    
    # 记录原始数据类型
    original_dtypes = {}
    for name in namelist:
        original_dtypes[name] = fw.variables[name].dtype
    
    # 修改属性
    for i, name in enumerate(namelist):
        var = fw.variables[name]
        var.MemoryOrder = MO[i]
        var.stagger = 'M'
        var.description = des[i]
        var.units = UNI[i]
        print(f"Modified attributes for: {name}")
    
    # 处理 LANDF 变量
    if 'LANDF' in fw.variables:
        landf = fw.variables['LANDF']
        landf.units = ""
        landf.description = "fraction of land cover in a grid"
        landf.stagger = "M"
        landf.MemoryOrder = "XY "
        
        # 复制其他属性（如果存在）
        sample_var = fw.variables[namelist[0]]  # 使用第一个变量作为样本
        if hasattr(sample_var, 'sr_y'):
            landf.sr_y = sample_var.sr_y
        if hasattr(sample_var, 'sr_x'):
            landf.sr_x = sample_var.sr_x
        if hasattr(sample_var, 'FieldType'):
            landf.FieldType = sample_var.FieldType
    
    # 强制同步到磁盘
    fw.sync()

print("Time used: %.2f seconds" % (time.time() - time0))
print("=====> Finish take_care_all.py <=====")


print("\n\n=====> Start fix_geo_em.py <=====")
time0 = time.time()

def compare_and_fix_attributes(original_file, modified_file):
    # 打开原始和修改后的 NetCDF 文件
    original_ds = xr.open_dataset(original_file)
    modified_ds = xr.open_dataset(modified_file)

    # 遍历修改后的文件中的所有变量
    for var_name in modified_ds.data_vars:
        # 获取修改后的变量和原始文件中的变量（如果存在）
        original_var = original_ds.get(var_name, None)
        modified_var = modified_ds[var_name]
        
        # 强制转换数据类型为 float32，除了 'Times' 变量
        if var_name != 'Times':
            if modified_var.dtype != np.float32:
                print(f"Variable '{var_name}' dtype mismatch: "
                      f"Original dtype = {original_var.dtype}, Modified dtype = {modified_var.dtype}")
                # 强制转换数据类型
                modified_ds[var_name] = modified_var.astype(np.float32)
                print(f"Fixed dtype of '{var_name}' to float32.")
        
        # 如果原始文件中没有该变量，跳过属性修正
        if original_var is None:
            print(f"Variable '{var_name}' is not present in original file. Skipping attribute correction.")
            continue
       
        # 校正变量的属性（如 description, units 等）
        for attr in original_var.attrs:
            if attr not in modified_var.attrs:
                print(f"Variable '{var_name}' is missing attribute '{attr}' in modified file.")
                modified_var.attrs[attr] = original_var.attrs[attr]
                print(f"Added missing attribute '{attr}' to '{var_name}'.")

            elif original_var.attrs[attr] != modified_var.attrs[attr]:
                print(f"Variable '{var_name}' attribute '{attr}' mismatch: "
                      f"Original value = {original_var.attrs[attr]}, "
                      f"Modified value = {modified_var.attrs[attr]}")
                modified_var.attrs[attr] = original_var.attrs[attr]
                print(f"Fixed attribute '{attr}' of '{var_name}'.")

    # 保存修正后的文件
    modified_ds.to_netcdf("geo_em_temp.nc", format="NETCDF4")
    print(f"Modified file saved to geo_em_temp.nc.")

    # 关闭文件
    original_ds.close()
    modified_ds.close()

# 使用方法
original_file = "geo_em.d01.nc"  # 修改前的文件路径
modified_file = "geo_em.d01_veg.nc"  # 修改后的文件路径

compare_and_fix_attributes(original_file, modified_file)

print("Time used: %.2f seconds" % (time.time() - time0))
print("=====> Finish fix_geo_em.py <=====")