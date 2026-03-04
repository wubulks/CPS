from netCDF4 import Dataset
import glob
from tqdm import tqdm
import numpy as np
from multiprocessing import Process
import argparse

parser = argparse.ArgumentParser(description='Process some integers.')
#with short name
parser.add_argument('-lons','--lonsize', help='lonsize of west_east grids in wrfout',required=True)
parser.add_argument('-lats','--latsize', help='latsize of south_north grids in wrfout',required=True)
parser.add_argument('-cpus','--cpusize', help='cpusize',required=True)

lons=int(parser.parse_args().lonsize)
lats=int(parser.parse_args().latsize)
cpus=int(parser.parse_args().cpusize)

def refil(elmindex, latlon2d, old, oldtype):
    temp = np.zeros((latlon2d[0],latlon2d[1]), dtype=oldtype)
    temp[:] = -9999
    temp_re = temp.reshape(latlon2d[0]*latlon2d[1], order='F')
    temp_re[elmindex-1] = old
    temp_new = temp_re.reshape(latlon2d[0], latlon2d[1], order='F')
    return temp_new
    
    


def postprocess_one_fil(infil):
    latlon2d=[lats,lons]
    infil=infil
    ncin=Dataset(infil,"r")
    outfil=infil.replace(".nc","_remap.nc")
    ncout=Dataset(outfil,"w",format="NETCDF4")
    #set missing value to -9999
    #create dimensions
    ncout.createDimension("time",None)
    ncout.createDimension("lat",latlon2d[0])
    ncout.createDimension("lon",latlon2d[1])
    ncout.createDimension("soilsnow",ncin.dimensions["soilsnow"].size)
#    ncout.createDimension("vegnodes",ncin.dimensions["vegnodes"].size)
    ncout.createDimension("rtyp",ncin.dimensions["rtyp"].size)
    ncout.createDimension("band",ncin.dimensions["band"].size)
    ncout.createDimension("lake",ncin.dimensions["lake"].size)
    ncout.createDimension("soil",ncin.dimensions["soil"].size)
    ncout.createDimension("sensor",ncin.dimensions["sensor"].size)
    #copy global attributes from input file to output file
    ncout.setncatts(ncin.__dict__)

    # target_var_list=['ATSK','TSK','PRAVG','AHFX','ALFX','HFX','ASWDNS','ASWUPS','ALWDNS','ALWUPS','AT2M','AQ2M','PSFC']
    # tagetvar_list=['f_t_grnd','f_xy_rain','f_fsena','f_fevpa','f_lfevpa','f_rnet','f_tref','f_t_soisno','f_wliq_soisno','f_qref','f_fevpl','f_fevpg']
    # varlist=tagetvar_list#ncin.variables.keys()
    varlist=ncin.variables.keys()
    #tagetvar_list=['f_xy_solarin','f_fevpl','f_fevpg','f_fevpa','f_xy_frl']
    #varlist=tagetvar_list
    
    elmindex = ncin.variables["elmindex"][:]

    new_elmindex = np.zeros((latlon2d[0],latlon2d[1]), dtype=int)
    new_elmindex[:] = -9999
    new_elmindex_re = new_elmindex.reshape(latlon2d[0]*latlon2d[1], order='F')
    new_elmindex_re[elmindex-1] = elmindex
    new_elmindex = new_elmindex_re.reshape(latlon2d[0], latlon2d[1], order='F')
    
    ncout.createVariable("elmindex",ncin.variables["elmindex"].dtype,("lat","lon"))
    ncout.variables["elmindex"][:]=new_elmindex
    ncout.variables["elmindex"].missing_value=-9999
    for var in varlist:
        shape_of_var=ncin.variables[var].shape
        if len(shape_of_var)>1:
            #the first dimension is always time so we don't need to worry about it
            #the second dimension is always the number of elements
            #if there are the third dimension is the vertical dimension
            #now reshape the element dimension to be lat,lon
            if len(shape_of_var)==2:
                ncout.createVariable(var,ncin.variables[var].dtype,("time","lat","lon"))
                temp = ncin.variables[var][:]
                new_temp = np.zeros((shape_of_var[0],latlon2d[0],latlon2d[1]), dtype=ncin.variables[var].dtype)
                new_temp[:] = -9999
                
                for itime in range(shape_of_var[0]):
                    new_temp[itime,:,:] = refil(elmindex, latlon2d, temp[itime,:], ncin.variables[var].dtype)
                
                # temp=ncin.variables[var][:].reshape((shape_of_var[0],latlon2d[0],latlon2d[1]),order="F")
                ncout.variables[var][:]=new_temp
                #set missing value to -9999
                ncout.variables[var].missing_value=-9999
            elif len(shape_of_var)==3:
                if "lake" in ncin.variables[var].dimensions:
                    ncout.createVariable(var,ncin.variables[var].dtype,("time","lat","lon","lake"))
                    # ncout.variables[var][:]=ncin.variables[var][:].reshape((shape_of_var[0],latlon2d[0],latlon2d[1],shape_of_var[2]),order="F")
                    temp = ncin.variables[var][:]
                    new_temp = np.zeros((shape_of_var[0],latlon2d[0],latlon2d[1],shape_of_var[2]), dtype=ncin.variables[var].dtype)
                    new_temp[:] = -9999
                    for itime in range(shape_of_var[0]):
                        for i3d in range(shape_of_var[2]):
                            new_temp[itime,:,:,i3d] = refil(elmindex, latlon2d, temp[itime,:,i3d], ncin.variables[var].dtype)
                    ncout.variables[var][:]=new_temp
                    ncout.variables[var].missing_value=-9999
                if "soil" in ncin.variables[var].dimensions:
                    ncout.createVariable(var,ncin.variables[var].dtype,("time","lat","lon","soil"))
                    # ncout.variables[var][:]=ncin.variables[var][:].reshape((shape_of_var[0],latlon2d[0],latlon2d[1],shape_of_var[2]),order="F")
                    temp = ncin.variables[var][:]
                    new_temp = np.zeros((shape_of_var[0],latlon2d[0],latlon2d[1],shape_of_var[2]), dtype=ncin.variables[var].dtype)
                    new_temp[:] = -9999
                    for itime in range(shape_of_var[0]):
                        for i3d in range(shape_of_var[2]):
                            new_temp[itime,:,:,i3d] = refil(elmindex, latlon2d, temp[itime,:,i3d], ncin.variables[var].dtype)
                    ncout.variables[var][:]=new_temp
                    ncout.variables[var].missing_value=-9999
                if "soilsnow" in ncin.variables[var].dimensions:
                    ncout.createVariable(var,ncin.variables[var].dtype,("time","lat","lon","soilsnow"))
                    # ncout.variables[var][:]=ncin.variables[var][:].reshape((shape_of_var[0],latlon2d[0],latlon2d[1],shape_of_var[2]),order="F")
                    temp = ncin.variables[var][:]
                    new_temp = np.zeros((shape_of_var[0],latlon2d[0],latlon2d[1],shape_of_var[2]), dtype=ncin.variables[var].dtype)
                    new_temp[:] = -9999
                    for itime in range(shape_of_var[0]):
                        for i3d in range(shape_of_var[2]):
                            new_temp[itime,:,:,i3d] = refil(elmindex, latlon2d, temp[itime,:,i3d], ncin.variables[var].dtype)
                    ncout.variables[var][:]=new_temp
                    ncout.variables[var].missing_value=-9999
                if "sensor" in ncin.variables[var].dimensions:
                    ncout.createVariable(var,ncin.variables[var].dtype,("time","lat","lon","sensor"))
                    # ncout.variables[var][:]=ncin.variables[var][:].reshape((shape_of_var[0],latlon2d[0],latlon2d[1],shape_of_var[2]),order="F")
                    temp = ncin.variables[var][:]
                    new_temp = np.zeros((shape_of_var[0],latlon2d[0],latlon2d[1],shape_of_var[2]), dtype=ncin.variables[var].dtype)
                    new_temp[:] = -9999
                    for itime in range(shape_of_var[0]):
                        for i3d in range(shape_of_var[2]):
                            new_temp[itime,:,:,i3d] = refil(elmindex, latlon2d, temp[itime,:,i3d], ncin.variables[var].dtype)
                    ncout.variables[var][:]=new_temp
                    ncout.variables[var].missing_value=-9999
            elif len(shape_of_var)==4:
                if "rtyp" in ncin.variables[var].dimensions:
                    ncout.createVariable(var,ncin.variables[var].dtype,("time","lat","lon","rtyp",'band'))
                    # ncout.variables[var][:]=ncin.variables[var][:].reshape((shape_of_var[0],latlon2d[0],latlon2d[1],shape_of_var[2],shape_of_var[3]),order="F")
                    temp = ncin.variables[var][:]
                    new_temp = np.zeros((shape_of_var[0],latlon2d[0],latlon2d[1],shape_of_var[2],shape_of_var[3]), dtype=ncin.variables[var].dtype)
                    new_temp[:] = -9999
                    for itime in range(shape_of_var[0]):
                        for i3d in range(shape_of_var[2]):
                            for i4d in range(shape_of_var[3]):
                                new_temp[itime,:,:,i3d,i4d] = refil(elmindex, latlon2d, temp[itime,:,i3d,i4d], ncin.variables[var].dtype)
                    ncout.variables[var][:]=new_temp
                    ncout.variables[var].missing_value=-9999

    #compute time variable based on the file name
    time=ncin.variables["time"]
    timeunits=time.units
    timelong_name=time.long_name
    timevar=ncout.createVariable("time",time.dtype,("time",))
    timevar.units=timeunits
    timevar.long_name=timelong_name
    timevar[:]=time[:]
    ncout.close()



count=0
for fil in tqdm(glob.glob("*_hist_????*.nc")[:]):
    if "remap" in fil:
        continue
    #if the remap file already exists, skip it
    if glob.glob(fil.replace(".nc","_remap.nc")):
        continue
    job=Process(target=postprocess_one_fil,args=(fil,))
    job.start()
    count+=1
    if count==cpus:
        job.join()
        count=0
