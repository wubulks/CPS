# Import modules
from pysheds.grid import Grid
from netCDF4 import Dataset
import numpy as np
import xarray as xr
import rasterio
from rasterio.transform import from_origin
from pyproj import CRS
import f90nml as nml

# Read DEM and LANDMASK
HGT_geo = np.array(Dataset('geo_em.d01.nc').variables['HGT_M'][0, :, :])
landmsk = np.array(Dataset('geo_em.d01.nc').variables['LANDMASK'][0, :, :])

# Replace invalid values in DEM (e.g., negative or NaN)
HGT_geo = np.where(np.isnan(HGT_geo) | (HGT_geo < 0), 0, HGT_geo)

# Read projection parameters from namelist.wps
nmlf = nml.read('namelist.wps')
central_latitude = float(nmlf['geogrid']['ref_lat'])
central_longitude = float(nmlf['geogrid']['ref_lon'])
true_lat1 = float(nmlf['geogrid']['truelat1'])
true_lat2 = float(nmlf['geogrid']['truelat2'])
resolution = float(nmlf['geogrid']['dx'])

# Create a CRS object from the proj4 string
crs = CRS.from_proj4(
    f"+proj=lcc +lat_1={true_lat1} +lat_2={true_lat2} "
    f"+lat_0={central_latitude} +lon_0={central_longitude} "
    f"+x_0=0 +y_0=0 +ellps=WGS84 +units=m +no_defs"
)

# Calculate the affine transformation
transform = from_origin(-180, 90, resolution, resolution)

# Write geotiff using rasterio
with rasterio.open(
    'output.tif', 'w',
    driver='GTiff',
    height=HGT_geo.shape[0],
    width=HGT_geo.shape[1],
    count=1,
    dtype=str(HGT_geo.dtype),
    crs=crs,
    transform=transform
) as dst:
    dst.write(HGT_geo, 1)

# Begin to calculate flowdir and accumulation
# Read raw DEM
grid = Grid.from_raster('./output.tif')
dem = grid.read_raster('./output.tif')

# Fill pits and depressions, resolve flats
pit_filled_dem = grid.fill_pits(dem)
flooded_dem = grid.fill_depressions(pit_filled_dem)
inflated_dem = grid.resolve_flats(flooded_dem)

# Calculate flow direction and accumulation
fdir = grid.flowdir(inflated_dem)
acc = grid.accumulation(fdir)

# Filter flowdir negative values (set to 0)
fdir = np.where(fdir < 0, 0, fdir)

# Mask out ocean
acc = acc * landmsk
fdir = fdir * landmsk

# Save to NetCDF
acc = xr.DataArray(acc, dims=('south_north', 'west_east'), name='acc')
acc.to_netcdf('acc.nc')

fd = xr.DataArray(fdir, dims=('south_north', 'west_east'), name='fdir')
fd.to_netcdf('fdir.nc')

print("Flow direction and accumulation have been successfully computed!")
