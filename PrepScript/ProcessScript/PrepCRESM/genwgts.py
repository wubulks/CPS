import xesmf as xe
import numpy as np
import xarray as xr
from shapely.geometry import Polygon
import esmpy as ESMF
import numpy.lib.recfunctions as nprec
import matplotlib.pyplot as plt
import matplotlib.tri as tri
from matplotlib.colors import ListedColormap
from scipy.interpolate import griddata
import time


class FVCOM_MESH(xe.backend.Mesh):
    @classmethod
    def from_fvcom_file(cls, fvcom_file):
        """
        Create an ESMF.Mesh object from a fvcom output file.

        Parameters
        ----------
        fvcom_file : a fvcom surface output file includes some grid infos:
            number of node, number of element
            lon/lat at node location
            lonc/latc at element location
            nv indicating element connection

        Returns
        -------
        mesh : ESMF.Mesh
            A mesh where each triangle is represented as an Element.
        """

        ds = xr.open_dataset(fvcom_file, decode_times=False)
        lons = ds['lon'].values     # node lon
        lats = ds['lat'].values     # node lat
        lonc = ds['lonc'].values    # element lon
        latc = ds['latc'].values    # element lat
        nv = ds['nv'].values - 1    # 如果 start_index 是 1，则需要减去 1
        nv = nv.T                   # nv.shape needs to be (elem_num, 3)

        node_num = ds.node.size
        elem_num = ds.nele.size
        # Pre alloc arrays. Special structure for coords makes the code faster.
        crd_dt = np.dtype([('x', np.float32), ('y', np.float32)])
        node_coords = np.empty(node_num, dtype=crd_dt)
        node_coords['x'] = lons
        node_coords['y'] = lats
        element_types = np.full(elem_num, 3, dtype=np.uint32)  # Triangle elements
        element_conn = nv.flatten().astype(np.uint32)

        # Combine lonc and latc for element coordinates
        element_coords = np.empty((elem_num, 2), dtype=np.float32)
        element_coords[:, 0] = lonc
        element_coords[:, 1] = latc
        element_coords = element_coords.flatten()

        mesh = cls(2, 2, coord_sys=ESMF.CoordSys.SPH_DEG)

        mesh.add_nodes(
            node_num,
            np.arange(node_num) + 1,
            nprec.structured_to_unstructured(node_coords).ravel(),
            np.zeros(node_num)
        )

        # Add elements to the mesh
        try:
            mesh.add_elements(
                elem_num,
                np.arange(elem_num) + 1,
                element_types,
                element_conn,
                element_coords=element_coords
            )
        except ValueError as err:
            raise ValueError(
                'ESMF failed to create the Mesh, this usually happens when some polygons are invalid.'
            ) from err

        mesh._nv = nv

        return mesh
    
    def get_nv(self):
        return self._nv

def 

hycomfile = xr.open_dataset("./sst.nc")
lon = hycomfile.lon.values
lat = hycomfile.lat.values
lon2d, lat2d = np.meshgrid(lon, lat)
mask = ~np.isnan(hycomfile['sst'][0].values)
hycom_grid = xr.Dataset(
                {
                    "lat": (["lat", "lon"], lat2d, {"units": "degrees_north"}),
                    "lon": (["lat", "lon"], lon2d, {"units": "degrees_east"}),
                    "mask": (["lat", "lon"], mask)
                }
             )
hycom_esmfgrid = xe.backend.Grid.from_xarray(lon2d.T, lat2d.T, mask=mask.T)

# creat fvcom ESMF.mesh object
fvcom_esmfmesh = FVCOM_MESH.from_fvcom_file("Newgrid01_0001.nc")
# save some infos for later uses
node_lon = fvcom_esmfmesh.get_coords(0, meshloc=ESMF.node)
node_lat = fvcom_esmfmesh.get_coords(1, meshloc=ESMF.node)
ele_lon = fvcom_esmfmesh.get_coords(0, meshloc=ESMF.element)
ele_lat = fvcom_esmfmesh.get_coords(1, meshloc=ESMF.element)
nv = fvcom_esmfmesh.get_nv()

# fvcom_mesh can also be created by this way
# polys = []
# for i in range(nv.shape[0]):
#     vertices = [(lons[nv[i, j]], lats[nv[i, j]]) for j in range(nv.shape[1])]
#     poly = Polygon(vertices)
#     polys.append(poly)
# print(len(polys))
# fvcom_esmfmesh = xe.backend.Mesh.from_polygons(polys, element_coords='centroid')


node_locs = xr.Dataset()
node_locs["lon"] = xr.DataArray(data=node_lon, dims=("locations"))
node_locs["lat"] = xr.DataArray(data=node_lat, dims=("locations"))
fvcom_locstream = xe.backend.LocStream.from_xarray(node_lon, node_lat)

# Create the regridder
# regridder_ele = xe.frontend.BaseRegridder(hycom_esmfgrid, fvcom_esmfmesh, 'bilinear', extrap_method="nearest_s2d", filename='hycom2fvcomele_weight.nc')
regridder_ele = xe.frontend.BaseRegridder(hycom_esmfgrid, fvcom_esmfmesh, 'bilinear', extrap_method="nearest_s2d", weights='hycom2fvcomele_weight.nc')

# regridder_node = xe.frontend.BaseRegridder(hycom_esmfgrid, fvcom_locstream, "bilinear", extrap_method="inverse_dist", weights="hycom2fvcomnode_weight.nc", extrap_dist_exponent=10, extrap_num_src_pnts=6)
regridder_node = xe.Regridder(hycom_grid, node_locs, "bilinear", locstream_out=True, extrap_method="nearest_s2d", weights="hycom2fvcomnode_weight.nc")

# print(regridder_ele)


sst_out_ele = regridder_ele(hycomfile.sst, skipna=True)
sst_out_node = regridder_node(hycomfile.sst, skipna=True)

sst_out_ele = sst_out_ele.squeeze()
sst_out_node = sst_out_node.squeeze()

# fill nan using nearest neighbor interpolation
valid_idx = ~np.isnan(sst_out_node)
points = np.array([node_lat[valid_idx], node_lon[valid_idx]]).T
values = sst_out_node[valid_idx]
grid_points = np.array([node_lat, node_lon]).T
filled_sst = griddata(points, values, grid_points, method='nearest')

print("ele nan is ", np.isnan(sst_out_ele).sum())
print("node nan is ", np.isnan(sst_out_node).sum())
print("filled_sst nan is ", np.isnan(filled_sst).sum())
print("\nout_ele is ", sst_out_ele)
print("\nout_node is ", sst_out_node)

fig, ax = plt.subplots()
triangs=tri.Triangulation(node_lon,node_lat,nv)

# plt.scatter(np.array(node_lon), np.array(node_lat), c=np.array(filled_sst), s=0.2, cmap='viridis', linewidth=0)
# plt.tripcolor(node_lon, node_lat, nv, sst_out_node[0,:], cmap="viridis", linewidth=0.03)
plt.tripcolor(triangs,facecolors=sst_out_ele,cmap='viridis',zorder=2,linewidth=0.03)
plt.triplot(triangs,linewidth=0.03,zorder=4)
plt.colorbar()
plt.gca().set_aspect('equal', adjustable='box')
plt.savefig('sst_out.png', dpi=2000)
# Clean up
# regridder.clean_weight_file()

