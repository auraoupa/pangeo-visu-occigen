import xarray as xr
import dask
import dask.threaded
import dask.multiprocessing
from dask.distributed import Client
import zarr
import numpy as np                                                                                        

c = Client()

import os
 

compressor = zarr.Blosc(cname='zstd', clevel=3, shuffle=2)                                                

ds=xr.open_mfdataset('/mnt/meom/workdir/albert/eNATL60/eNATL60-I/mesh_zgr_eNATL60_3.6.nc',chunks={'z':1})
encoding = {vname: {'compressor': compressor} for vname in ds.variables}
ds.to_zarr(store='/mnt/meom/workdir/albert/eNATL60/eNATL60-I/zarr_mesh_zgr_eNATL60_3.6', encoding=encoding)

ds_z = xr.open_zarr('/mnt/meom/workdir/albert/eNATL60/eNATL60-I/zarr_mesh_zgr_eNATL60_3.6')
ds_z


