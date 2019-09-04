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

m=3
print('beginning month ',str(m))
mm=str(m).zfill(2)
if m > 6:
	year=2009
else:
	year=2010
ds=xr.open_mfdataset('/scratch/cnt0024/hmg2840/albert7a/eNATL60/eNATL60-BLB002-S/1h/surf/*'+str(year)+'m'+str(mm)+'*sozocrtx.nc', parallel=True, concat_dim='time_counter',chunks={'time_counter':672,'y':120,'x':120})

encoding = {vname: {'compressor': compressor} for vname in ds.variables}
ds.to_zarr(store='/scratch/cnt0024/hmg2840/albert7a/eNATL60/zarr/zarr_eNATL60-BLB002-SSU-1h-y'+str(year)+'m'+str(mm), encoding=encoding)
print('ending month ',str(m))

ds_z = xr.open_zarr('/scratch/cnt0024/hmg2840/albert7a/eNATL60/zarr/eNATL60-BLBT02-SSV-1h')
ds_z


