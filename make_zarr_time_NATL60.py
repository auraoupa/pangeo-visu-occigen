import xarray as xr
import dask
import dask.threaded
import dask.multiprocessing
from dask.distributed import Client
import zarr     
import numpy as np
import os
 

compressor = zarr.Blosc(cname='zstd', clevel=3, shuffle=2)                                                


ds=xr.open_mfdataset('/store/albert7a/NATL60/NATL60-CJM165-S/1h/SSH/m07-09/NATL60-CJM165_y201?m??d??.1h_time.nc', parallel=True, concat_dim='time_counter',chunks={'time_counter':672,'y':120,'x':120})

encoding = {vname: {'compressor': compressor} for vname in ds.variables}                                  
ds.to_zarr(store='/store/albert7a/NATL60/NATL60-CJM165-S/1h/SSH/m07-09/zarr_NATL60_y2013m07-09', encoding=encoding)
