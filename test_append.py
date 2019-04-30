import xarray as xr
import dask
import dask.threaded
import dask.multiprocessing
from dask.distributed import Client
import zarr     
import numpy as np
import os

def empty_zarr(name, variable=None):
   '''Empty the Zarr archive of its data (but not its metadata).
   Arguments:
       - name: the name of the archive.
       - variable: the name of the variable to empty (if None, empty all
         variables)
   '''
   for dname in [f for f in os.listdir(name) if not f.startswith('.')]:
       if variable is not None and dname == variable:
           for fname in [f for f in os.listdir(f'{name}/{dname}')
                         if not f.startswith('.')]:
               os.remove(f'{name}/{dname}/{fname}')

def append_zarr(src_name, dst_name):
   '''Append a Zarr archive to another one.
   Arguments:
       - src_name: the name of the archive to append.
       - dst_name: the name of the archive to be appended to.
   '''
   zarr_src = zarr.open(src_name, mode='r')
   zarr_dst = zarr.open(dst_name, mode='a')
   for key in [k for k in zarr_src.array_keys() if k not in ['nav_lat', 'nav_lon']]:
       zarr_dst[key].append(zarr_src[key])

ds=xr.open_mfdataset('/store/albert7a/NATL60/NATL60-CJM165-S/1h/SSH/NATL60-CJM165_y2012m10d??.1h_SSH.nc', parallel=True, concat_dim='time_counter',chunks={'time_counter':1})

compressor = zarr.Blosc(cname='zstd', clevel=3, shuffle=2)                                               
encoding = {vname: {'compressor': compressor} for vname in ds.variables}                                  
ds.to_zarr(store='/scratch/cnt0024/hmg2840/albert7a/NATL60/NATL60-CJM165-S/1h/SSH/test_zarr_first_month', encoding=encoding)

m=11
ds=xr.open_mfdataset('/store/albert7a/NATL60/NATL60-CJM165-S/1h/SSH/NATL60-CJM165_y2012m'+str(m)+'d??.1h_SSH.nc', parallel=True, concat_dim='time_counter',chunks={'time_counter':1})
ds.to_zarr(store='/scratch/cnt0024/hmg2840/albert7a/NATL60/NATL60-CJM165-S/1h/SSH/test_zarr_second_month', encoding=encoding)


empty_zarr('/scratch/cnt0024/hmg2840/albert7a/NATL60/NATL60-CJM165-S/1h/SSH/test_zarr_first_month')
append_zarr('/scratch/cnt0024/hmg2840/albert7a/NATL60/NATL60-CJM165-S/1h/SSH/test_zarr_second_month','/scratch/cnt0024/hmg2840/albert7a/NATL60/NATL60-CJM165-S/1h/SSH/test_zarr_first_month')

!mkdir -p /scratch/cnt0024/hmg2840/albert7a/NATL60/NATL60-CJM165-S/1h/SSH/test_zarr_append
!cp -r /scratch/cnt0024/hmg2840/albert7a/NATL60/NATL60-CJM165-S/1h/SSH/test_zarr_first_month/* /scratch/cnt0024/hmg2840/albert7a/NATL60/NATL60-CJM165-S/1h/SSH/test_zarr_append/
!cp -r /scratch/cnt0024/hmg2840/albert7a/NATL60/NATL60-CJM165-S/1h/SSH/test_zarr_first_month/.[^.]* /scratch/cnt0024/hmg2840/albert7a/NATL60/NATL60-CJM165-S/1h/SSH/test_zarr_append/

ds_z=xr.open_zarr('/scratch/cnt0024/hmg2840/albert7a/NATL60/NATL60-CJM165-S/1h/SSH/test_zarr_append')
ds_z
