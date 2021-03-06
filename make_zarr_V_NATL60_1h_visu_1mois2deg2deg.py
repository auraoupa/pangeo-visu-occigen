import xarray as xr
import dask
import dask.threaded
import dask.multiprocessing
from dask.distributed import Client
import zarr     
import numpy as np
import os
 

compressor = zarr.Blosc(cname='zstd', clevel=3, shuffle=2)                                                


ds=xr.open_mfdataset('/store/albert7a/NATL60/NATL60-CJM165-S/1h/surf-U-V/NATL60-CJM165_y2012m10d??.1h_SSV.nc', parallel=True, concat_dim='time_counter',chunks={'time_counter':672,'y':120,'x':120})

encoding = {vname: {'compressor': compressor} for vname in ds.variables}                                  
ds.to_zarr(store='/scratch/cnt0024/hmg2840/albert7a/NATL60/NATL60-CJM165-S/1h/SSV/zarr/NATL60-CJM165-SSV-1h-1m2deg2deg', encoding=encoding)
zrtot=zarr.open('/scratch/cnt0024/hmg2840/albert7a/NATL60/NATL60-CJM165-S/1h/SSV/zarr/NATL60-CJM165-SSV-1h-1m2deg2deg',mode='a')

for m in np.arange(11,13):
        print('beginning month ',str(m))
	ds=xr.open_mfdataset('/store/albert7a/NATL60/NATL60-CJM165-S/1h/surf-U-V/NATL60-CJM165_y2012m'+str(m)+'d??.1h_SSV.nc', parallel=True, concat_dim='time_counter',chunks={'time_counter':672,'y':120,'x':120})
	encoding = {vname: {'compressor': compressor} for vname in ds.variables}                                  
	ds.to_zarr(store='/scratch/cnt0024/hmg2840/albert7a/NATL60/NATL60-CJM165-S/1h/SSV/zarr/zarr_SSV_2012m'+str(m)+'-1m2deg2deg', encoding=encoding)
        print('ending month ',str(m))

for m in np.arange(1,10):
        print('beginning month ',str(m))
        ds=xr.open_mfdataset('/store/albert7a/NATL60/NATL60-CJM165-S/1h/surf-U-V/NATL60-CJM165_y2013m0'+str(m)+'d??.1h_SSV.nc', parallel=True, concat_dim='time_counter',chunks={'time_counter':672,'y':120,'x':120})
	encoding = {vname: {'compressor': compressor} for vname in ds.variables}                                  
        ds.to_zarr(store='/scratch/cnt0024/hmg2840/albert7a/NATL60/NATL60-CJM165-S/1h/SSV/zarr/zarr_SSV_2013m0'+str(m)+'-1m2deg2deg', encoding=encoding)
        print('ending month ',str(m))

for m in np.arange(11,13):
        print('beginning month ',str(m))
	zr=zarr.open('/scratch/cnt0024/hmg2840/albert7a/NATL60/NATL60-CJM165-S/1h/SSV/zarr/zarr_SSV_2012m'+str(m)+'-1m2deg2deg',mode='r')
	for key in [k for k in zr.array_keys() if k not in ['nav_lat','nav_lon']]:
	        zrtot[key].append(zr[key])
        print('ending month ',str(m))

for m in np.arange(1,10):
        print('beginning month ',str(m))
        zr=zarr.open('/scratch/cnt0024/hmg2840/albert7a/NATL60/NATL60-CJM165-S/1h/SSV/zarr/zarr_SSV_2013m0'+str(m)+'-1m2deg2deg',mode='r')
        for key in [k for k in zr.array_keys() if k not in ['nav_lat','nav_lon']]:
                zrtot[key].append(zr[key])
        print('ending month ',str(m))

name='/scratch/cnt0024/hmg2840/albert7a/NATL60/NATL60-CJM165-S/1h/SSV/zarr/NATL60-CJM165-SSV-1h-1m2deg2deg'
for variable in 'time_counter' 'nav_lat' 'nav_lon':
	for dname in [f for f in os.listdir(name) if not f.startswith('.')]:
	     for fname in [f for f in os.listdir(f'{name}/{variable}') if not f.startswith('.')]:
	         os.remove(f'{name}/{variable}/{fname}')

#ds=xr.open_mfdataset('/store/albert7a/NATL60/NATL60-CJM165-S/1h/SSH/NATL60-CJM165_y2012m10d01-y2013m09d30.1h_time.nc', parallel=True, concat_dim='time_counter')
#encoding = {vname: {'compressor': compressor} for vname in ds.variables}
#ds.to_zarr(store='/scratch/cnt0024/hmg2840/albert7a/NATL60/NATL60-CJM165-S/1h/SSH/zarr_time', encoding=encoding)

!cp -r /scratch/cnt0024/hmg2840/albert7a/NATL60/NATL60-CJM165-S/1h/SSH/zarr_time/time_counter /scratch/cnt0024/hmg2840/albert7a/NATL60/NATL60-CJM165-S/1h/SSV/zarr/NATL60-CJM165-SSV-1h-1m2deg2deg/


ds_z = xr.open_zarr('/scratch/cnt0024/hmg2840/albert7a/NATL60/NATL60-CJM165-S/1h/SSV/zarr/NATL60-CJM165-SSV-1h-1m2deg2deg')
ds_z
