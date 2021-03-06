import xarray as xr
import dask
import dask.threaded
import dask.multiprocessing
from dask.distributed import Client
import zarr     
import numpy as np
import os 

compressor = zarr.Blosc(cname='zstd', clevel=3, shuffle=2)                                                


#ds=xr.open_mfdataset('/store/albert7a/NATL60/NATL60-CJM165-S/1h/SSH/NATL60-CJM165_y2012m10d??.1h_SSH.nc', parallel=True, concat_dim='time_counter',chunks={'y':10,'x':10})

#encoding = {vname: {'compressor': compressor} for vname in ds.variables}                                  
#ds.to_zarr(store='/scratch/cnt0024/hmg2840/albert7a/NATL60/NATL60-CJM165-S/1h/SSH/NATL60-CJM165-SSH-1h-temp', encoding=encoding)
#zrtot=zarr.open('/scratch/cnt0024/hmg2840/albert7a/NATL60/NATL60-CJM165-S/1h/SSH/NATL60-CJM165-SSH-1h-temp',mode='a')

for m in np.arange(12,13):
	print('beginning month ',str(m))
	ds=xr.open_mfdataset('/store/albert7a/NATL60/NATL60-CJM165-S/1h/SSH/NATL60-CJM165_y2012m'+str(m)+'d??.1h_SSH.nc', parallel=True, concat_dim='time_counter',chunks={'y':10,'x':10})
	encoding = {vname: {'compressor': compressor} for vname in ds.variables}
	ds.to_zarr(store='/scratch/cnt0024/hmg2840/albert7a/NATL60/NATL60-CJM165-S/1h/SSH/zarr_SSH_2012m'+str(m)+'-temp', encoding=encoding)
#	zr=zarr.open('/scratch/cnt0024/hmg2840/albert7a/NATL60/NATL60-CJM165-S/1h/SSH/zarr_SSH_2012m'+str(m)+'-temp',mode='r')
#	for key in [k for k in zr.array_keys() if k not in ['nav_lat','nav_lon']]:
#		zrtot[key].append(zr[key])
	print('ending month ',str(m))

for m in np.arange(1,10):
	print('beginning month ',str(m))
	ds=xr.open_mfdataset('/store/albert7a/NATL60/NATL60-CJM165-S/1h/SSH/NATL60-CJM165_y2013m0'+str(m)+'d??.1h_SSH.nc', parallel=True, concat_dim='time_counter',chunks={'y':10,'x':10})
	encoding = {vname: {'compressor': compressor} for vname in ds.variables} 
	ds.to_zarr(store='/scratch/cnt0024/hmg2840/albert7a/NATL60/NATL60-CJM165-S/1h/SSH/zarr_SSH_2013m0'+str(m)+'-temp', encoding=encoding)
#	zr=zarr.open('/scratch/cnt0024/hmg2840/albert7a/NATL60/NATL60-CJM165-S/1h/SSH/zarr_SSH_2013m0'+str(m)+'-temp',mode='r')
#	for key in [k for k in zr.array_keys() if k not in ['nav_lat','nav_lon']]:
#		zrtot[key].append(zr[key])
	print('ending month ',str(m))



ds_z = xr.open_zarr('/scratch/cnt0024/hmg2840/albert7a/NATL60/NATL60-CJM165-S/1h/SSH/NATL60-CJM165-SSH-1h-temp')
ds_z
