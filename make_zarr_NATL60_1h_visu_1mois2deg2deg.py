import xarray as xr
import dask
import dask.threaded
import dask.multiprocessing
from dask.distributed import Client
import zarr     
import numpy as np                                                                                          



ds=xr.open_mfdataset('/store/albert7a/NATL60/NATL60-CJM165-S/1h/SSH/NATL60-CJM165_y2012m10d??.1h_SSH.nc', parallel=True, concat_dim='time_counter',chunks={'time_counter':672,'y':120,'x':120})

compressor = zarr.Blosc(cname='zstd', clevel=3, shuffle=2)                                                
encoding = {vname: {'compressor': compressor} for vname in ds.variables}                                  
ds.to_zarr(store='/scratch/cnt0024/hmg2840/albert7a/NATL60/NATL60-CJM165-S/1h/SSH/NATL60-CJM165-SSH-1h-1m2deg2deg', encoding=encoding)
zrtot=zarr.open('/scratch/cnt0024/hmg2840/albert7a/NATL60/NATL60-CJM165-S/1h/SSH/NATL60-CJM165-SSH-1h-1m2deg2deg',mode='a')

for m in np.arange(11,13):
	ds=xr.open_mfdataset('/store/albert7a/NATL60/NATL60-CJM165-S/1h/SSH/NATL60-CJM165_y2012m'+str(m)+'d??.1h_SSH.nc', parallel=True, concat_dim='time_counter',chunks={'time_counter':672,'y':120,'x':120})
	ds.to_zarr(store='/scratch/cnt0024/hmg2840/albert7a/NATL60/NATL60-CJM165-S/1h/SSH/zarr_SSH_2012m'+str(m)+'-1m2deg2deg', encoding=encoding)
	zr=zarr.open('/scratch/cnt0024/hmg2840/albert7a/NATL60/NATL60-CJM165-S/1h/SSH/zarr_SSH_2012m'+str(m)+'-1m2deg2deg',mode='r')
	for key in [k for k in zr.array_keys() if k not in ['nav_lat','nav_lon']]:
	        zrtot[key].append(zr[key])

for m in np.arange(1,10):
        ds=xr.open_mfdataset('/store/albert7a/NATL60/NATL60-CJM165-S/1h/SSH/NATL60-CJM165_y2013m0'+str(m)+'d??.1h_SSH.nc', parallel=True, concat_dim='time_counter',chunks={'time_counter':672,'y':120,'x':120})
        ds.to_zarr(store='/scratch/cnt0024/hmg2840/albert7a/NATL60/NATL60-CJM165-S/1h/SSH/zarr_SSH_2013m0'+str(m)+'-1m2deg2deg', encoding=encoding)
        zr=zarr.open('/scratch/cnt0024/hmg2840/albert7a/NATL60/NATL60-CJM165-S/1h/SSH/zarr_SSH_2013m0'+str(m)+'-1m2deg2deg',mode='r')
        for key in [k for k in zr.array_keys() if k not in ['nav_lat','nav_lon']]:
                zrtot[key].append(zr[key])



ds_z = xr.open_zarr('/scratch/cnt0024/hmg2840/albert7a/NATL60/NATL60-CJM165-S/1h/SSH/NATL60-CJM165-SSH-1h')
ds_z