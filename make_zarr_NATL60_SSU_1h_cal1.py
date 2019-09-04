import xarray as xr
import dask
import dask.threaded
import dask.multiprocessing
from dask.distributed import Client
import zarr     
import numpy as np
import os
 

compressor = zarr.Blosc(cname='zstd', clevel=3, shuffle=2)                                                

for m in np.arange(7,10):
	mm=str(m).zfill(2)
	ds=xr.open_mfdataset('/mnt/meom/workdir/albert/NATL60/NATL60-CJM165-S/1h/SSU/NATL60-CJM165_y2013m'+str(mm)+'d??.1h_SSU.nc', parallel=True, concat_dim='time_counter',chunks={'time_counter':672,'y':120,'x':120})

	encoding = {vname: {'compressor': compressor} for vname in ds.variables}
	ds.to_zarr(store='/mnt/meom/workdir/albert/NATL60/NATL60-CJM165-S/1h/SSU/zarr/zarr_NATL60-CJM165_SSU_1h_y2013m'+str(mm), encoding=encoding)

!cp -r /mnt/meom/workdir/albert/NATL60/NATL60-CJM165-S/1h/SSU/zarr/zarr_NATL60-CJM165_SSU_1h_y2013m07 /mnt/meom/workdir/albert/NATL60/NATL60-CJM165-S/1h/SSU/zarr/zarr_NATL60-CJM165_SSU_1h_y2013m07-09

zrtot=zarr.open('/mnt/meom/workdir/albert/NATL60/NATL60-CJM165-S/1h/SSU/zarr/zarr_NATL60-CJM165_SSU_1h_y2013m07-09',mode='a')

for m in np.arange(8,10):
	print('beginning month ',str(m))
        mm=str(m).zfill(2)
	zr=zarr.open('/mnt/meom/workdir/albert/NATL60/NATL60-CJM165-S/1h/SSU/zarr/zarr_NATL60-CJM165_SSU_1h_y2013m'+str(mm),mode='r')
	for key in [k for k in zr.array_keys() if k not in ['nav_lat','nav_lon']]:
	        zrtot[key].append(zr[key])
	print('ending month ',str(m))


for m in np.arange(4,7):
	print('beginning month ',str(m))
        mm=str(m).zfill(2)
	zr=zarr.open('/mnt/meom/workdir/albert/eNATL60/zarr_eNATL60-BLBT02-SSV-1h-y2010m'+str(mm),mode='r')
	for key in [k for k in zr.array_keys() if k not in ['nav_lat','nav_lon']]:
	        zrtot[key].append(zr[key])
	print('ending month ',str(m))



name='/mnt/meom/workdir/albert/eNATL60/eNATL60-BLBT02-SSV-1h'
variable='time_counter'
for dname in [f for f in os.listdir(name) if not f.startswith('.')]:
     for fname in [f for f in os.listdir(f'{name}/{variable}') if not f.startswith('.')]:
         os.remove(f'{name}/{variable}/{fname}')

!cp -r /scratch/cnt0024/hmg2840/albert7a/NATL60/NATL60-CJM165-S/1h/SSH/zarr_time/time_counter /scratch/cnt0024/hmg2840/albert7a/NATL60/NATL60-CJM165-S/1h/SSU/zarr/NATL60-CJM165-SSU-1h-1m2deg2deg/


ds_z = xr.open_zarr('/mnt/meom/workdir/albert/eNATL60/eNATL60-BLBT02-SSU-1h')
ds_z

ds_z = xr.open_zarr('/mnt/meom/workdir/albert/eNATL60/eNATL60-BLBT02-SSV-1h')
ds_z


