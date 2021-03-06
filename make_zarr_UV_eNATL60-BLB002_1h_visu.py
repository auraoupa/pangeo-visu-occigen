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

#for m in [3 ,5, 6, 7, 8 ,9, 10]:
for m in $(seq 5 10):
	print('beginning month ',str(m))
	mm=str(m).zfill(2)
	if m > 6:
		year=2009
	else:
		year=2010

	ds=xr.open_mfdataset('/scratch/cnt0024/hmg2840/albert7a/eNATL60/eNATL60-BLB002-S/1h/surf/eNATL60-BLB002_*'+str(year)+'m'+str(mm)+'*sozocrtx.nc', parallel=True, concat_dim='time_counter',chunks={'time_counter':672,'y':120,'x':120})

	encoding = {vname: {'compressor': compressor} for vname in ds.variables}
	ds.to_zarr(store='/scratch/cnt0024/hmg2840/albert7a/eNATL60/zarr/zarr_eNATL60-BLB002-SSU-1h-y'+str(year)+'m'+str(mm), encoding=encoding)
	print('ending month ',str(m))


#for m in [3, 5, 6, 7, 8, 9, 10]:
for m in $(seq 5 10):
	print('beginning month ',str(m))
        mm=str(m).zfill(2)
        if m > 6:
                year=2009
        else:
                year=2010

        ds=xr.open_mfdataset('/scratch/cnt0024/hmg2840/albert7a/eNATL60/eNATL60-BLB002-S/1h/surf/eNATL60-BLB002_*'+str(year)+'m'+str(mm)+'*somecrty.nc', parallel=True, concat_dim='time_counter',chunks={'time_counter':672,'y':120,'x':120})

        encoding = {vname: {'compressor': compressor} for vname in ds.variables}
        ds.to_zarr(store='/scratch/cnt0024/hmg2840/albert7a/eNATL60/zarr/zarr_eNATL60-BLB002-SSV-1h-y'+str(year)+'m'+str(mm), encoding=encoding)
	print('ending month ',str(m))



for m in np.arange(1,13):
        mm=str(m).zfill(2)
        if m > 6:
                year=2009
        else:
                year=2010

        ds=xr.open_mfdataset('/scratch/cnt0024/hmg2840/albert7a/eNATL60/eNATL60-BLB002-S/1h/surf/*'+str(year)+'m'+str(mm)+'*somecrty.nc', parallel=True, concat_dim='time_counter',chunks={'time_counter':672,'y':120,'x':120})

        encoding = {vname: {'compressor': compressor} for vname in ds.variables}
        ds.to_zarr(store='/scratch/cnt0024/hmg2840/albert7a/eNATL60/zarr/zarr_eNATL60-BLB002-SSV-1h-y'+str(year)+'m'+str(mm), encoding=encoding)

!cp -r /scratch/cnt0024/hmg2840/albert7a/eNATL60/zarr/zarr_eNATL60-BLB002-SSU-1h-y2009m07 /scratch/cnt0024/hmg2840/albert7a/eNATL60/zarr/eNATL60-BLB002-SSU-1h
!cp -r /scratch/cnt0024/hmg2840/albert7a/eNATL60/zarr/zarr_eNATL60-BLB002-SSV-1h-y2009m07 /scratch/cnt0024/hmg2840/albert7a/eNATL60/zarr/eNATL60-BLB002-SSV-1h

zrtot=zarr.open('/scratch/cnt0024/hmg2840/albert7a/eNATL60/zarr/eNATL60-BLB002-SSU-1h',mode='a')

for m in np.arange(9,13):
	print('beginning month ',str(m))
        mm=str(m).zfill(2)
	zr=zarr.open('/scratch/cnt0024/hmg2840/albert7a/eNATL60/zarr/zarr_eNATL60-BLB002-SSU-1h-y2009m'+str(mm),mode='r')
	for key in [k for k in zr.array_keys() if k not in ['nav_lat','nav_lon']]:
	        zrtot[key].append(zr[key])
	print('ending month ',str(m))


for m in np.arange(1,7):
	print('beginning month ',str(m))
        mm=str(m).zfill(2)
	zr=zarr.open('/scratch/cnt0024/hmg2840/albert7a/eNATL60/zarr/zarr_eNATL60-BLB002-SSU-1h-y2010m'+str(mm),mode='r')
	for key in [k for k in zr.array_keys() if k not in ['nav_lat','nav_lon']]:
	        zrtot[key].append(zr[key])
	print('ending month ',str(m))


zrtot=zarr.open('/scratch/cnt0024/hmg2840/albert7a/eNATL60/zarr/eNATL60-BLB002-SSU-1h',mode='a')
for m in np.arange(9,13):
	print('beginning month ',str(m))
        mm=str(m).zfill(2)
	zr=zarr.open('/scratch/cnt0024/hmg2840/albert7a/eNATL60/zarr/zarr_eNATL60-BLB002-SSU-1h-y2009m'+str(mm),mode='r')
#	zr=zarr.open('/store/albert7a/eNATL60/zarr/zarr_eNATL60-BLB002-SSU-1h-y2009m'+str(mm),mode='r')
	for key in [k for k in zr.array_keys() if k not in ['nav_lat','nav_lon']]:
	        zrtot[key].append(zr[key])
	print('ending month ',str(m))


for m in np.arange(1,7):
	print('beginning month ',str(m))
        mm=str(m).zfill(2)
	zr=zarr.open('/scratch/cnt0024/hmg2840/albert7a/eNATL60/zarr/zarr_eNATL60-BLB002-SSU-1h-y2010m'+str(mm),mode='r')
	for key in [k for k in zr.array_keys() if k not in ['nav_lat','nav_lon']]:
	        zrtot[key].append(zr[key])
	print('ending month ',str(m))

ds=xr.open_mfdataset('/scratch/cnt0024/hmg2840/albert7a/eNATL60/eNATL60-BLB002-S/1h/surf/eNATL60-BLB002_y2009m07d01-y2010m06d30.1h_time.nc', parallel=True, concat_dim='time_counter')

encoding = {vname: {'compressor': compressor} for vname in ds.variables}

ds.to_zarr(store='/scratch/cnt0024/hmg2840/albert7a/eNATL60/zarr/zarr_time', encoding=encoding)


name='/scratch/cnt0024/hmg2840/albert7a/eNATL60/zarr/eNATL60-BLB002-SSU-1h'
variable='time_counter'
for dname in [f for f in os.listdir(name) if not f.startswith('.')]:
     for fname in [f for f in os.listdir(f'{name}/{variable}') if not f.startswith('.')]:
         os.remove(f'{name}/{variable}/{fname}')

name='/scratch/cnt0024/hmg2840/albert7a/eNATL60/zarr/eNATL60-BLB002-SSV-1h'
variable='time_counter'
for dname in [f for f in os.listdir(name) if not f.startswith('.')]:
     for fname in [f for f in os.listdir(f'{name}/{variable}') if not f.startswith('.')]:
         os.remove(f'{name}/{variable}/{fname}')

!cp -r /scratch/cnt0024/hmg2840/albert7a/NATL60/NATL60-CJM165-S/1h/SSH/zarr_time/time_counter /scratch/cnt0024/hmg2840/albert7a/NATL60/NATL60-CJM165-S/1h/SSU/zarr/NATL60-CJM165-SSU-1h-1m2deg2deg/


ds_z = xr.open_zarr('/scratch/cnt0024/hmg2840/albert7a/eNATL60/zarr/eNATL60-BLB002-SSU-1h')
ds_z

ds_z = xr.open_zarr('/scratch/cnt0024/hmg2840/albert7a/eNATL60/zarr/eNATL60-BLB002-SSV-1h')
ds_z


