import dask
import dask.threaded
import dask.multiprocessing
from dask.distributed import Client

c = Client()

import xarray as xr
import zarr     
import numpy as np
import os
import glob 

compressor = zarr.Blosc(cname='zstd', clevel=3, shuffle=2)                                                

for m in np.arange(1,7):
	print('beginning month ',str(m))
	mm=str(m).zfill(2)
	if m > 6:
		year=2009
	else:
		year=2010

	if m in [1,3,5,7,8,10,12]:
		d2=31
	elif m in [4,6,9,11]:
		d2=30
	else:
		d2=28

	for day in np.arange(1,d2+1,1):
		print('beginning day',str(day))	
		if m in [7,8,9,10]:
			CASE='BLB002'
		elif m in [12,1,2,3,4,5,6]:
			CASE='BLB002X'
		elif m == 11: 
			if day < 7:
				CASE='BLB002'
			else:
				CASE='BLB002X'
		dd=str(day).zfill(2)

		datadir='/store/CT1/hmg2840/lbrodeau/eNATL60/eNATL60-'+str(CASE)+'-S/'
		files=sorted(glob.glob(datadir+'*/eNATL60-'+str(CASE)+'_1h_*_gridU-2D_'+str(year)+str(mm)+str(dd)+'-'+str(year)+str(mm)+str(dd)+'.nc'))
		u=xr.open_mfdataset(files[0],chunks={'time_counter':672,'y':120,'x':120})['sozocrtx']
		ds=u0.to_dataset(name='sozocrtx')
		encoding = {vname: {'compressor': compressor} for vname in ds.variables}
		ds.to_zarr(store='/store/albert7a/eNATL60/zarr/zarr_eNATL60-BLB002-SSU-1h-y'+str(year)+'m'+str(mm)+'d'+str(dd), encoding=encoding)
		print('ending day',str(day))	

	print('ending month ',str(m))

for m in np.arange(10,13):
	print('beginning month ',str(m))
        mm=str(m).zfill(2)
        if m > 6:
                year=2009
        else:
                year=2010

        ds=xr.open_mfdataset('/scratch/cnt0024/hmg2840/albert7a/eNATL60/eNATL60-BLB002-S/1h/surf/*'+str(year)+'m'+str(mm)+'*somecrty.nc', parallel=True, concat_dim='time_counter',chunks={'time_counter':672,'y':120,'x':120})

        encoding = {vname: {'compressor': compressor} for vname in ds.variables}
        ds.to_zarr(store='/scratch/cnt0024/hmg2840/albert7a/eNATL60/zarr/zarr_eNATL60-BLB002-SSV-1h-y'+str(year)+'m'+str(mm), encoding=encoding)
	print('ending month ',str(m))

!cp -r /scratch/cnt0024/hmg2840/albert7a/eNATL60/zarr/zarr_eNATL60-BLB002-SSV-1h-y2009m07 /scratch/cnt0024/hmg2840/albert7a/eNATL60/zarr/eNATL60-BLB002-SSV-1h
!cp -r /scratch/cnt0024/hmg2840/albert7a/eNATL60/zarr/zarr_eNATL60-BLB002-SSU-1h-y2009m07 /scratch/cnt0024/hmg2840/albert7a/eNATL60/zarr/eNATL60-BLB002-SSU-1h

#check if zarr ok
for m in np.arange(10,13):
        print('beginning month ',str(m))
        mm=str(m).zfill(2)
        if m > 6:
                year=2009
        else:
                year=2010
	ds_z = xr.open_zarr('/scratch/cnt0024/hmg2840/albert7a/eNATL60/zarr/zarr_eNATL60-BLB002-SSV-1h-y'+str(year)+'m'+str(mm))
	ds_z
#	u=ds_z['somecrty']
#	check_NaN = (1*np.isnan(u.values)).sum()
#	print('zarr_eNATL60-BLB002-SSV-1h-y'+str(year)+'m'+str(mm)' : '+str(check_NaN))

zrtot=zarr.open('/store/albert7a/eNATL60/zarr/eNATL60-BLB002-SSU-1h',mode='a')

for m in np.arange(8,13):
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


zrtot=zarr.open('/scratch/cnt0024/hmg2840/albert7a/eNATL60/zarr/eNATL60-BLBT02-SSU-1h',mode='a')
for m in np.arange(9,13):
	print('beginning month ',str(m))
        mm=str(m).zfill(2)
	zr=zarr.open('/scratch/cnt0024/hmg2840/albert7a/eNATL60/zarr/zarr_eNATL60-BLBT02-SSU-1h-y2009m'+str(mm),mode='r')
#	zr=zarr.open('/store/albert7a/eNATL60/zarr/zarr_eNATL60-BLBT02-SSU-1h-y2009m'+str(mm),mode='r')
	for key in [k for k in zr.array_keys() if k not in ['nav_lat','nav_lon']]:
	        zrtot[key].append(zr[key])
	print('ending month ',str(m))


for m in np.arange(1,7):
	print('beginning month ',str(m))
        mm=str(m).zfill(2)
	zr=zarr.open('/scratch/cnt0024/hmg2840/albert7a/eNATL60/zarr/zarr_eNATL60-BLBT02-SSU-1h-y2010m'+str(mm),mode='r')
	for key in [k for k in zr.array_keys() if k not in ['nav_lat','nav_lon']]:
	        zrtot[key].append(zr[key])
	print('ending month ',str(m))

ds=xr.open_mfdataset('/scratch/cnt0024/hmg2840/albert7a/eNATL60/eNATL60-BLBT02-S/1h/surf/eNATL60-BLBT02_y2009m07d01-y2010m06d30.1h_time.nc', parallel=True, concat_dim='time_counter')

encoding = {vname: {'compressor': compressor} for vname in ds.variables}

ds.to_zarr(store='/scratch/cnt0024/hmg2840/albert7a/eNATL60/zarr/zarr_time', encoding=encoding)


name='/scratch/cnt0024/hmg2840/albert7a/eNATL60/zarr/eNATL60-BLBT02-SSU-1h'
variable='time_counter'
for dname in [f for f in os.listdir(name) if not f.startswith('.')]:
     for fname in [f for f in os.listdir(f'{name}/{variable}') if not f.startswith('.')]:
         os.remove(f'{name}/{variable}/{fname}')

name='/store/albert7a/eNATL60/zarr/eNATL60-BLB002-SSV-1h'
variable='time_counter'
for dname in [f for f in os.listdir(name) if not f.startswith('.')]:
     for fname in [f for f in os.listdir(f'{name}/{variable}') if not f.startswith('.')]:
         os.remove(f'{name}/{variable}/{fname}')

!cp -r /scratch/cnt0024/hmg2840/albert7a/eNATL60/zarr/zarr_time/time_counter /store/albert7a/eNATL60/zarr/eNATL60-BLB002-SSV-1h


ds_z = xr.open_zarr('/store/albert7a/eNATL60/zarr/eNATL60-BLB002-SSV-1h')
ds_z

ds_z = xr.open_zarr('/scratch/cnt0024/hmg2840/albert7a/eNATL60/zarr/eNATL60-BLBT02-SSV-1h')
ds_z


