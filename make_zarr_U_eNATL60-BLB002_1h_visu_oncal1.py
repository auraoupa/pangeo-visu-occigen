import xarray as xr
import dask
import dask.threaded
import dask.multiprocessing
from dask.distributed import Client
import zarr
import numpy as np                                                                                        

c = Client()

import xarray as xr
import dask
import dask.threaded
import dask.multiprocessing
from dask.distributed import Client
import zarr     
import numpy as np
import os
 

compressor = zarr.Blosc(cname='zstd', clevel=3, shuffle=2)                                                

for m in np.arange(7,8):
	print('beginning month ',str(m))
	mm=str(m).zfill(2)
	if m > 6:
		year=2009
	else:
		year=2010

	ds=xr.open_mfdataset('/mnt/meom/workdir/albert/eNATL60/eNATL60-BLB002-S/1h/surf/*'+str(year)+'m'+str(mm)+'*sozocrtx.nc', parallel=True, concat_dim='time_counter',chunks={'time_counter':672,'y':120,'x':120})

	encoding = {vname: {'compressor': compressor} for vname in ds.variables}
	ds.to_zarr(store='/mnt/meom/workdir/albert/eNATL60/zarr/zarr_eNATL60-BLB002-SSU-1h-y'+str(year)+'m'+str(mm), encoding=encoding)
	print('ending month ',str(m))


zrtot=zarr.open('/mnt/meom/workdir/albert/eNATL60/zarr/eNATL60-BLB002-SSU-1h',mode='a')

for m in np.arange(8,13):
	print('beginning month ',str(m))
        mm=str(m).zfill(2)
	zr=zarr.open('/mnt/meom/workdir/albert/eNATL60/zarr/zarr_eNATL60-BLB002-SSU-1h-y2009m'+str(mm),mode='r')
	for key in [k for k in zr.array_keys() if k not in ['nav_lat','nav_lon']]:
	        zrtot[key].append(zr[key])
	print('ending month ',str(m))


for m in np.arange(1,7):
	print('beginning month ',str(m))
        mm=str(m).zfill(2)
	zr=zarr.open('/mnt/meom/workdir/albert/eNATL60/zarr/zarr_eNATL60-BLB002-SSU-1h-y2010m'+str(mm),mode='r')
	for key in [k for k in zr.array_keys() if k not in ['nav_lat','nav_lon']]:
	        zrtot[key].append(zr[key])
	print('ending month ',str(m))


