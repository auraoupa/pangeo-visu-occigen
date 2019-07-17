import xarray as xr
import dask
import dask.threaded
import dask.multiprocessing
from dask.distributed import Client
import zarr
import numpy as np                                                     
import glob
import time

c = Client()
                                   

import BigBox as bb

for ibox in bb.bigbox:
   box = ibox

data_dir = '/scratch/cnt0024/hmg2840/albert7a/eNATL60/eNATL60-BLBT02-S/1h/UV/'

ufiles = sorted(glob.glob(data_dir + 'eNATL60-BLBT02_y*.1h_gridU.nc'))
vfiles = sorted(glob.glob(data_dir + 'eNATL60-BLBT02_y*.1h_gridV.nc'))

%time dsu1=xr.open_mfdataset(ufiles[0],parallel=True,concat_dim='time_counter') # 1 jour : 1.8s
%time dsu1=xr.open_mfdataset(ufiles[0],parallel=True,concat_dim='time_counter',decode_cf=False) # 1 jour : 34.3 ms

def non_time_coords(ds):
    return [v for v in ds.data_vars
            if 'time' not in ds[v].dims]

def drop_non_essential_vars_pop(ds):
    return ds.drop(non_time_coords(ds))   


%time dsu1=xr.open_mfdataset(ufiles[0],parallel=True,concat_dim='time_counter',decode_cf=False,preprocess=drop_non_essential_vars_pop) # 1 jour : 35.6ms


%time dsu10=xr.open_mfdataset(ufiles[0:10],parallel=True,concat_dim='time_counter') # 10 jours : 20s

%time dsu10=xr.open_mfdataset(ufiles[0:10],parallel=True,concat_dim='time_counter',decode_cf=False) # 10 jours : 410 ms



%time dsu1=xr.open_mfdataset(ufiles[0],parallel=True,concat_dim='time_counter',chunks={'x':100,'y':100,'time_counter':1}) # 1 jour : 19.1s

#with dask.config.set(get=c.get):

dsu=xr.open_mfdataset(ufiles,parallel=True,concat_dim='time_counter',chunks={'x':100,'y':100,'time_counter':1})


