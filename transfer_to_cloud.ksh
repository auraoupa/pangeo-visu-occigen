gsutil -m cp -r /mnt/alberta/equipes/IGE/meom/workdir/albert/NATL60/NATL60-CJM165-S/zarr/NATL60-CJM165-SSH-1h-2D/time_counter gs://pangeo-data/NATL60-CJM165-SSH-1h-2D/

gsutil -m rm -rf gs://pangeo-data/NATL60-CJM165-SSH-1h-2D/time_counter

scp -r albert7a@occigen.cines.fr:/scratch/cnt0024/hmg2840/albert7a/NATL60/NATL60-CJM165-S/1h/SSH/NATL60-CJM165-SSH-1h-2D/time_counter /mnt/alberta/equipes/IGE/meom/workdir/albert/NATL60/NATL60-CJM165-S/zarr/NATL60-CJM165-SSH-1h-2D/

rm -rf /mnt/alberta/equipes/IGE/meom/workdir/albert/NATL60/NATL60-CJM165-S/zarr/NATL60-CJM165-SSH-1h-2D/time_counter

gsutil -m cp -r /mnt/alberta/equipes/IGE/meom/workdir/albert/NATL60/NATL60-CJM165-S/zarr/NATL60-CJM165-SSH-1h-2D gs://pangeo-data/


scp -r albert7a@occigen.cines.fr:/scratch/cnt0024/hmg2840/albert7a/NATL60/NATL60-CJM165-S/1h/SSH/NATL60-CJM165-SSH-1h-1m2deg2deg /mnt/alberta/equipes/IGE/meom/workdir/albert/NATL60/NATL60-CJM165-S/zarr/

gsutil -m cp -r /mnt/alberta/equipes/IGE/meom/workdir/albert/NATL60/NATL60-CJM165-S/zarr/NATL60-CJM165-SSH-1h-1m2deg2deg gs://pangeo-data/

gsutil -m cp -r /mnt/alberta/equipes/IGE/meom/workdir/albert/NATL60/NATL60-CJM165-S/1h/SSU/zarr/NATL60-CJM165-SSU-1h-1m2deg2deg gs://pangeo-data/

gsutil -m cp -r /mnt/alberta/equipes/IGE/meom/workdir/albert/NATL60/NATL60-CJM165-S/1h/SSV/zarr/NATL60-CJM165-SSV-1h-1m2deg2deg gs://pangeo-data/
