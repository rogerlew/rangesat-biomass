import os
from os.path import exists as _exists
from os.path import join as _join
from os.path import split as _split

from glob import glob

import numpy as np
import rasterio


def aggregate_scns(scn_fns, dst_fn, agg_func=np.max, nodata=-9999.0):
    stack = []
    for scn_fn in scn_fns:
        ds = rasterio.open(scn_fn)
        stack.append(ds.read())

        print(scn_fn, stack[-1].shape)

    stack = np.concatenate(stack)
    data = agg_func(stack, axis=0)

    if isinstance(data, np.ma.core.MaskedArray):
        data.fill_value = nodata
        _data = data.filled()
    else:
        _data = data

    with rasterio.Env():
        profile = ds.profile
        profile.update(
            dtype=rasterio.float32,
            count=1,
            nodata=nodata,
            compress='lzw')

        with rasterio.open(dst_fn, 'w', **profile) as dst:
            dst.write(_data.astype(rasterio.float32), 1)


if __name__ == "__main__":
    start_year = 2015
    end_year = 2017
    for year in range(start_year, end_year+1):
        scns = glob('biomass_rasters/{year}/*.tif'.format(year=year))
        scns = [scn for scn in scns if '042028' in scn]

        aggregate_scns(scns, '{year}_max.tif'.format(year=year))
