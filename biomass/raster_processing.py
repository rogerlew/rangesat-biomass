import os
from os.path import exists as _exists
from os.path import join as _join
from os.path import split as _split

from glob import glob

import numpy as np
import rasterio


def make_raster_difference(scn_fn1, scn_fn2, dst_fn, nodata=-9999.0):
    ds = rasterio.open(scn_fn1)
    ds2 = rasterio.open(scn_fn2)

    _data1 = ds.read()
    _data2 = ds2.read()
    assert _data1.shape == _data2.shape

    data = (_data2[0, :, :] - _data1[0, :, :]) / _data1[0, :, :]

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


def make_aggregated_rasters(scn_fns, dst_fn, agg_func=np.max, nodata=-9999.0):
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
    fn = '/Users/roger/rangesat-biomass/scripts/biomass_rasters/2015/LC08_L1TP_042028_20150510_20170301_01_T1.tif'
    fn2 = '/Users/roger/rangesat-biomass/scripts/biomass_rasters/2015/LE07_L1TP_042028_20150721_20160905_01_T1.tif'
    dst_fn = '/Users/roger/rangesat-biomass/scripts/biomass_rasters/2015-diff.tif'
    make_raster_difference(fn, fn2, dst_fn)