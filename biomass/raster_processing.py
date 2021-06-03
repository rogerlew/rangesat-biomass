import os
from os.path import exists as _exists
from os.path import join as _join
from os.path import split as _split

from glob import glob

from subprocess import Popen

import numpy as np
import rasterio
from rasterio.mask import raster_geometry_mask
import warnings

from osgeo import gdal


def transform_to_template_ds(template_fn, src_fn, dst_fn):
    ds = gdal.Open(template_fn)

    transform = ds.GetGeoTransform()
    ul_x, ul_y = transform[0], transform[3]
    xres, yres = transform[1], transform[5]
    wkt = ds.GetProjection()
    x, y = ds.RasterXSize, ds.RasterYSize
    lr_x, lr_y = ul_x + x * xres, ul_y + y * yres

    xmin, ymin, xmax, ymax = ul_x, lr_y, lr_x, ul_y

    assert xmax > xmin
    assert ymax > ymin

    if _exists(dst_fn):
        os.remove(dst_fn)

    cmd = ['gdalwarp', '-ts', x, y, '-te', xmin, ymin, xmax, ymax, '-t_srs', wkt, src_fn, dst_fn]
    cmd = [str(v) for v in cmd]
    p = Popen(cmd)
    p.wait()

    assert _exists(dst_fn)


def make_raster_difference(scn_fn1, scn_fn2, dst_fn, nodata=-9999.0):
    ds = rasterio.open(scn_fn1)
    ds2 = rasterio.open(scn_fn2)

    with open(dst_fn + '.log', 'w') as fp:
        fp.write('scn_fn1 = ' + scn_fn1)
        fp.write('scn_fn2 = ' + scn_fn2)

    _data1 = np.ma.masked_values(ds.read(), nodata)
    _data1 = np.ma.masked_values(_data1, 0)
    _data1 = np.ma.masked_values(_data1, 1)

    _data2 = np.ma.masked_values(ds2.read(), nodata)
    _data2 = np.ma.masked_values(_data2, 0)
    _data2 = np.ma.masked_values(_data2, 1)

    if not _data1.shape == _data2.shape:
        transformed_scn_fn2 = scn_fn2[:-4] + '.x.tif'
        transform_to_template_ds(scn_fn1, scn_fn2, transformed_scn_fn2)
        return make_raster_difference(scn_fn1, transformed_scn_fn2, dst_fn, nodata)

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


def calc_by_pastures(raster_fn, location, agg_func, ranches=None, nodata=-9999.0, verbose=False, value_scalar=1.0):
    """
    Iterate over each pasture and determine the biomass, etc. for each model

    :param sf:
    :return:
    """

    ds = rasterio.open(raster_fn)
    raster_data = np.ma.masked_values(ds.read(), nodata)

    data = []
    for ranch in location.ranches:
        if ranches is None or ranch in ranches:
            for pasture in location.pastures[ranch]:
                indx, indy = location.get_pasture_indx(raster_fn=raster_fn, pasture=pasture, ranch=ranch)
                if verbose:
                    print(pasture, ranch)
                    print(indx)
                value = float(agg_func(raster_data[:, indx, indy]))
                try:
                    value *= value_scalar
                except:
                    pass

                data.append(dict(ranch=ranch, pasture=pasture, value=value))

    return data


if __name__ == "__main__":
    fn = '/Users/roger/rangesat-biomass/scripts/biomass_rasters/2015/LC08_L1TP_042028_20150510_20170301_01_T1.tif'
    fn2 = '/Users/roger/rangesat-biomass/scripts/biomass_rasters/2015/LE07_L1TP_042028_20150721_20160905_01_T1.tif'
    dst_fn = '/Users/roger/rangesat-biomass/scripts/biomass_rasters/2015-diff.tif'
    make_raster_difference(fn, fn2, dst_fn)
