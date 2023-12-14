from os.path import split as _split
from os.path import join as _join
from os.path import exists as _exists

from glob import glob

import sys
import os

from subprocess import Popen

import numpy as np


sys.path.append('/var/www/rangesat-biomass/')

from biomass.landsat import LandSatScene


def reproject_raster(src):
    dst = src[:-4] + '.wgs.vrt'
    dst2 = src[:-4] + '.wgs.tif'

    if _exists(dst):
        os.remove(dst)
    if _exists(dst2):
        os.remove(dst2)

    cmd = ['gdalwarp', '-t_srs', 'EPSG:4326', '-of', 'vrt', src, dst]
    p = Popen(cmd)
    p.wait()

    cmd = ['gdal_translate', '-co', 'COMPRESS=LZW', '-of', 'GTiff', dst, dst2]
    p = Popen(cmd)
    p.wait()
    assert _exists(dst)



def make_sr_ndvi(scene):
    ls = LandSatScene(scene)
    ndvi = ls.ndvi
    ndvi = np.array(ndvi * 10000, dtype=np.int16)

    ndvi_fn = _join(scene, f'{ls.product_id}_sr_ndvi.tif')
    ls.dump(ndvi, ndvi_fn, dtype=np.int16)

    reproject_raster(ndvi_fn)
    

if __name__ == "__main__":
    scenes = glob('/geodata/nas/rangesat/Zumwalt4/analyzed_rasters/L*/')

    for scene in scenes:
        sr_ndvi = glob(_join(scene, '*sr_ndvi*.tif'))
        if len(sr_ndvi) == 0:
            print('scene:', scene)
            make_sr_ndvi(scene)
