from glob import glob
import os
from os.path import join as _join
from os.path import exists as _exists
import sys
from subprocess import Popen

sys.path.append(os.path.abspath('../../'))
sys.path.insert(0, '/Users/roger/rangesat-biomass')

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

scenes = glob('/geodata/nas/rangesat/Zumwalt2/analyzed_rasters/L*')

print(len(scenes))

for scn in scenes:
    if not os.path.isdir(scn):
        continue

    if len(glob(_join(scn, '*ndvi.wgs.tif'))) > 0:
        continue

    print(scn)

    ls = LandSatScene(scn)
    ndvi_fn = _join(scn, '%s_ndvi.tif' % ls.product_id)
    ls.dump(ls.ndvi, ndvi_fn)
    reproject_raster(ndvi_fn)

