import os
import sys
import json

from os.path import join as _join
from os.path import split as _split
from os.path import exists as _exists

from subprocess import check_output, Popen

from glob import glob

sys.path.insert(0, '/var/www/rangesat-biomass')
from api.app import RANGESAT_DIRS, Location
from all_your_base import isfloat


def get_gdalinfo(fn):
    js = check_output(f'gdalinfo {fn} -json -stats', shell=True)
    return json.loads(js)


def rescale_ndvi(fn):
    fn_wgs_tif = fn[:-4] + '.wgs.tif'
    fn_wgs_vrt = fn[:-4] + '.wgs.vrt'

    if _exists(fn_wgs_tif):
        os.remove(fn_wgs_tif)

    if _exists(fn_wgs_vrt):
        os.remove(fn_wgs_vrt)

    cmd = ['gdalwarp', '-t_srs', 'EPSG:4326', '-of', 'vrt', fn, fn_wgs_vrt]
    p = Popen(cmd)
    p.wait()

    cmd = ['gdal_translate', '-co', 'COMPRESS=LZW', '-of', 'GTiff', '-scale', 0, 10000, -1, 1, '-ot', 'Float32',  fn_wgs_vrt, fn_wgs_tif]
    cmd = [str(x) for x in cmd]
    p = Popen(cmd)
    p.wait()
    assert _exists(fn_wgs_tif)

locations = [sys.argv[-1]]

for location in locations:
    _location = None
    for rangesat_dir in RANGESAT_DIRS:
        loc_path = _join(rangesat_dir, location)
        if _exists(loc_path):
            _location = Location(loc_path)
            break

    assert _location is not None

    fns = glob(_join(_location.out_dir, '*/*ndvi.tif'))
    #fns = ['/geodata/nas/rangesat/JISA/analyzed_rasters/LE07_L1TP_039030_20010709_20161001_01_T1/LE07_L1TP_039030_20010709_20161001_01_T1_sr_ndvi.tif',
    #       '/geodata/nas/rangesat/JISA/analyzed_rasters/LC08_L1TP_040030_20200712_20200722_01_T1/LC08_L1TP_040030_20200712_20200722_01_T1_ndvi.tif']
    
    for fn in fns:
        d = get_gdalinfo(fn)
        L = list(d['bands'][0]['metadata'].values())
        if len(L) == 0:
            continue

        v_max = float(L[0]['STATISTICS_MAXIMUM'])

        if v_max > 100.0:
            rescale_ndvi(fn)

