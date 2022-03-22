from os.path import exists
from os.path import join as _join

from subprocess import check_output
import json

import fiona
import pyproj

from osgeo import osr
import numpy as np


GEODATA_DIRS = []
if exists('/geodata'):
    GEODATA_DIRS.append('/geodata')
if exists('/Volumes/Space/geodata'):
    GEODATA_DIRS.append('/Volumes/Space/geodata')
if exists('/space'):
    GEODATA_DIRS.append('/space')
assert len(GEODATA_DIRS) > 0

RANGESAT_DIRS = []
for geo_dir in GEODATA_DIRS:
    if exists(_join(geo_dir, 'rangesat')):
        RANGESAT_DIRS.append(_join(geo_dir, 'rangesat'))
if exists('/geodata/nas/rangesat'):
    RANGESAT_DIRS.append('/geodata/nas/rangesat')
assert len(RANGESAT_DIRS) > 0


SCRATCH = '/media/ramdisk'

if not exists(SCRATCH):
    SCRATCH = '/Users/roger/Downloads'


def wkt_2_proj4(wkt):
    srs = osr.SpatialReference()
    srs.ImportFromWkt(wkt)
    return srs.ExportToProj4().strip()


wgs84_proj4 = '+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs'


def rat_extract(fn):
    """extract raster attribute table from geotiff"""

    js = check_output('gdalinfo -json ' + fn, shell=True)
    rat = json.loads(js.decode())['rat']

    field_defs = rat['fieldDefn']

    d = {}
    for row in rat['row']:
        row = row['f']
        px_value = row[0]
        row = {fd['name']: v for fd, v in zip(field_defs, row)}
        d[px_value] = row

    return d


def get_sf_wgs_bounds(_sf_fn):
    """
    returns the bbox of the features in a fiona.shapefile
    """
    _sf = fiona.open(_sf_fn, 'r')
    bboxs = []
    for feature in _sf:
        bboxs.append(fiona.bounds(feature))

    bboxs = np.array(bboxs)
    e, s, w, n = [np.min(bboxs[:, 0]), np.min(bboxs[:, 1]),
                  np.max(bboxs[:, 2]), np.max(bboxs[:, 3])]

    proj_wkt = open(_sf_fn.replace('.shp', '') + '.prj').read()
    sf_proj4 = wkt_2_proj4(proj_wkt)
    sf_proj = pyproj.Proj(sf_proj4)
    wgs_proj = pyproj.Proj(wgs84_proj4)
    e, s = pyproj.transform(sf_proj, wgs_proj, e, s)
    w, n = pyproj.transform(sf_proj, wgs_proj, w, n)

    return e, s, w, n


def bounds_intersect(a, b):
     l_a, b_a, r_a, t_a = a
     assert l_a < r_a
     assert b_a < t_a

     l_b, b_b, r_b, t_b = b
     assert l_b < r_b
     assert b_a < t_a

     print(max(l_a, l_b), min(r_a, r_b), max(b_a, b_b), min(t_a, t_b))
     return max(l_a, l_b) < min(r_a, r_b) and max(b_a, b_b) < min(t_a, t_b)


def bounds_contain(a, b):
     """
     returns true if a is contained within b
     """
     l_a, b_a, r_a, t_a = a
     assert l_a < r_a
     assert b_a < t_a

     l_b, b_b, r_b, t_b = b
     assert l_b < r_b
     assert b_a < t_a

     return l_a >= l_b and b_a >= b_b and r_a <= r_b and t_a <= t_b

def isfloat(f):
    # noinspection PyBroadException
    try:
        float(f)
        return True
    except Exception:
        return False
