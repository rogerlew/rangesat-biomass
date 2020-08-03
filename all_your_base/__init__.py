from os.path import exists
from os.path import join as _join

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
    assert a[0] < a[2]
    assert a[1] < a[3]
    assert b[0] < b[2]
    assert b[1] < b[3]

    x1 = max(min(a[0], a[2]), min(b[0], b[2]))
    y1 = max(min(a[1], a[3]), min(b[1], b[3]))
    x2 = min(max(a[0], a[2]), max(b[0], b[2]))
    y2 = min(max(a[1], a[3]), max(b[1], b[3]))
    return x1 < x2 and y1 < y2


def isfloat(f):
    # noinspection PyBroadException
    try:
        float(f)
        return True
    except Exception:
        return False