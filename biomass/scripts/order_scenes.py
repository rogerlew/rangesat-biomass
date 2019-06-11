
import os
import sys
import json

from os.path import split as _split
from os.path import join as _join

from datetime import date
from glob import glob


from datetime import datetime
from lsru import Espa, Usgs

import fiona
import pyproj

from osgeo import osr
import numpy as np


_thisdir = os.path.dirname(__file__)


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


def place_order(bbox,
            t0: datetime,
            tend: datetime,
            max_cloud_cover=100,
            landsat_num=8,
            verbose=True):
    """


        collections = {4: 'LANDSAT_TM_C1',
                       5: 'LANDSAT_TM_C1',
                       7: 'LANDSAT_ETM_C1',
                       8: 'LANDSAT_8_C1'}

    :param bbox: left, bottom, right, top
    :param t0: datetime object
    :param tend: datetime object
    :return:
    """
    global catalog

    assert landsat_num in [4, 5, 7, 8]

    if landsat_num == 4:
        collection = 'LANDSAT_TM_C1'
    elif landsat_num == 5:
        collection = 'LANDSAT_TM_C1'
    elif landsat_num == 7:
        collection = 'LANDSAT_ETM_C1'
    else:
        collection = 'LANDSAT_8_C1'

    # Instantiate Usgs class and login
    usgs = Usgs(conf='.lsru')
    success = usgs.login()

    if success:
        if verbose:
            print('Logged in to usgs espa')
    else:
        if verbose:
            print('ERROR: Failed to login to usgs espa')
        return

    # Instantiate Espa class
    espa = Espa(conf='.lsru')

    # Query the Usgs api to find scene intersecting with the spatio-temporal window
    scene_list = usgs.search(collection=collection,
                             bbox=bbox,
                             begin=t0,
                             end=tend,
                             max_cloud_cover=max_cloud_cover)

    if len(scene_list) == 0:
        return

    # Extract Landsat scene ids for each hit from the metadata
    _scene_list = [x['displayId'] for x in scene_list]

    if verbose:
        print(scene_list)

    scene_list = []
    for scene in _scene_list:

        if not scene.endswith('T1'):
            continue

        satellite = int(scene[2:4])
        _scene = scene.split('_')
        wrs_path = int(_scene[2][:3])
        wrs_row = int(_scene[2][3:])

        _date = _scene[3]
        year, month, day = int(_date[:4]), int(_date[4:6]), int(_date[6:])
        #acquisition_date = date(year, month, day)

        key = tuple([satellite, wrs_path, wrs_row, year, month, day])
        print(scene, key)
        if key in catalog:
            continue

        if verbose:
            print('checking products for %s' % scene)

        products = espa.get_available_products(scene)
        if 'date_restricted' in json.dumps(products):
            print('   date restricted.')
            continue

        scene_list.append(scene)

    products = espa.get_available_products(scene_list[0])
    if landsat_num == 4:
        products = products['tm5_collection']['products']
    elif landsat_num == 5:
        products = products['tm5_collection']['products']
    elif landsat_num == 7:
        products = products['etm7_collection']['products']
    else:
        products = products['olitirs8_collection']['products']

    if verbose:
        print(landsat_num, products)

    # Place order (full scenes, no reprojection, sr and pixel_qa)
    order = espa.order(scene_list=scene_list, products=products)#, extent=bbox)

    if verbose:
        print(order.orderid)

    return order


def build_catalog(directory):
    fns = glob(_join(directory, '*.gz'))

    catalog = []
    for fn in fns:
        product_id = _split(fn)[-1].split('-')[0]

        satellite = int(product_id[2:4])
        wrs_path = int(product_id[4:7])
        wrs_row = int(product_id[7:10])

        _date = product_id[10:18]
        year, month, day = int(_date[:4]), int(_date[4:6]), int(_date[6:])
        catalog.append(tuple([satellite, wrs_path, wrs_row, year, month, day]))

    return catalog

if __name__ == "__main__":

    landsat_data_dir = "D:\Zumwalt\RS"
    catalog = build_catalog(landsat_data_dir)

#    sf_fn = "D:\RCR\VectorData\RockCreekRanch_habitatmodeled.shp"
    sf_fn = "D:\Zumwalt\VectorData\Zumwalt_AnalysisArea\Pastures_ForageAreas_2018_AllZumClip.shp"

    bbox = get_sf_wgs_bounds(sf_fn)
    print(bbox)

    y0 = 1990
    yend = 2019
    for landsat_num in [7, 8]:
        for yr in range(y0, yend+1):
            print(yr)
            place_order(bbox=bbox,
                        t0=datetime(yr, 1, 1),
                        tend=datetime(yr, 12, 31),
                        landsat_num=landsat_num)
