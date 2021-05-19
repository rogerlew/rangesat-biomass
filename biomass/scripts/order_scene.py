import sys

sys.path.insert(0, '/var/www/rangesat-biomass')

from datetime import datetime
import os
from os.path import join as _join
from os.path import exists


from api.app import Location
from all_your_base import RANGESAT_DIRS
from climate.gridmet import retrieve_timeseries, GridMetVariable


import json

from os.path import split as _split
from os.path import join as _join

from datetime import date
from glob import glob

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


def place_order(scene, verbose=True):
    global catalog

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

    scene_list = [scene]
    orders = []
    for scene in scene_list:
        satellite = int(scene[2:4])
        _scene = scene.split('_')
        wrs_path = int(_scene[2][:3])
        wrs_row = int(_scene[2][3:])

        _date = _scene[3]
        year, month, day = int(_date[:4]), int(_date[4:6]), int(_date[6:])
        #acquisition_date = date(year, month, day)

        key = tuple([satellite, wrs_path, wrs_row, year, month, day])
        print(key)
        if key in catalog:
            continue

        if verbose:
            print('checking products for %s' % scene)

        products = espa.get_available_products(scene)
        if 'date_restricted' in json.dumps(products):
            print('   date restricted.')
            continue

        products = espa.get_available_products(scene_list[0])
        if satellite == '04' or satellite == '05':
            try:
                products = products['tm5_collection']['products']
            except:
                products = products['tm4_collection']['products']
        elif satellite == '07':
            products = products['etm7_collection']['products']
        else:
            products = products['olitirs8_collection']['products']

        if verbose:
            print('satellite:', satellite, 'products:', products)

        # Place order (full scenes, no reprojection, sr and pixel_qa)
        order = espa.order(scene_list=scene_list, products=products)

        if verbose:
            print(order.orderid)

        orders.append(order)

    return orders


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


if __name__ == '__main__':
    landsat_data_dir = "/geodata/torch-landsat"
    catalog = build_catalog(landsat_data_dir)
    if len(catalog) <= 0:
        raise Exception

    scene = sys.argv[-1]
    assert not scene.endswith('.py'), 'expecting product id'
    place_order(scene)

