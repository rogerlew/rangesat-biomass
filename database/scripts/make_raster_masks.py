import sqlite3
from glob import glob
from datetime import date
import os
import csv
from os.path import join as _join
from os.path import exists
import shutil
import sys

sys.path.insert(0, '/var/www/rangesat-biomass')
from api.app import RANGESAT_DIRS, Location
from all_your_base import isfloat

locations = [sys.argv[-1]]


for location in locations:
    _location = None
    for rangesat_dir in RANGESAT_DIRS:
        loc_path = _join(rangesat_dir, location)
        if exists(loc_path):
            _location = Location(loc_path)
            break

    assert _location is not None

    reverse_pasture_ranch_key = _location.reverse_key

    out_dir = _location.out_dir
    key_delimiter = _location.key_delimiter
    print(_location.out_dir)

    raster_fn = glob(_join(_location.out_dir, '*/*ndvi.tif'))[0]
    print(raster_fn)

    mask_dir = _join(out_dir, '../raster_masks')
    if exists(mask_dir):
        shutil.rmtree(mask_dir)
    os.mkdir(mask_dir)

    for ranch in _location.ranches:
        print(ranch)
        _location.make_ranch_mask(raster_fn, [ranch], _join(mask_dir, f'{ranch}.tif'), nodata=255)
        _location.make_pastures_mask(raster_fn, ranch, _join(mask_dir, f'{ranch}.pastures.tif'), nodata=255)

