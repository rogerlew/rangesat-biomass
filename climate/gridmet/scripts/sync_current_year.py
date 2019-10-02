import sys

sys.path.insert(0, '/var/www/rangesat-biomass')

from datetime import datetime
import os
from os.path import join as _join
from os.path import exists


from api.app import RANGESAT_DIR, Location
from climate.gridmet import retrieve_timeseries, GridMetVariable

current_year = datetime.now().year

locations = ['Zumwalt', 'RCR']

for location in locations:
    loc_path = _join(RANGESAT_DIR, location)
    if exists(loc_path):
        _location = Location(loc_path)

        ranches = _location.ranches

        geo_locations = {}
        for ranch in ranches:
            _ranch = _location.serialized_ranch(ranch)
            pastures = _ranch['pastures']

            for pasture in pastures:
                _pasture = _location.serialized_pasture(ranch, pasture)
                ranch = ranch.replace("'", "~").replace(' ', '_')
                pasture = pasture.replace("'", "~").replace(' ', '_')
                geo_locations[(pasture, ranch)] = _pasture['centroid']

    start_year = current_year
    end_year = current_year

    met_dir = _join(_location.loc_path, 'gridmet')

    d = retrieve_timeseries([var for var in GridMetVariable],
                            geo_locations, start_year, end_year, met_dir)

