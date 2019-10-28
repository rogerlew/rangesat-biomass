import uuid

import requests
import shutil
from enum import Enum
import os
import sys
from os.path import join as _join
from os.path import exists, dirname

import numpy as np

import netCDF4

sys.path.insert(0, '/var/www/rangesat-biomass')
sys.path.insert(0, '/Users/roger/rangesat-biomass')

from all_your_base import SCRATCH
from all_your_base.locationinfo import RasterDatasetInterpolator


class GridMetVariable(Enum):
    Precipitation = 1
    MinimumTemperature = 2
    MaximumTemperature = 3
    SurfaceRadiation = 4
    PalmarDroughtSeverityIndex = 5
    PotentialEvapotranspiration = 6
    BurningIndex = 7


_var_meta = {
    GridMetVariable.Precipitation: ('pr', 'precipitation_amount'),
    GridMetVariable.MinimumTemperature: ('tmmn', 'air_temperature'),
    GridMetVariable.MaximumTemperature: ('tmmx', 'air_temperature'),
    GridMetVariable.SurfaceRadiation: ('srad', 'surface_downwelling_shortwave_flux_in_air'),
    GridMetVariable.PalmarDroughtSeverityIndex: ('pdsi', 'palmer_drought_severity_index'),
    GridMetVariable.PotentialEvapotranspiration: ('pet', 'potential_evapotranspiration'),
    GridMetVariable.BurningIndex: ('bi', 'burning_index_g'),
}


def nc_extract(fn, locations):
    rds = RasterDatasetInterpolator(fn, proj='EPSG:4326')

    d = {}
    for key in locations:
        lng, lat = locations[key]
        data = rds.get_location_info(lng, lat, 'nearest')

        d[key] = data

    return d


def _retrieve(gridvariable: GridMetVariable, bbox, year):
    global _var_meta

    abbrv, variable_name = _var_meta[gridvariable]

    assert len(bbox) == 4
    west, north, east, south = [float(v) for v in bbox]
    assert east > west
    assert south < north

    url = 'http://thredds.northwestknowledge.net:8080/thredds/ncss/MET/{abbrv}/{abbrv}_{year}.nc?' \
          'var={variable_name}&' \
          'north={north}&west={west}&east={east}&south={south}&' \
          'disableProjSubset=on&horizStride=1&' \
          'time_start={year}-01-01T00%3A00%3A00Z&' \
          'time_end={year}-12-31T00%3A00%3A00Z&' \
          'timeStride=1&accept=netcdf' \
        .format(year=year, east=east, west=west, south=south, north=north,
                abbrv=abbrv, variable_name=variable_name)

    referer = 'https://rangesat.nkn.uidaho.edu'
    s = requests.Session()
    response = s.get(url, headers={'referer': referer}, stream=True)
    id = uuid.uuid4()
    with open(_join(SCRATCH, '%s.nc' % id), 'wb') as out_file:
        shutil.copyfileobj(response.raw, out_file)
    del response

    return id


def dump(abbrv, year, key, ts, desc, units, met_dir):
    pasture, ranch = key
    fn = _join(met_dir, ranch, pasture, str(year), '%s.npy' % abbrv)
    os.makedirs(dirname(fn), exist_ok=True)

    with open(fn, 'wb') as fp:
        np.save(fp, ts)


def retrieve_timeseries(variables, locations, start_year, end_year, met_dir):
    global _var_meta

    lons = [loc[0] for loc in locations.values()]
    lats = [loc[1] for loc in locations.values()]

    ll_x, ll_y = min(lons), min(lats)
    ur_x, ur_y = max(lons), max(lats)

    if len(locations) ==  1:
        ll_x -= 0.04
        ll_y -= 0.04
        ur_x += 0.04
        ur_y += 0.04

    bbox = [ll_x, ur_y, ur_x, ll_y]

    start_year = int(start_year)
    end_year = int(end_year)

    assert start_year <= end_year

    for gridvariable in variables:
        for year in range(start_year, end_year + 1):
            id = _retrieve(gridvariable, bbox, year)
            fn = _join(SCRATCH, '%s.nc' % id)

            try:
                _d = nc_extract(fn, locations)

                abbrv, variable_name = _var_meta[gridvariable]
                ds = netCDF4.Dataset(fn)
                variable = ds.variables[variable_name]
                desc = variable.description
                units = variable.units
                scale_factor = getattr(variable, 'scale_factor', 1.0)
                add_offset = getattr(variable, 'add_offset', 0.0)

                if _d is not None:
                    for key, ts in _d.items():
                        ts = np.array(ts, dtype=np.float64)
                        ts *= scale_factor
                        ts += add_offset
                        units = variable.units
                        if 'K' == variable.units:
                            ts -= 273.15
                            units = 'C'

                        dump(abbrv, year, key, ts, desc, units, met_dir)
                        #ts = [int(x) for x in ts]
                        #d['{}-{}-{}'.format(abbrv, year, key)] = (ts, desc, units)

                os.remove(fn)
            except:
                os.remove(fn)
                raise
    #return d


if __name__ == "__main__":
    from api.app import RANGESAT_DIRS, Location
    import os

    location = 'Zumwalt'

    _location = None
    for rangesat_dir in RANGESAT_DIRS:
        loc_path = _join(rangesat_dir, location)
        if exists(loc_path):
            _location = Location(loc_path)
            break

    assert _location is not None

    ranches = _location.ranches

    geo_locations = {}
    for ranch in ranches:
        _ranch = _location.serialized_ranch(ranch)
        pastures = _ranch['pastures']

        for pasture in pastures:
            #print(pasture, ranch)
            _pasture = _location.serialized_pasture(ranch, pasture)
            ranch = ranch.replace("'", "~").replace(' ', '_')
            pasture = pasture.replace("'", "~").replace(' ', '_')
            geo_locations[(pasture, ranch)] = _pasture['centroid']

    start_year = 1979
    end_year = 2019

    met_dir = _join(_location.loc_path, 'gridmet')
    #print(met_dir)

    if exists(met_dir):
        shutil.rmtree(met_dir)

    os.mkdir(met_dir)
    d = retrieve_timeseries([var for var in GridMetVariable],
                            geo_locations, start_year, end_year, met_dir)

