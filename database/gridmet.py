from glob import glob
from os.path import join as _join
from os.path import split as _split
from os.path import isdir, exists
from datetime import date, timedelta, datetime
from math import sqrt

import numpy as np

_variables = ('pr', 'tmmn', 'tmmx', 'srad', 'pdsi', 'pet', 'bi')

_descriptions = ('precipitation_amount',
                 'air_temperature',
                 'air_temperature',
                 'surface_downwelling_shortwave_flux_in_air',
                 'palmer_drought_severity_index',
                 'potential_evapotranspiration',
                 'burning_index_g')


def load_gridmet_single_year(directory, year):

    d = {}
    for var in _variables:
        fn = _join(directory, str(year), '%s.npy' % var)
        if exists(fn):
            d[var] = [float(x) for x in list(np.load(fn))]
        else:
            d[var] = None

    if d['pr'] is not None:
        d['dates'] = [str(date(int(year), 1, 1) + timedelta(i)) for i, _ in enumerate(d['pr'])]

    return d


def load_gridmet_all_years(directory, start_year=None, end_year=None):
    if start_year is None:
        start_year = 1979
    if end_year is None:
        end_year = datetime.now().year

    start_year = int(start_year)
    end_year = int(end_year)

    d = {}
    for year in range(start_year, end_year + 1):
        d[year] = load_gridmet_single_year(directory, year)

    return d


def load_gridmet_annual_progression(directory, start_year=None, end_year=None):
    if start_year is None:
        start_year = 1979
    if end_year is None:
        end_year = datetime.now().year

    start_year = int(start_year)
    end_year = int(end_year)

    d = {var: [] for var in _variables}
    for year in range(start_year, end_year + 1):
        _d = load_gridmet_single_year(directory, year)
        for var in _variables:
            ts = _d[var]
            if ts is not None:
                if len(ts) >= 365:
                    d[var].append(ts[:365])

    for var in _variables:
        m = np.array(d[var])
        if len(m.shape) == 2:
            d[var + '_mean'] = [float(x) for x in np.mean(m, axis=0)]
            d[var + '_std'] = [float(x) for x in np.std(m, axis=0)]
            d[var + '_ci90'] = [1.645 * (mu / sigma)
                                for mu, sigma in zip(d[var + '_mean'], d[var + '_std'])]
        else:
            d[var + '_mean'] = None
            d[var + '_std'] = None
            d[var + '_ci90'] = None

        del d[var]

    return d