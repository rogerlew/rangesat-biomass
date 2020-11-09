from glob import glob
from os.path import join as _join
from os.path import split as _split
from os.path import isdir, exists
from datetime import date, timedelta, datetime
from math import sqrt
import math
import numpy as np

_variables = ('pr', 'tmmn', 'tmmx', 'srad', 'pdsi', 'pet', 'bi')

_descriptions = ('precipitation_amount',
                 'air_temperature',
                 'air_temperature',
                 'surface_downwelling_shortwave_flux_in_air',
                 'palmer_drought_severity_index',
                 'potential_evapotranspiration',
                 'burning_index_g')


_days_in_mo = np.array([31, 28.25, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31])


def c_to_f(x):
    if x is None:
        return None

    return 9.0/5.0 * x + 32.0


def mm_to_in(x):
    if x is None:
        return None

    return x / 25.4


def load_gridmet_single_year(directory, year, units='SI'):

    d = {}
    for var in _variables:
        fn = _join(directory, str(year), '%s.npy' % var)
        if exists(fn):
            d[var] = [float(x) for x in list(np.load(fn))]
        else:
            d[var] = None

    if d['pr'] is not None:
        d['dates'] = [str(date(int(year), 1, 1) + timedelta(i)) for i, _ in enumerate(d['pr'])]
        d['cum_pr'] = list(np.cumsum(d['pr']))

    if d['pr'] is not None and d['pet'] is not None:
        d['pwd'] = [pr - pet for pr, pet in zip(d['pr'], d['pet'])]
    else:
        d['pwd'] = None

    if units.lower().startswith('e'):
        for var in ['tmmn', 'tmmx']:
            if d[var] is not None:
                d[var] = [c_to_f(v) for v in d[var]]

        for var in ['pr', 'pet', 'pwd']:
            if d[var] is not None:
                d[var] = [mm_to_in(v) for v in d[var]]

    return d


_month_labels = ['January', 'February', 'March', 'April', 'May', 'June',
                 'July', 'August', 'September', 'October', 'November', 'December']


def load_gridmet_single_year_monthly(directory, year, units='SI'):

    d = {}
    for var in _variables:
        fn = _join(directory, str(year), '%s.npy' % var)
        if exists(fn):
            d[var] = [float(x) for x in list(np.load(fn))]
        else:
            d[var] = None

    if d['pr'] is not None:
        d['dates'] = [str(date(int(year), 1, 1) + timedelta(i)) for i, _ in enumerate(d['pr'])]
        d['cum_pr'] = list(np.cumsum(d['pr']))

    _dates = [date(int(year), 1, 1) + timedelta(i) for i, _ in enumerate(d['pr'])]

    _d = {}
    for var in _variables:
        if var in ['pr', 'pet']:
            agg_func = np.sum
        else:
            agg_func = np.mean

        _d[var] = []
        for mo in range(1, 13):
            if d[var] is None:
                _d[var].append(None)
                continue

            res = [v for j, v in enumerate(d[var]) if int(_dates[j].month) == int(mo)]
            res = agg_func(res)
            if math.isnan(res):
                res = None
            _d[var].append(res)

    _d['pwd'] = []
    for pr, pet in zip(_d['pr'], _d['pet']):
        if pr is not None and pet is not None:
            res = pr - pet
        else:
            res = None

        _d['pwd'].append(res)

    if units.lower().startswith('e'):
        for var in ['tmmn', 'tmmx']:
            _d[var] = [c_to_f(v) for v in _d[var]]

        for var in ['pr', 'pet', 'pwd']:
            _d[var] = [mm_to_in(v) for v in _d[var]]

    _d['months'] = _month_labels
    _d['year'] = year

    return _d


def load_gridmet_all_years(directory, start_year=None, end_year=None, units='SI'):
    if start_year is None:
        start_year = 1979
    if end_year is None:
        end_year = datetime.now().year

    start_year = int(start_year)
    end_year = int(end_year)

    d = {}
    for year in range(start_year, end_year + 1):
        d[year] = load_gridmet_single_year(directory, year, units)

    return d


def load_gridmet_annual_progression(directory, start_year=None, end_year=None, units='SI'):
    if start_year is None:
        start_year = 1979
    if end_year is None:
        end_year = datetime.now().year

    start_year = int(start_year)
    end_year = int(end_year)

    d = {var: [] for var in list(_variables) + ['pwd']}
    for year in range(start_year, end_year + 1):
        _d = load_gridmet_single_year(directory, year, units)
        for var in _variables:
            ts = _d[var]
            if ts is not None:
                if len(ts) >= 365:
                    d[var].append(ts[:365])

    for var in list(_variables) + ['pwd']:
        m = np.array(d[var])
        if len(m.shape) == 2:
            d[var + '_mean'] = [float(x) for x in np.mean(m, axis=0)]
            d[var + '_std'] = [float(x) for x in np.std(m, axis=0)]

            d[var + '_ci90'] = []
            for mu, sigma in zip(d[var + '_mean'], d[var + '_std']):
                if sigma > 0.0:
                    d[var + '_ci90'].append(1.645 * (mu / sigma))
                else:
                    d[var + '_ci90'].append(None)

        else:
            d[var + '_mean'] = None
            d[var + '_std'] = None
            d[var + '_ci90'] = None

        del d[var]

    return d


def load_gridmet_annual_progression_monthly(directory, start_year=None, end_year=None, units='SI'):
    if start_year is None:
        start_year = 1979
    if end_year is None:
        end_year = datetime.now().year

    start_year = int(start_year)
    end_year = int(end_year)

    d = {var: [] for var in list(_variables) + ['pwd']}
    for year in range(start_year, end_year + 1):
        _d = load_gridmet_single_year(directory, year, units)
        for var in list(_variables) + ['pwd']:
            ts = _d[var]
            if ts is not None:
                if len(ts) >= 365:
                    d[var].append(ts[:365])

    bad = []
    for var in d:
        if len(d[var]) > 0:
            d[var] = np.concatenate(d[var])
        else:
            bad.append(var)

    _dates = [date(int(start_year), 1, 1) + timedelta(i) for i, _ in enumerate(d['pr'])]

    _d = {}
    for var in list(_variables) + ['pwd']:
        if var in bad:
            continue

        _d[var] = []
        for mo in range(1, 13):
            res = [v for j, v in enumerate(d[var]) if int(_dates[j].month) == int(mo)]
            res = np.mean(res)
            if var in ['pr', 'pet', 'pwd']:
                res *= _days_in_mo[mo-1]

            if math.isnan(res):
                res = None
            _d[var].append(res)

    _d['months'] = _month_labels
    _d['year'] = year

    for var in bad:
        _d[var] = None

    return _d
