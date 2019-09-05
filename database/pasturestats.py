import sqlite3
from datetime import date
from glob import glob
import os
import csv
from os.path import join as _join
from os.path import exists as _exists
import shutil

import numpy as np


_header = ('product_id', 'key', 'pasture', 'ranch', 'total_px', 'snow_px', 
                 'water_px', 'aerosol_px',
                 'valid_px', 'coverage', 'model', 'biomass_mean_gpm', 'biomass_ci90_gpm',
                 'biomass_10pct_gpm', 'biomass_75pct_gpm', 'biomass_90pct_gpm', 'biomass_total_kg',
                 'biomass_sd_gpm', 'summer_vi_mean_gpm', 'fall_vi_mean_gpm', 'fraction_summer',
                 'satellite', 'acquisition_date', 'wrs', 'bounds', 'valid_pastures_cnt')

_measures = ('biomass_mean_gpm', 'biomass_ci90_gpm',
             'biomass_10pct_gpm', 'biomass_75pct_gpm', 'biomass_90pct_gpm', 'biomass_total_kg',
             'biomass_sd_gpm', 'summer_vi_mean_gpm', 'fall_vi_mean_gpm')


def _aggregate(rows, agg_func):
    results = {}
    for measure in _measures:
        results[measure] = agg_func([row[measure] for row in rows if row[measure] is not None])
    return results


def _sortkeypicker(keynames):
    negate = set()
    for i, k in enumerate(keynames):
        if k[:1] == '-':
            keynames[i] = k[1:]
            negate.add(k[1:])

    def getit(adict):
        composite = [adict[k] for k in keynames]
        for i, (k, v) in enumerate(zip(keynames, composite)):
            if k in negate:
                composite[i] = -v
        return composite

    return getit


def query_pasture_stats(db_fn, ranch=None, acquisition_date=None, pasture=None):
    conn = sqlite3.connect(db_fn)
    c = conn.cursor()

    query = 'SELECT * FROM pasture_stats'

    i = 0
    if ranch is not None or acquisition_date is not None or pasture is not None:
        query += ' WHERE'

    if ranch is not None:
        query += ' ranch = "{ranch}"'.format(ranch=ranch.replace('_', ' '))
        i += 1

    if pasture is not None:
        if i > 0:
            query += ' AND'
        query += ' pasture = "{pasture}"'.format(pasture=pasture.replace('_', ' '))
        i += 1

    if acquisition_date is not None:
        if i > 0:
            query += ' AND'
        query += ' acquisition_date = "{acquisition_date}"'.format(acquisition_date=acquisition_date)
        i += 1

    c.execute(query)
    rows = c.fetchall()

    return [dict(zip(_header, row)) for row in rows]


def query_intrayear_pasture_stats(db_fn, ranch=None, pasture=None, year=None,
                                  start_date=None, end_date=None, agg_func=np.mean):
    conn = sqlite3.connect(db_fn)
    c = conn.cursor()

    query = 'SELECT * FROM pasture_stats'

    i = 0
    if ranch is not None or year is not None or pasture is not None:
        query += ' WHERE'

    if ranch is not None:
        query += ' ranch = "{ranch}"'.format(ranch=ranch.replace('_', ' '))
        i += 1

    if pasture is not None:
        if i > 0:
            query += ' AND'
        query += ' pasture = "{pasture}"'.format(pasture=pasture.replace('_', ' '))
        i += 1

    if year is not None:
        if i > 0:
            query += ' AND'
        query += " instr(acquisition_date, '{year}') > 0".format(year=year)
        i += 1

    c.execute(query)
    rows = c.fetchall()

    dates = [date(*map(int, row[22].split('-'))) for row in rows]
    _start_date = date(*map(int, '{}-{}'.format(year, start_date).split('-')))
    _end_date = date(*map(int, '{}-{}'.format(year, end_date).split('-')))
    mask = [_start_date < d < _end_date for d in dates]

    rows = [dict(zip(_header, row)) for m, row in zip(mask, rows) if m]

    keys = set(row['key'] for row in rows)
    d = {key: [] for key in keys}

    for row in rows:
        d[row['key']].append(row)

    agg = []
    for key in d:
        agg.append(_aggregate(d[key], agg_func))
        _pasture, _ranch = key.split('+')
        agg[-1]['pasture'] = _pasture
        agg[-1]['ranch'] = _ranch

    return agg


def query_interyear_pasture_stats(db_fn, ranch=None, pasture=None, start_year=None, end_year=None,
                                  start_date=None, end_date=None, agg_func=np.mean):
    conn = sqlite3.connect(db_fn)
    c = conn.cursor()

    query = 'SELECT * FROM pasture_stats'

    i = 0
    if ranch is not None or pasture is not None:
        query += ' WHERE'

    if ranch is not None:
        query += ' ranch = "{ranch}"'.format(ranch=ranch.replace('_', ' '))
        i += 1

    if pasture is not None:
        if i > 0:
            query += ' AND'
        query += ' pasture = "{pasture}"'.format(pasture=pasture.replace('_', ' '))
        i += 1

    c.execute(query)
    rows = c.fetchall()

    dates = [date(*map(int, row[22].split('-'))) for row in rows]
    start_year = int(start_year)
    end_year = int(end_year)
    mask = [start_year < d.year < end_year for d in dates]

    for i, (d, m) in enumerate(zip(dates, mask)):
        if not m:
            continue

        _start_date = date(*map(int, '{}-{}'.format(d.year, start_date).split('-')))
        _end_date = date(*map(int, '{}-{}'.format(d.year, end_date).split('-')))
        mask[i] = _start_date < d < _end_date

    rows = [dict(zip(_header, row)) for m, row in zip(mask, rows) if m]

    keys = set(row['key'] for row in rows)
    d = {key: [] for key in keys}

    for row in rows:
        d[row['key']].append(row)

    agg = []
    for key in d:
        agg.append(_aggregate(d[key], agg_func))
        _pasture, _ranch = key.split('+')
        agg[-1]['pasture'] = _pasture
        agg[-1]['ranch'] = _ranch

    return agg


def query_multiyear_pasture_stats(db_fn, ranch=None, pasture=None, start_year=None, end_year=None,
                                  start_date=None, end_date=None, agg_func=np.mean):
    conn = sqlite3.connect(db_fn)
    c = conn.cursor()

    query = 'SELECT * FROM pasture_stats'

    i = 0
    if ranch is not None or pasture is not None:
        query += ' WHERE'

    if ranch is not None:
        query += ' ranch = "{ranch}"'.format(ranch=ranch.replace('_', ' '))
        i += 1

    if pasture is not None:
        if i > 0:
            query += ' AND'
        query += ' pasture = "{pasture}"'.format(pasture=pasture.replace('_', ' '))
        i += 1

    c.execute(query)
    rows = c.fetchall()
    dates = [date(*map(int, row[22].split('-'))) for row in rows]
    keys = set(row[1] for row in rows)

    agg = []
    start_year = int(start_year)
    end_year = int(end_year)
    for year in range(start_year, end_year+1):
        _start_date = date(*map(int, '{}-{}'.format(year, start_date).split('-')))
        _end_date = date(*map(int, '{}-{}'.format(year, end_date).split('-')))
        mask = [_start_date < d < _end_date for d in dates]
        _rows = [dict(zip(_header, row)) for m, row in zip(mask, rows) if m]

        d = {key: [] for key in keys}
        for row in _rows:
            d[row['key']].append(row)

        for key in d:
            agg.append(_aggregate(d[key], agg_func))
            _pasture, _ranch = key.split('+')
            agg[-1]['pasture'] = _pasture
            agg[-1]['ranch'] = _ranch
            agg[-1]['year'] = year

    return sorted(agg, key=_sortkeypicker(['ranch', 'pasture', 'year']))

