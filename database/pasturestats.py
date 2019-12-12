import sqlite3
from datetime import date
from glob import glob
import os
import csv
from os.path import join as _join
from os.path import exists as _exists
import shutil
import ast
import math

import numpy as np

from datetime import datetime


_month_labels = ['January', 'February', 'March', 'April', 'May', 'June',
                'July', 'August', 'September', 'October', 'November', 'December']


_header = ('product_id', 'key', 'pasture', 'ranch', 'total_px', 'snow_px', 
           'water_px', 'aerosol_px',
           'valid_px', 'coverage', 'model', 'biomass_mean_gpm', 'biomass_ci90_gpm',
           'biomass_10pct_gpm', 'biomass_75pct_gpm', 'biomass_90pct_gpm', 'biomass_total_kg',
           'biomass_sd_gpm', 'summer_vi_mean_gpm', 'fall_vi_mean_gpm', 'fraction_summer',
           'ndvi_mean', 'ndvi_sd', 'ndvi_10pct', 'ndvi_75pct', 'ndvi_90pct', 'ndvi_ci90',
           'nbr_mean', 'nbr_sd', 'nbr_10pct', 'nbr_75pct', 'nbr_90pct', 'nbr_ci90',
           'nbr2_mean', 'nbr2_sd', 'nbr2_10pct', 'nbr2_75pct', 'nbr2_90pct', 'nbr2_ci90',
           'satellite', 'acquisition_date')

_measures = ('biomass_mean_gpm', 'biomass_ci90_gpm',
             'biomass_10pct_gpm', 'biomass_75pct_gpm', 'biomass_90pct_gpm', 'biomass_total_kg',
             'biomass_sd_gpm', 'summer_vi_mean_gpm', 'fall_vi_mean_gpm',
             'ndvi_mean', 'ndvi_sd', 'ndvi_10pct', 'ndvi_75pct', 'ndvi_90pct', 'ndvi_ci90',
             'nbr_mean', 'nbr_sd', 'nbr_10pct', 'nbr_75pct', 'nbr_90pct', 'nbr_ci90',
             'nbr2_mean', 'nbr2_sd', 'nbr2_10pct', 'nbr2_75pct', 'nbr2_90pct', 'nbr2_ci90',
             )


def _aggregate(rows, agg_func):
    results = {}
    for measure in _measures:
        val = agg_func([row[measure] for row in rows if row[measure] is not None])
        if math.isnan(val):
            val = None
        results[measure] = val
    return results


def _date_of_max(rows, measure='biomass_90pct_gpm'):
    indx = np.argmax([(row[measure], -99999.0)[row[measure] is None] for row in rows])
    return rows[indx]


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


def query_scene_product_ids(db_fn):
    conn = sqlite3.connect(db_fn)
    c = conn.cursor()

    query = 'SELECT product_id FROM pasture_stats'

    c.execute(query)
    rows = c.fetchall()
    return sorted(set([row[0] for row in rows]))


def query_scenes_coverage(scn_cov_db_fn, product_ids):
    conn = sqlite3.connect(scn_cov_db_fn)
    c = conn.cursor()

    query = 'SELECT * FROM scenemeta_coverage'

    c.execute(query)
    rows = c.fetchall()
    d = {product_id: coverage for product_id, coverage in rows}

    res = []
    for product_id in product_ids:
        res.append(d.get(product_id, 0.0))

    return res


def query_pasture_stats(db_fn, ranch=None, acquisition_date=None, pasture=None):
    conn = sqlite3.connect(db_fn)
    c = conn.cursor()

    query = 'SELECT ' + \
            ', '.join(_header) + \
            ' FROM pasture_stats'

    i = 0
    if ranch is not None or acquisition_date is not None or pasture is not None:
        query += ' WHERE'

    if ranch is not None:
        query += ' ranch = "{ranch}"'.format(ranch=ranch.replace('_', ' '))
        i += 1

    if pasture is not None:
        if i > 0:
            query += ' AND'
        query += ' pasture = "{pasture}"'.format(pasture=pasture.replace('_', ' ').replace('Derrick Old', 'Derrick_Old'))
        i += 1

    if acquisition_date is not None:
        if i > 0:
            query += ' AND'
        query += ' acquisition_date = "{acquisition_date}"'.format(acquisition_date=acquisition_date)
        i += 1

    c.execute(query)
    rows = c.fetchall()

    return [dict(zip(_header, row)) for row in rows]


def query_singleyear_pasture_stats(db_fn, ranch=None, pasture=None, year=None,
                                  start_date=None, end_date=None, agg_func=np.mean,
                                  key_delimiter='+'):
    if start_date is None:
        start_date = '1-1'

    if end_date is None:
        end_date = '12-31'

    conn = sqlite3.connect(db_fn)
    c = conn.cursor()

    query = 'SELECT ' + \
            ', '.join(_header) + \
            ' FROM pasture_stats'

    i = 0
    if ranch is not None or year is not None or pasture is not None:
        query += ' WHERE'

    if ranch is not None:
        query += ' ranch = "{ranch}"'.format(ranch=ranch.replace('_', ' '))
        i += 1

    if pasture is not None:
        if i > 0:
            query += ' AND'
        query += ' pasture = "{pasture}"'.format(pasture=pasture.replace('_', ' ').replace('Derrick Old', 'Derrick_Old'))
        i += 1

    if year is not None:
        if i > 0:
            query += ' AND'
        query += " instr(acquisition_date, '{year}') > 0".format(year=year)
        i += 1

    c.execute(query)
    rows = c.fetchall()

    dates = [date(*map(int, row[-1].split('-'))) for row in rows]
    _start_date = date(*map(int, '{}-{}'.format(year, start_date).split('-')))
    _end_date = date(*map(int, '{}-{}'.format(year, end_date).split('-')))
    mask = [_start_date < d < _end_date for d in dates]

    rows = [dict(zip(_header, row)) for m, row in zip(mask, rows) if m]
    rows = [d for d in rows if d['biomass_mean_gpm'] is not None]
    rows = sorted(rows, key=lambda k: k['acquisition_date'])
    return rows


def query_intrayear_pasture_stats(db_fn, ranch=None, pasture=None, year=None,
                                  start_date=None, end_date=None, agg_func=np.mean,
                                  key_delimiter='+'):
    rows = query_singleyear_pasture_stats(db_fn, ranch=ranch, pasture=pasture, year=year,
                                          start_date=start_date, end_date=end_date, agg_func=agg_func,
                                          key_delimiter=key_delimiter)

    keys = set(row['key'] for row in rows)
    d = {key: [] for key in keys}

    for row in rows:
        d[row['key']].append(row)

    agg = []
    for key in d:
        agg.append(_aggregate(d[key], agg_func))
        _pasture, _ranch = key.split(key_delimiter)
        agg[-1]['pasture'] = _pasture
        agg[-1]['ranch'] = _ranch

    return agg


def query_max_pasture_seasonal_pasture_stats(db_fn, ranch=None, pasture=None, year=None,
                                             start_date=None, end_date=None, measure='biomass_mean_gpm',
                                             key_delimiter='+'):

    rows = query_singleyear_pasture_stats(db_fn, ranch=ranch, pasture=pasture, year=year,
                                          start_date=start_date, end_date=end_date, agg_func=None,
                                          key_delimiter=key_delimiter)

    keys = set(row['key'] for row in rows)
    d = {key: [] for key in keys}

    for row in rows:
        d[row['key']].append(row)

    agg = []
    for key in d:
        _pasture, _ranch = key.split(key_delimiter)
        agg.append(_date_of_max(d[key], measure))

    return agg


def query_singleyearmonthly_pasture_stats(db_fn, ranch=None, pasture=None, year=None,
                                          start_date=None, end_date=None,
                                         agg_func=np.mean,
                                         key_delimiter='+'):
    if year is None:
        year = datetime.now().year

    if start_date is None:
        start_date = '1-1'

    if end_date is None:
        end_date = '12-31'

    rows = query_singleyear_pasture_stats(db_fn, ranch=ranch, pasture=pasture, year=year,
                                          start_date=start_date, end_date=end_date, agg_func=agg_func,
                                          key_delimiter=key_delimiter)

    keys = set(row['key'] for row in rows)

    monthlies = {key: [[] for i in range(12)] for key in keys}
    for i, row in enumerate(rows):
        if row['biomass_mean_gpm'] is None:
            continue

        if row['biomass_mean_gpm'] == 0:
            continue

        if row['coverage'] < 0.5:
            continue

        month_indx = int(row['acquisition_date'].split('-')[1]) - 1

        assert 0 <= month_indx <= 11, month_indx
        monthlies[row['key']][month_indx].append(row)

    agg = {}
    for key in keys:
        _pasture, _ranch = key.split(key_delimiter)
        _monthlies = []

        for i, d in enumerate(monthlies[key]):
            _monthlies.append(_aggregate(d, agg_func))
            _monthlies[-1]['pasture'] = _pasture
            _monthlies[-1]['ranch'] = _ranch
            _monthlies[-1]['month'] = _month_labels[i]

        agg[key] = _monthlies

    return agg


def query_seasonalprogression_pasture_stats(db_fn, ranch=None, pasture=None,
                                            start_year=None, end_year=None,
                                            agg_func=np.mean, key_delimiter='+'):
    if end_year is None:
        end_year = datetime.now().year
    else:
        end_year = int(end_year)

    if start_year is None:
        start_year = 1981
    else:
        start_year = int(start_year)

    conn = sqlite3.connect(db_fn)
    c = conn.cursor()

    query = 'SELECT ' + \
            ', '.join(_header) + \
            ' FROM pasture_stats'

    i = 0
    if ranch is not None or pasture is not None:
        query += ' WHERE'

    if ranch is not None:
        query += ' ranch = "{ranch}"'.format(ranch=ranch.replace('_', ' '))
        i += 1

    if pasture is not None:
        if i > 0:
            query += ' AND'
        query += ' pasture = "{pasture}"'.format(pasture=pasture.replace('_', ' ')
                                                                .replace('Derrick Old', 'Derrick_Old'))
        i += 1

    c.execute(query)
    rows = c.fetchall()
    rows = [dict(zip(_header, row)) for row in rows]

    keys = set(row['key'] for row in rows)

    monthlies = {key: [[] for i in range(12)] for key in keys}
    for i, row in enumerate(rows):
        if row['biomass_mean_gpm'] is None:
            continue

        if row['biomass_mean_gpm'] == 0:
            continue

        if row['coverage'] < 0.5:
            continue
        acquisition_date = row['acquisition_date']
        yr, mo, da = map(int, row['acquisition_date'].split('-'))

        if not start_year <= yr <= end_year:
            continue

        month_indx = mo - 1

        assert 0 <= month_indx <= 11, month_indx
        monthlies[row['key']][month_indx].append(row)

    agg = {}
    for key in keys:
        _pasture, _ranch = key.split(key_delimiter)
        _monthlies = []

        for i, d in enumerate(monthlies[key]):
            _monthlies.append(_aggregate(d, agg_func))
            _monthlies[-1]['pasture'] = _pasture
            _monthlies[-1]['ranch'] = _ranch
            _monthlies[-1]['month'] = _month_labels[i]

        agg[key] = _monthlies

    return agg


def query_interyear_pasture_stats(db_fn, ranch=None, pasture=None, start_year=None, end_year=None,
                                  start_date=None, end_date=None, agg_func=np.mean,
                                  key_delimiter='+'):
    conn = sqlite3.connect(db_fn)
    c = conn.cursor()

    query = 'SELECT ' + \
            ', '.join(_header) + \
            ' FROM pasture_stats'

    i = 0
    if ranch is not None or pasture is not None:
        query += ' WHERE'

    if ranch is not None:
        query += ' ranch = "{ranch}"'.format(ranch=ranch.replace('_', ' '))
        i += 1

    if pasture is not None:
        if i > 0:
            query += ' AND'
        query += ' pasture = "{pasture}"'.format(pasture=pasture.replace('_', ' ').replace('Derrick Old', 'Derrick_Old'))
        i += 1

    c.execute(query)
    rows = c.fetchall()

    dates = [date(*map(int, row[-1].split('-'))) for row in rows]
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
    rows = [d for d in rows if d['biomass_mean_gpm'] is not None]

    keys = set(row['key'] for row in rows)
    d = {key: [] for key in keys}

    for row in rows:
        d[row['key']].append(row)

    agg = []
    for key in d:
        agg.append(_aggregate(d[key], agg_func))
        _pasture, _ranch = key.split(key_delimiter)
        agg[-1]['pasture'] = _pasture
        agg[-1]['ranch'] = _ranch

    return agg


def query_multiyear_pasture_stats(db_fn, ranch=None, pasture=None, start_year=None, end_year=None,
                                  start_date=None, end_date=None, agg_func=np.mean,
                                  key_delimiter='+'):
    if end_year is None:
        end_year = datetime.now().year
    if start_year is None:
        start_year = 1981

    if start_date is None:
        start_date = '1-1'

    if end_date is None:
        end_date = '12-31'

    conn = sqlite3.connect(db_fn)
    c = conn.cursor()

    query = 'SELECT ' + \
            ', '.join(_header) + \
            ' FROM pasture_stats'

    i = 0
    if ranch is not None or pasture is not None:
        query += ' WHERE'

    if ranch is not None:
        query += ' ranch = "{ranch}"'.format(ranch=ranch.replace('_', ' '))
        i += 1

    if pasture is not None:
        if i > 0:
            query += ' AND'
        query += ' pasture = "{pasture}"'.format(pasture=pasture.replace('_', ' ').replace('Derrick Old', 'Derrick_Old'))
        i += 1

    c.execute(query)
    rows = c.fetchall()
    dates = [date(*map(int, row[-1].split('-'))) for row in rows]
    keys = set(row[1] for row in rows)

    agg = []
    start_year = int(start_year)
    end_year = int(end_year)
    for year in range(start_year, end_year+1):
        _start_date = date(*map(int, '{}-{}'.format(year, start_date).split('-')))
        _end_date = date(*map(int, '{}-{}'.format(year, end_date).split('-')))
        mask = [_start_date < d < _end_date for d in dates]
        if not any(mask):
            continue

        _rows = [dict(zip(_header, row)) for m, row in zip(mask, rows) if m]
        _rows = [d for d in _rows if d['biomass_mean_gpm'] is not None]

        d = {key: [] for key in keys}
        for row in _rows:
            d[row['key']].append(row)

        for key in d:
            agg.append(_aggregate(d[key], agg_func))
            _pasture, _ranch = key.split(key_delimiter)
            agg[-1]['pasture'] = _pasture
            agg[-1]['ranch'] = _ranch
            agg[-1]['year'] = year

    return sorted(agg, key=_sortkeypicker(['ranch', 'pasture', 'year']))


if __name__ == "__main__":
    db_fn = '/Users/roger/Downloads/sqlite3.db'
    res = query_scene_coverage(db_fn, product_id='LT05_L1TP_042028_20000820_20160922_01_T1')
    print(res)
