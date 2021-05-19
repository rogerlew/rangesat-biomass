from os.path import join as _join
from os.path import split as _split
from database.pasturestats import query_scenes_coverage

from os.path import isdir, exists

from glob import glob

from datetime import date

import numpy as np

from all_your_base import SCRATCH, RANGESAT_DIRS

from .location import Location

import sqlite3


def _scene_wrs_filter(fns, rowpath):
    return [fn for fn in fns if fn.split('_')[2] in rowpath]


def _scene_coverage_filter(location, fns, pasture_coverage_threshold=0.5, ls8_only=False):
    for rangesat_dir in RANGESAT_DIRS:
        loc_path = _join(rangesat_dir, location)
        if exists(loc_path):
            _location = Location(loc_path)
            scn_cov_db_fn = _location.scn_cov_db_fn

    if ls8_only:
        fns = [fn for fn in fns if fn[3] == '8']

    mask = query_scenes_coverage(scn_cov_db_fn, fns)
    mask = [coverage < pasture_coverage_threshold for coverage in mask]

    return [fn for m, fn in zip(mask, fns) if not m]


def _query_all(_location):
    scn_cov_db_fn = _location.scn_cov_db_fn

    conn = sqlite3.connect(scn_cov_db_fn)
    c = conn.cursor()

    query = 'SELECT * FROM scenemeta_coverage'

    c.execute(query)
    rows = c.fetchall()
    return [product_id for product_id, coverage in rows]


def scenemeta_location_all(location, rowpath=None, pasture_coverage_threshold=0.5, ls8_only=False):

    for rangesat_dir in RANGESAT_DIRS:
        loc_path = _join(rangesat_dir, location)
        if exists(loc_path):
            _location = Location(loc_path)
            ls_fns = _query_all(_location)

            if rowpath is not None:
                ls_fns = _scene_wrs_filter(ls_fns, rowpath)

            if pasture_coverage_threshold is not None:
                ls_fns = _scene_coverage_filter(location, ls_fns, pasture_coverage_threshold, ls8_only)

            return _scene_sorter(ls_fns)


def scenemeta_location_closest_date(location, target_date, rowpath=None, pasture_coverage_threshold=0.5, ls8_only=False):
    yr, mo, da = map(int, target_date.split('-'))
    _target = date(yr, mo, da)

    for rangesat_dir in RANGESAT_DIRS:
        loc_path = _join(rangesat_dir, location)
        if exists(loc_path):
            _location = Location(loc_path)
            ls_fns = _query_all(_location)

            if ls8_only:
                ls_fns = [fn for fn in ls_fns if fn[3] == '8']

            if rowpath is not None:
                ls_fns = _scene_wrs_filter(ls_fns, rowpath)

            if pasture_coverage_threshold is not None:
                ls_fns = _scene_coverage_filter(location, ls_fns, pasture_coverage_threshold)

            dates = []
            for fn in ls_fns:
                date_str = fn.split('_')[3]
                dates.append(date(int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8])))

            return ls_fns[np.argmin([abs((_target - _date).days) for _date in dates])]


def scenemeta_location_latest(location, rowpath=None, pasture_coverage_threshold=0.5, ls8_only=False):
    for rangesat_dir in RANGESAT_DIRS:
        loc_path = _join(rangesat_dir, location)
        if exists(loc_path):
            _location = Location(loc_path)
            ls_fns = _query_all(_location)

            if rowpath is not None:
                ls_fns = _scene_wrs_filter(ls_fns, rowpath)

            if pasture_coverage_threshold is not None:
                ls_fns = _scene_coverage_filter(location, ls_fns, pasture_coverage_threshold, ls8_only)

            dates = [int(fn.split('_')[3]) for fn in ls_fns]
            return ls_fns[np.argmax(dates)]


def _scene_sorter(fns):
    return sorted(fns, key=lambda fn: int(fn.split('_')[3]))


def scenemeta_location_intrayear(location, year, start_date, end_date, rowpath=None,
                                 pasture_coverage_threshold=0.5, ls8_only=False):
    for rangesat_dir in RANGESAT_DIRS:
        loc_path = _join(rangesat_dir, location)
        if exists(loc_path):
            _location = Location(loc_path)
            ls_fns = _query_all(_location)

            dates = [fn.split('_')[3] for fn in ls_fns]
            dates = [date(int(d[:4]), int(d[4:6]), int(d[6:8])) for d in dates]
            _start_date = date(*map(int, '{}-{}'.format(year, start_date).split('-')))
            _end_date = date(*map(int, '{}-{}'.format(year, end_date).split('-')))
            mask = [_start_date < d < _end_date for d in dates]

            ls_fns = [fn for fn, m in zip(ls_fns, mask) if m]

            if rowpath is not None:
                ls_fns = _scene_wrs_filter(ls_fns, rowpath)

            if pasture_coverage_threshold is not None:
                ls_fns = _scene_coverage_filter(location, ls_fns, pasture_coverage_threshold, ls8_only)
            return _scene_sorter(ls_fns)


def scenemeta_location_interyear(location, start_year, end_year, start_date, end_date, rowpath=None,
                                 pasture_coverage_threshold=0.5, ls8_only=False):
    for rangesat_dir in RANGESAT_DIRS:
        loc_path = _join(rangesat_dir, location)
        if exists(loc_path):
            _location = Location(loc_path)
            ls_fns = _query_all(_location)

            dates = [fn.split('_')[3] for fn in ls_fns]
            dates = [date(int(d[:4]), int(d[4:6]), int(d[6:8])) for d in dates]
            start_year = int(start_year)
            end_year = int(end_year)
            mask = [start_year < d.year < end_year for d in dates]

            if start_date is not None and end_date is not None:
                for i, (d, m) in enumerate(zip(dates, mask)):
                    if not m:
                        continue

                    _start_date = date(*map(int, '{}-{}'.format(d.year, start_date).split('-')))
                    _end_date = date(*map(int, '{}-{}'.format(d.year, end_date).split('-')))
                    mask[i] = _start_date < d < _end_date

            ls_fns = [fn for fn, m in zip(ls_fns, mask) if m]

            if rowpath is not None:
                ls_fns = _scene_wrs_filter(ls_fns, rowpath)

            if pasture_coverage_threshold is not None:
                ls_fns = _scene_coverage_filter(location, ls_fns, pasture_coverage_threshold, ls8_only)
            return _scene_sorter(ls_fns)
