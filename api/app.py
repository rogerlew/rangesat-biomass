#!/usr/bin/python3

from os.path import join as _join
from os.path import split as _split

from os.path import isdir, exists

from flask import Flask, jsonify, send_file, request, after_this_request
from glob import glob
from datetime import datetime, date
import os
import uuid
import numpy as np
from ast import literal_eval
from csv import DictWriter

import fiona
import rasterio
from rasterio.mask import raster_geometry_mask

from all_your_base import SCRATCH, RANGESAT_DIRS

from database import Location
from database.pasturestats import (
    query_pasture_stats,
    query_singleyear_pasture_stats,
    query_intrayear_pasture_stats,
    query_singleyearmonthly_pasture_stats,
    query_seasonalprogression_pasture_stats,
    query_interyear_pasture_stats,
    query_multiyear_pasture_stats
)

from database.gridmet import (
    load_gridmet_all_years,
    load_gridmet_single_year,
    load_gridmet_single_year_monthly,
    load_gridmet_annual_progression,
    load_gridmet_annual_progression_monthly
)

from biomass.landsat import LandSatScene
from biomass.raster_processing import make_raster_difference, make_aggregated_rasters

app = Flask(__name__)

_thisdir = os.path.dirname(__file__)
STATIC_DIR = _join(_thisdir, 'static')

@app.route('/')
def index():
    return jsonify(
       dict(location=[
                'location',
                'location/<location>'
                'location/<location>/<ranch>',
                'location/<location>/<ranch>/<pasture>'],
            geojson=[
                'geojson/<location>',
                'geojson/<location>/<ranch>',
                'geojson/<location>/<ranch>/<pasture>'],
            scenemeta=[
                'scenemeta/<location>',
                'scenemeta/<location>/<product_id>'],
            raster=[
                'raster/<location>/<product_id>/<product:{ndvi,nbr,nbr2,biomass,fall_vi,summer_vi}>'],
            rasterprocessing=[
                'raster-processing/difference/<location>/<product:{ndvi,nbr,nbr2,biomass,fall_vi,summer_vi}>?product_id=<product_id>&product_id2=<product_id2>&',
                'raster-processing/intra-year/<location>/<product:{ndvi,nbr,nbr2,biomass,fall_vi,summer_vi}>?year=<year>&start_date=<start_date>&end_date=<end_date>',
                'raster-processing/inter-year/<location>/<product:{ndvi,nbr,nbr2,biomass,fall_vi,summer_vi}>?start_year=<start_year>&end_year=<end_year>&start_date=<start_date>&end_date=<end_date>'],
            pasturestats=[
                'pasturestats/<location>/?ranch=<ranch>&pasture=<pasture>&acquisition_date=<acquisition_date>',
                'pasturestats/single-year/<location>/?ranch=<ranch>&pasture=<pasture>&year=<year>&start_date=<start_date>&end_date=<end_date>',
                'pasturestats/single-year-monthly/<location>/?ranch=<ranch>&pasture=<pasture>&year=<year>&start_date=<start_date>&end_date=<end_date>',
                'pasturestats/intra-year/<location>/?ranch=<ranch>&pasture=<pasture>&year=<year>&start_date=<start_date>&end_date=<end_date>',
                'pasturestats/inter-year/<location>/?ranch=<ranch>&pasture=<pasture>&start_year=<start_year>&end_year=<end_year>&start_date=<start_date>&end_date=<end_date>',
                'pasturestats/multi-year/<location>/?ranch=<ranch>&pasture=<pasture>&start_year=<start_year>&end_year=<end_year>&start_date=<start_date>&end_date=<end_date>'],
            gridmet=[
                'gridmet/single-year/<location>/<ranch>/<pasture>?year=<year>',
                'gridmet/single-year-monthly/<location>/<ranch>/<pasture>?year=<year>',
                'gridmet/multi-year/<location>/<ranch>/<pasture>?start_year=<start_year>&end_year=<end-year>',
                'gridmet/annual-progression/<location>/<ranch>/<pasture>?start_year=<start_year>&end_year=<end-year>',
                'gridmet/annual-progression-monthly/<location>/<ranch>/<pasture>?start_year=<start_year>&end_year=<end-year>'],
            histogram=[
                'histogram/single-scene/<location>/<ranch>/<pasture>?product_id=<product_id>&product=<product>',
                'histogram/intra-year/<location>/<ranch>/<pasture>?year=<year>&start_date=<start_date>&end_date=<end_date>&product=<product>',
                'histogram/inter-year/<location>/<ranch>/<pasture>?start_year=<start_year>&end_year=<end_year>&start_date=<start_date>&end_date=<end_date>&product=<product>']

       ))


@app.route('/location')
@app.route('/location/')
def locations():
    queryset = []
    for rangesat_dir in RANGESAT_DIRS:
        queryset.extend(glob('{}/*'.format(rangesat_dir)))
    queryset = [_split(loc)[-1] for loc in queryset if isdir(loc)]
    return jsonify(dict(locations=queryset))


@app.route('/location/<location>')
@app.route('/location/<location>/')
def location(location):
    for rangesat_dir in RANGESAT_DIRS:
        loc_path = _join(rangesat_dir, location)
        if exists(loc_path):
            _location = Location(loc_path)
            return jsonify(_location.serialized())

    return jsonify(None)


@app.route('/location/<location>/<ranch>')
@app.route('/location/<location>/<ranch>/')
def ranch(location, ranch):
    for rangesat_dir in RANGESAT_DIRS:
        loc_path = _join(rangesat_dir, location)
        if exists(loc_path):
            _location = Location(loc_path)
            return jsonify(_location.serialized_ranch(ranch))

    return jsonify(None)


@app.route('/location/<location>/<ranch>/<pasture>')
@app.route('/location/<location>/<ranch>/<pasture>/')
def pasture(location, ranch, pasture):
    for rangesat_dir in RANGESAT_DIRS:
        loc_path = _join(rangesat_dir, location)
        if exists(loc_path):
            _location = Location(loc_path)
            return jsonify(_location.serialized_pasture(ranch, pasture))

    return jsonify(None)


@app.route('/geojson/<location>')
@app.route('/geojson/<location>/')
def geojson_location(location):
    for rangesat_dir in RANGESAT_DIRS:
        loc_path = _join(rangesat_dir, location)
        if exists(loc_path):
            _location = Location(loc_path)
            return jsonify(_location.geojson_filter())

    return jsonify(None)


@app.route('/geojson/<location>/<ranch>')
@app.route('/geojson/<location>/<ranch>/')
def geojson_ranch(location, ranch):
    for rangesat_dir in RANGESAT_DIRS:
        loc_path = _join(rangesat_dir, location)
        if exists(loc_path):
            _location = Location(loc_path)
            return jsonify(_location.geojson_filter(ranch))

    return jsonify(None)


@app.route('/geojson/<location>/<ranch>/<pasture>')
@app.route('/geojson/<location>/<ranch>/<pasture>/')
def geojson_pasture(location, ranch, pasture):
    for rangesat_dir in RANGESAT_DIRS:
        loc_path = _join(rangesat_dir, location)
        if exists(loc_path):
            _location = Location(loc_path)
            return jsonify(_location.geojson_filter(ranch, pasture))

    return jsonify(None)


def _scenemeta_location_latest(location):
    for rangesat_dir in RANGESAT_DIRS:
        loc_path = _join(rangesat_dir, location)
        if exists(loc_path):
            _location = Location(loc_path)
            out_dir = _location.out_dir
            ls_fns = glob(_join(out_dir, '*/*ndvi.wgs.tif'))
            ls_fns = [_split(fn)[0] for fn in ls_fns]
            ls_fns = [_split(fn)[-1] for fn in ls_fns]
            dates = [int(fn.split('_')[3]) for fn in ls_fns]
            return ls_fns[np.argmax(dates)]


def _scene_sorter(fns):
    return sorted(fns, key=lambda fn: int(fn.split('_')[3]))


def _scenemeta_location_intrayear(location, year, start_date, end_date, rowpath=None):
    for rangesat_dir in RANGESAT_DIRS:
        loc_path = _join(rangesat_dir, location)
        if exists(loc_path):
            _location = Location(loc_path)
            out_dir = _location.out_dir
            ls_fns = glob(_join(out_dir, '*/*ndvi.wgs.tif'))
            ls_fns = [_split(fn)[0] for fn in ls_fns]
            ls_fns = [_split(fn)[-1] for fn in ls_fns]
            dates = [fn.split('_')[3] for fn in ls_fns]
            dates = [date(int(d[:4]), int(d[4:6]), int(d[6:8])) for d in dates]
            _start_date = date(*map(int, '{}-{}'.format(year, start_date).split('-')))
            _end_date = date(*map(int, '{}-{}'.format(year, end_date).split('-')))
            mask = [_start_date < d < _end_date for d in dates]

            fns = []
            for fn, m in zip(ls_fns, mask):
                if not m:
                    continue

                if rowpath is not None:
                    if rowpath != fn.split('_')[2]:
                        continue

                fns.append(fn)

            return _scene_sorter(fns)


def _scenemeta_location_interyear(location, start_year, end_year, start_date, end_date, rowpath=None):
    for rangesat_dir in RANGESAT_DIRS:
        loc_path = _join(rangesat_dir, location)
        if exists(loc_path):
            _location = Location(loc_path)
            out_dir = _location.out_dir
            ls_fns = glob(_join(out_dir, '*/*ndvi.wgs.tif'))
            ls_fns = [_split(fn)[0] for fn in ls_fns]
            ls_fns = [_split(fn)[-1] for fn in ls_fns]
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

            fns = []
            for fn, m in zip(ls_fns, mask):
                if not m:
                    continue

                if rowpath is not None:
                    if rowpath != fn.split('_')[2]:
                        continue

                fns.append(fn)

            return _scene_sorter(fns)


#@app.route('/scenemeta/<location>/latest')
#@app.route('/scenemeta/<location>/latest/')
#def scenemeta_location_latest(location):
#    return jsonify(_scenemeta_location_latest(location))


@app.route('/scenemeta/<location>')
@app.route('/scenemeta/<location>/')
def scenemeta_location(location):
    filter = request.args.get('filter', None)

    if filter == 'latest':
        return jsonify(_scenemeta_location_latest(location))
    elif filter == 'inter-year':
        start_year =request.args.get('start_year', None)
        end_year = request.args.get('end_year', None)
        start_date = request.args.get('start_date', None)
        end_date = request.args.get('end_date', None)
        return jsonify(_scenemeta_location_interyear(location, start_year, end_year, start_date, end_date))
    elif filter == 'intra-year':
        year =request.args.get('year', None)
        start_date = request.args.get('start_date', None)
        end_date = request.args.get('end_date', None)
        return jsonify(_scenemeta_location_interyear(location, year, start_date, end_date))

    for rangesat_dir in RANGESAT_DIRS:
        loc_path = _join(rangesat_dir, location)
        if exists(loc_path):
            _location = Location(loc_path)
            out_dir = _location.out_dir
            ls_fns = glob(_join(out_dir, '*/*pixel_qa.tif'))
            ls_fns = [_split(fn)[0] for fn in ls_fns]
            ls_fns = [_split(fn)[-1] for fn in ls_fns]

            return jsonify(_scene_sorter(ls_fns))

    return jsonify(None)


@app.route('/scenemeta/<location>/<product_id>')
@app.route('/scenemeta/<location>/<product_id>/')
def scenemeta_location_product_id(location, product_id):

    for rangesat_dir in RANGESAT_DIRS:
        loc_path = _join(rangesat_dir, location)
        if exists(loc_path):
            _location = Location(loc_path)
            out_dir = _location.out_dir
            ls_fn = glob(_join(out_dir, product_id))

            if len(ls_fn) != 1:
                return jsonify(None)

            ls_fn = ls_fn[0]

            ls = LandSatScene(ls_fn)
            meta = ls.summary_dict()

            return jsonify(meta)

    return jsonify(None)


@app.route('/raster/<location>/<product_id>/<product>')
@app.route('/raster/<location>/<product_id>/<product>/')
def raster(location, product_id, product):
    ranches = request.args.get('ranches', None)
    utm = request.args.get('utm', False)

    for rangesat_dir in RANGESAT_DIRS:
        loc_path = _join(rangesat_dir, location)
        if exists(loc_path):
            _location = Location(loc_path)
            out_dir = _location.out_dir

            fn = []
            if not utm:
                if product in ['ndvi', 'nbr', 'nbr2', 'pixel_qa', 'rgb', 'aerosol']:
                    fn = glob(_join(out_dir, product_id, '*{}.wgs.tif'.format(product)))

                elif product in ['biomass', 'fall_vi', 'summer_vi']:
                    fn = glob(_join(out_dir, product_id, 'biomass/*{}.wgs.tif'.format(product)))
            else:
                if product in ['ndvi', 'nbr', 'nbr2', 'pixel_qa', 'rgb', 'aerosol']:
                    fn = glob(_join(out_dir, product_id, '*{}.tif'.format(product)))

                elif product in ['biomass', 'fall_vi', 'summer_vi']:
                    fn = glob(_join(out_dir, product_id, 'biomass/*{}.tif'.format(product)))

            if len(fn) != 1:
                return jsonify(None)
            fn = fn[0]

            if not utm:
                file_name = '{product_id}__{product}.wgs.tif'.format(product_id=product_id, product=product)
            else:
                file_name = '{product_id}__{product}.tif'.format(product_id=product_id, product=product)

            if ranches is None:
                return send_file(fn, as_attachment=True, attachment_filename=file_name)

            ranches = literal_eval(ranches)

            assert exists(fn.replace('.wgs.tif', '.tif'))

            file_path = os.path.abspath(_join(SCRATCH, file_name))
            _location.mask_ranches(fn.replace('.wgs.tif', '.tif'), ranches, file_path)

            @after_this_request
            def remove_file(response):
                try:
                    os.remove(file_path)
                except Exception as error:
                    app.logger.error("Error removing or closing downloaded file handle", error)
                return response

            return send_file(file_path, as_attachment=True, attachment_filename=file_name)

    return jsonify(None)


@app.route('/raster-processing/intra-year/<location>/<product>')
@app.route('/raster-processing/intra-year/<location>/<product>/')
def intrayear_rasterprocessing(location, product):
    ranches = request.args.get('ranches', None)
    rowpath = request.args.get('rowpath', None)
    year = request.args.get('year', None)
    start_date = request.args.get('start_date', None)
    end_date = request.args.get('end_date', None)

    for rangesat_dir in RANGESAT_DIRS:
        loc_path = _join(rangesat_dir, location)
        if exists(loc_path):
            _location = Location(loc_path)
            out_dir = _location.out_dir

            product_ids = _scenemeta_location_intrayear(location, year, start_date, end_date, rowpath=rowpath)

            fns = []
            for product_id in product_ids:

                if product in ['ndvi', 'nbr', 'nbr2', 'pixel_qa']:
                    fn = glob(_join(out_dir, product_id, '*{}.tif'.format(product)))
                elif product in ['biomass', 'fall_vi', 'summer_vi']:
                    fn = glob(_join(out_dir, product_id, 'biomass/*{}.tif'.format(product)))
                else:
                    return jsonify(None)

                if len(fn) != 1:
                    continue
                fn = fn[0]
                fns.append(fn)

            if ranches is not None:
                ranches = literal_eval(ranches)

                assert exists(fn.replace('.wgs.tif', '.tif'))

                _fns = []
                for fn in _fns:
                    head, tail = _split(fn)
                    _fn = _join(SCRATCH, tail)
                    _location.mask_ranches(fn.replace('.wgs.tif', '.tif'), ranches, _fn)
                    _fns.append(_fn)

                fns = _fns

            file_name = 'agg_{}.tif'.format(product)
            file_path = os.path.abspath(_join(SCRATCH, file_name))

            @after_this_request
            def remove_file(response):
                try:
                    os.remove(file_path)
                except Exception as error:
                    app.logger.error("Error removing or closing downloaded file handle", error)

                if ranches is not None:
                    for _fn in fns:
                        if SCRATCH in _fn:
                            os.remove(_fn)

                return response

            make_aggregated_rasters(fns, dst_fn=file_path)
            return send_file(file_path, as_attachment=True, attachment_filename=file_name)

    return jsonify(None)


# https://rangesat.nkn.uidaho.edu/api/raster-processing/inter-year/Zumwalt/biomass?start_year=2016&end_year=2018&start_date=5-1&end_date=8-30
@app.route('/raster-processing/inter-year/<location>/<product>')
@app.route('/raster-processing/inter-year/<location>/<product>/')
def interyear_rasterprocessing(location, product):
    ranches = request.args.get('ranches', None)
    rowpath = request.args.get('rowpath', None)
    start_year = request.args.get('start_year', None)
    end_year = request.args.get('end_year', None)
    start_date = request.args.get('start_date', None)
    end_date = request.args.get('end_date', None)

    for rangesat_dir in RANGESAT_DIRS:
        loc_path = _join(rangesat_dir, location)
        if exists(loc_path):
            _location = Location(loc_path)
            out_dir = _location.out_dir

            product_ids = _scenemeta_location_interyear(location, start_year, end_year, start_date, end_date, rowpath=rowpath)

            fns = []
            for product_id in product_ids:

                if product in ['ndvi', 'nbr', 'nbr2', 'pixel_qa']:
                    fn = glob(_join(out_dir, product_id, '*{}.tif'.format(product)))

                elif product in ['biomass', 'fall_vi', 'summer_vi']:
                    fn = glob(_join(out_dir, product_id, 'biomass/*{}.tif'.format(product)))
                else:
                    return jsonify(None)

                if len(fn) != 1:
                    continue
                fn = fn[0]
                fns.append(fn)

            if ranches is not None:
                ranches = literal_eval(ranches)

                assert exists(fn.replace('.wgs.tif', '.tif'))

                _fns = []
                for fn in _fns:
                    head, tail = _split(fn)
                    _fn = _join(SCRATCH, tail)
                    _location.mask_ranches(fn.replace('.wgs.tif', '.tif'), ranches, _fn)
                    _fns.append(_fn)

                fns = _fns

            file_name = 'agg_{}.tif'.format(product)
            file_path = os.path.abspath(_join(SCRATCH, file_name))

            @after_this_request
            def remove_file(response):
                try:
                    os.remove(file_path)
                except Exception as error:
                    app.logger.error("Error removing or closing downloaded file handle", error)

                if ranches is not None:
                    for _fn in fns:
                        if SCRATCH in _fn:
                            os.remove(_fn)

                return response

            make_aggregated_rasters(fns, dst_fn=file_path)
            return send_file(file_path, as_attachment=True, attachment_filename=file_name)

    return jsonify(None)


@app.route('/raster-processing/difference/<location>/<product>')
@app.route('/raster-processing/difference/<location>/<product>/')
def raster_difference(location, product):
    ranches = request.args.get('ranches', None)
    utm = request.args.get('utm', False)
    product_id = request.args.get('product_id', None)
    product_id2 = request.args.get('product_id2', _scenemeta_location_latest(location))

    assert product_id is not None
    assert product_id2 is not None

    for rangesat_dir in RANGESAT_DIRS:
        loc_path = _join(rangesat_dir, location)
        if exists(loc_path):
            _location = Location(loc_path)
            out_dir = _location.out_dir

            fn = []
            if not utm:
                if product in ['ndvi', 'nbr', 'nbr2', 'pixel_qa', 'rgb', 'aerosol']:
                    fn = glob(_join(out_dir, product_id, '*{}.wgs.tif'.format(product)))

                elif product in ['biomass', 'fall_vi', 'summer_vi']:
                    fn = glob(_join(out_dir, product_id, 'biomass/*{}.wgs.tif'.format(product)))
            else:
                if product in ['ndvi', 'nbr', 'nbr2', 'pixel_qa', 'rgb', 'aerosol']:
                    fn = glob(_join(out_dir, product_id, '*{}.tif'.format(product)))

                elif product in ['biomass', 'fall_vi', 'summer_vi']:
                    fn = glob(_join(out_dir, product_id, 'biomass/*{}.tif'.format(product)))

            if len(fn) != 1:
                return jsonify(None)
            fn = fn[0]

            if ranches is not None:
                ranches = literal_eval(ranches)
                assert exists(fn.replace('.wgs.tif', '.tif'))

                _fn = os.path.abspath(_join(SCRATCH, '%s.tif' % uuid.uuid4()))
                _location.mask_ranches(fn.replace('.wgs.tif', '.tif'), ranches, _fn)
                fn = _fn

            fn2 = []
            if not utm:
                if product in ['ndvi', 'nbr', 'nbr2', 'pixel_qa', 'rgb', 'aerosol']:
                    fn2 = glob(_join(out_dir, product_id2, '*{}.wgs.tif'.format(product)))

                elif product in ['biomass', 'fall_vi', 'summer_vi']:
                    fn2 = glob(_join(out_dir, product_id2, 'biomass/*{}.wgs.tif'.format(product)))
            else:
                if product in ['ndvi', 'nbr', 'nbr2', 'pixel_qa', 'rgb', 'aerosol']:
                    fn2 = glob(_join(out_dir, product_id2, '*{}.tif'.format(product)))

                elif product in ['biomass', 'fall_vi', 'summer_vi']:
                    fn2 = glob(_join(out_dir, product_id2, 'biomass/*{}.tif'.format(product)))

            if len(fn2) != 1:
                return jsonify(None)
            fn2 = fn2[0]

            if ranches is not None:
                ranches = literal_eval(ranches)
                assert exists(fn.replace('.wgs.tif', '.tif'))

                _fn2 = os.path.abspath(_join(SCRATCH, '%s.tif' % uuid.uuid4()))
                _location.mask_ranches(fn2.replace('.wgs.tif', '.tif'), ranches, _fn2)
                fn2 = _fn2

            if not utm:
                file_name = '{product_id2}-{product_id}__{product}.wgs.tif'\
                            .format(product_id=product_id, product_id2=product_id2, product=product)
            else:
                file_name = '{product_id2}-{product_id}__{product}.tif'\
                            .format(product_id=product_id, product_id2=product_id2, product=product)

            file_path = _join(SCRATCH, file_name)

            make_raster_difference(fn, fn2, file_path)

            @after_this_request
            def remove_file(response):
                try:
                    os.remove(file_path)
                except Exception as error:
                    app.logger.error("Error removing or closing downloaded file handle", error)
                return response

            return send_file(file_path, as_attachment=True, attachment_filename=file_name)

    return jsonify(None)


def _handle_pasturestat_request(data, csv):
    if csv:
        if isinstance(data, dict):
            _data = []
            for value in data.values():
                _data.extend(value)
            data = _data

        file_name = '%s.csv' % uuid.uuid4()
        file_path = _join(SCRATCH, file_name)
        fieldnames = list(data[0].keys())

        with open(file_path, 'w') as fp:
            csv_wtr = DictWriter(fp, fieldnames=fieldnames)
            csv_wtr.writeheader()
            csv_wtr.writerows(data)

        @after_this_request
        def remove_file(response):
            try:
                os.remove(file_path)
            except Exception as error:
                app.logger.error("Error removing or closing downloaded file handle", error)
            return response

        return send_file(file_path, as_attachment=True, attachment_filename=file_name)

    else:
        return jsonify(data)


@app.route('/pasturestats/<location>')
@app.route('/pasturestats/<location>/')
def pasturestats(location):

    ranch = request.args.get('ranch', None)
    pasture = request.args.get('pasture', None)
    acquisition_date = request.args.get('acquisition_date', None)
    csv = request.args.get('csv', False)

    for rangesat_dir in RANGESAT_DIRS:
        loc_path = _join(rangesat_dir, location)
        if exists(loc_path):
            _location = Location(loc_path)
            db_fn = _location.db_fn

            data = query_pasture_stats(db_fn, ranch=ranch, pasture=pasture, acquisition_date=acquisition_date)
            return _handle_pasturestat_request(data, csv)

    return jsonify(None)


def _get_agg_func(agg_func):
    if agg_func not in ['mean', 'median', 'sum', 'count', 'max', 'min', 'std']:
        return None

    if agg_func == 'mean':
        agg_func = np.mean
    elif agg_func == 'median':
        agg_func = np.median
    elif agg_func == 'sum':
        agg_func = np.sum
    elif agg_func == 'count':
        agg_func = len
    elif agg_func == 'max':
        agg_func = np.max
    elif agg_func == 'min':
        agg_func = np.min
    elif agg_func == 'std':
        agg_func = np.std

    return agg_func


@app.route('/pasturestats/single-year/<location>')
@app.route('/pasturestats/single-year/<location>/')
def singleyear_pasturestats(location):
    ranch = request.args.get('ranch', None)
    pasture = request.args.get('pasture', None)
    year = request.args.get('year', None)
    start_date = request.args.get('start_date', None)
    end_date = request.args.get('end_date', None)
    agg_func = _get_agg_func(request.args.get('agg_func', 'mean'))
    csv = request.args.get('csv', False)

    if agg_func is None:
        return jsonify(None)

    for rangesat_dir in RANGESAT_DIRS:
        loc_path = _join(rangesat_dir, location)
        if exists(loc_path):
            _location = Location(loc_path)
            db_fn = _location.db_fn

            data = query_singleyear_pasture_stats(db_fn, ranch=ranch, pasture=pasture, year=year,
                                                  start_date=start_date, end_date=end_date, agg_func=agg_func,
                                                  key_delimiter=_location.key_delimiter)
            return _handle_pasturestat_request(data, csv)

    return jsonify(None)


@app.route('/pasturestats/intra-year/<location>')
@app.route('/pasturestats/intra-year/<location>/')
def intrayear_pasturestats(location):
    ranch = request.args.get('ranch', None)
    pasture = request.args.get('pasture', None)
    year = request.args.get('year', None)
    start_date = request.args.get('start_date', None)
    end_date = request.args.get('end_date', None)
    agg_func = _get_agg_func(request.args.get('agg_func', 'mean'))
    csv = request.args.get('csv', False)

    if agg_func is None:
        return jsonify(None)

    for rangesat_dir in RANGESAT_DIRS:
        loc_path = _join(rangesat_dir, location)
        if exists(loc_path):
            _location = Location(loc_path)
            db_fn = _location.db_fn

            data = query_intrayear_pasture_stats(db_fn, ranch=ranch, pasture=pasture, year=year,
                                                 start_date=start_date, end_date=end_date, agg_func=agg_func,
                                                 key_delimiter=_location.key_delimiter)
            return _handle_pasturestat_request(data, csv)

    return jsonify(None)


@app.route('/pasturestats/single-year-monthly/<location>')
@app.route('/pasturestats/single-year-monthly/<location>/')
def singleyearmonthly_pasturestats(location):
    ranch = request.args.get('ranch', None)
    pasture = request.args.get('pasture', None)
    year = request.args.get('year', None)
    agg_func = _get_agg_func(request.args.get('agg_func', 'mean'))
    csv = request.args.get('csv', False)

    if agg_func is None:
        return jsonify(None)

    for rangesat_dir in RANGESAT_DIRS:
        loc_path = _join(rangesat_dir, location)
        if exists(loc_path):
            _location = Location(loc_path)
            db_fn = _location.db_fn

            data = query_singleyearmonthly_pasture_stats(db_fn, ranch=ranch, pasture=pasture, year=year,
                                                         agg_func=agg_func,
                                                         key_delimiter=_location.key_delimiter)
            return _handle_pasturestat_request(data, csv)

    return jsonify(None)


@app.route('/pasturestats/seasonal-progression/<location>')
@app.route('/pasturestats/seasonal-progression/<location>/')
def seasonalprogression_pasturestats(location):
    ranch = request.args.get('ranch', None)
    pasture = request.args.get('pasture', None)
    year = request.args.get('year', None)
    agg_func = _get_agg_func(request.args.get('agg_func', 'mean'))
    csv = request.args.get('csv', False)
    csv = request.args.get('csv', False)

    if agg_func is None:
        return jsonify(None)

    for rangesat_dir in RANGESAT_DIRS:
        loc_path = _join(rangesat_dir, location)
        if exists(loc_path):
            _location = Location(loc_path)
            db_fn = _location.db_fn

            data = query_seasonalprogression_pasture_stats(db_fn, ranch=ranch, pasture=pasture,
                                                           agg_func=agg_func, key_delimiter=_location.key_delimiter)
            return _handle_pasturestat_request(data, csv)

    return jsonify(None)


@app.route('/pasturestats/inter-year/<location>')
@app.route('/pasturestats/inter-year/<location>/')
def interyear_pasturestats(location):
    ranch = request.args.get('ranch', None)
    pasture = request.args.get('pasture', None)
    start_year = request.args.get('start_year', None)
    end_year = request.args.get('end_year', None)
    start_date = request.args.get('start_date', None)
    end_date = request.args.get('end_date', None)
    agg_func = _get_agg_func(request.args.get('agg_func', 'mean'))
    csv = request.args.get('csv', False)

    if agg_func is None:
        return jsonify(None)

    for rangesat_dir in RANGESAT_DIRS:
        loc_path = _join(rangesat_dir, location)
        if exists(loc_path):
            _location = Location(loc_path)
            db_fn = _location.db_fn

            data = query_interyear_pasture_stats(db_fn, ranch=ranch, pasture=pasture,
                                                 start_year=start_year, end_year=end_year,
                                                 start_date=start_date, end_date=end_date, agg_func=agg_func,
                                                 key_delimiter=_location.key_delimiter)
            return _handle_pasturestat_request(data, csv)

    return jsonify(None)


@app.route('/pasturestats/multi-year/<location>')
@app.route('/pasturestats/multi-year/<location>/')
def multiyear_pasturestats(location):
    ranch = request.args.get('ranch', None)
    pasture = request.args.get('pasture', None)
    start_year = request.args.get('start_year', None)
    end_year = request.args.get('end_year', None)
    start_date = request.args.get('start_date', None)
    end_date = request.args.get('end_date', None)
    agg_func = _get_agg_func(request.args.get('agg_func', 'mean'))
    csv = request.args.get('csv', False)

    if agg_func is None:
        return jsonify(None)

    for rangesat_dir in RANGESAT_DIRS:
        loc_path = _join(rangesat_dir, location)
        if exists(loc_path):
            _location = Location(loc_path)
            db_fn = _location.db_fn

            data =query_multiyear_pasture_stats(db_fn, ranch=ranch, pasture=pasture,
                                                start_year=start_year, end_year=end_year,
                                                start_date=start_date, end_date=end_date, agg_func=agg_func,
                                                key_delimiter=_location.key_delimiter)
            return _handle_pasturestat_request(data, csv)

    return jsonify(None)


@app.route('/gridmet/multi-year/<location>/<ranch>/<pasture>')
@app.route('/gridmet/multi-year/<location>/<ranch>/<pasture>/')
def gridmet_allyears_pasture(location, ranch, pasture):
    start_year = request.args.get('start_year', None)
    end_year = request.args.get('end_year', None)

    for rangesat_dir in RANGESAT_DIRS:
        loc_path = _join(rangesat_dir, location)
        if exists(loc_path):
            _location = Location(loc_path)
            ranch = ranch.replace("'", "~").replace(' ', '_')
            pasture = pasture.replace("'", "~").replace(' ', '_')
            d = load_gridmet_all_years(_join(_location.loc_path, 'gridmet', ranch, pasture), start_year, end_year)
            return jsonify(d)

    return jsonify(None)


@app.route('/gridmet/single-year/<location>/<ranch>/<pasture>')
@app.route('/gridmet/single-year/<location>/<ranch>/<pasture>/')
def gridmet_singleyear_pasture(location, ranch, pasture):
    year = request.args.get('year', datetime.now().year)
    year = int(year)

    for rangesat_dir in RANGESAT_DIRS:
        loc_path = _join(rangesat_dir, location)
        if exists(loc_path):
            _location = Location(loc_path)
            ranch = ranch.replace("'", "~").replace(' ', '_')
            pasture = pasture.replace("'", "~").replace(' ', '_')
            d = load_gridmet_single_year(_join(_location.loc_path, 'gridmet', ranch, pasture), year)
            return jsonify(d)

    return jsonify(None)


@app.route('/gridmet/single-year-monthly/<location>/<ranch>/<pasture>')
@app.route('/gridmet/single-year-monthly/<location>/<ranch>/<pasture>/')
def gridmet_singleyearmonthly_pasture(location, ranch, pasture):
    year = request.args.get('year', datetime.now().year)
    year = int(year)

    for rangesat_dir in RANGESAT_DIRS:
        loc_path = _join(rangesat_dir, location)
        if exists(loc_path):
            _location = Location(loc_path)
            ranch = ranch.replace("'", "~").replace(' ', '_')
            pasture = pasture.replace("'", "~").replace(' ', '_')
            d = load_gridmet_single_year_monthly(_join(_location.loc_path, 'gridmet', ranch, pasture), year)
            return jsonify(d)

    return jsonify(None)


@app.route('/gridmet/annual-progression/<location>/<ranch>/<pasture>')
@app.route('/gridmet/annual-progression/<location>/<ranch>/<pasture>/')
def gridmet_annualprogression_pasture(location, ranch, pasture):
    start_year = request.args.get('start_year', None)
    end_year = request.args.get('end_year', None)

    for rangesat_dir in RANGESAT_DIRS:
        loc_path = _join(rangesat_dir, location)
        if exists(loc_path):
            _location = Location(loc_path)
            ranch = ranch.replace("'", "~").replace(' ', '_')
            pasture = pasture.replace("'", "~").replace(' ', '_')
            d = load_gridmet_annual_progression(_join(_location.loc_path, 'gridmet', ranch, pasture), start_year, end_year)
            return jsonify(d)

    return jsonify(None)


@app.route('/gridmet/annual-progression-monthly/<location>/<ranch>/<pasture>')
@app.route('/gridmet/annual-progression-monthly/<location>/<ranch>/<pasture>/')
def gridmet_annualprogression_monthly_pasture(location, ranch, pasture):
    start_year = request.args.get('start_year', None)
    end_year = request.args.get('end_year', None)

    for rangesat_dir in RANGESAT_DIRS:
        loc_path = _join(rangesat_dir, location)
        if exists(loc_path):
            _location = Location(loc_path)
            ranch = ranch.replace("'", "~").replace(' ', '_')
            pasture = pasture.replace("'", "~").replace(' ', '_')
            d = load_gridmet_annual_progression_monthly(_join(_location.loc_path, 'gridmet', ranch, pasture), start_year, end_year)
            return jsonify(d)

    return jsonify(None)


@app.route('/histogram/single-scene/<location>/<ranch>/<pasture>')
@app.route('/histogram/single-scene/<location>/<ranch>/<pasture>/')
def histogram_singlescene_pasture(location, ranch, pasture):
    product_id = request.args.get('product_id', _scenemeta_location_latest(location))
    product = request.args.get('product', 'biomass')

    for rangesat_dir in RANGESAT_DIRS:
        loc_path = _join(rangesat_dir, location)
        if exists(loc_path):
            _location = Location(loc_path)
            out_dir = _location.out_dir

            if product in ['ndvi', 'nbr', 'nbr2', 'pixel_qa']:
                fn = glob(_join(out_dir, product_id, '*{}.tif'.format(product)))

            elif product in ['biomass', 'fall_vi', 'summer_vi']:
                fn = glob(_join(out_dir, product_id, 'biomass/*{}.tif'.format(product)))
            else:
                return jsonify(None)

            if len(fn) != 1:
                return jsonify(None)
            fn = fn[0]

            data = _location.extract_pixels(fn, ranch, pasture)
            counts, bins = np.histogram(data)
            counts = [int(x) for x in counts]
            bins = [float(x) for x in bins]

            return jsonify(dict(counts=counts, bins=bins, product_id=product_id, product=product))

    return jsonify(None)


@app.route('/histogram/intra-year/<location>/<ranch>/<pasture>')
@app.route('/histogram/intra-year/<location>/<ranch>/<pasture>/')
def intrayear_histogram(location, ranch, pasture):
    year = request.args.get('year', None)
    start_date = request.args.get('start_date', None)
    end_date = request.args.get('end_date', None)
    product = request.args.get('product', 'biomass')

    for rangesat_dir in RANGESAT_DIRS:
        loc_path = _join(rangesat_dir, location)
        if exists(loc_path):
            _location = Location(loc_path)
            out_dir = _location.out_dir

            product_ids = _scenemeta_location_intrayear(location, year, start_date, end_date)

            data = []
            for product_id in product_ids:

                if product in ['ndvi', 'nbr', 'nbr2', 'pixel_qa']:
                    fn = glob(_join(out_dir, product_id, '*{}.tif'.format(product)))

                elif product in ['biomass', 'fall_vi', 'summer_vi']:
                    fn = glob(_join(out_dir, product_id, 'biomass/*{}.tif'.format(product)))
                else:
                    return jsonify(None)

                if len(fn) != 1:
                    continue
                fn = fn[0]

                data.append(_location.extract_pixels(fn, ranch, pasture))

            counts, bins = np.histogram(data)
            counts = [int(x) for x in counts]
            bins = [float(x) for x in bins]

            return jsonify(dict(counts=counts, bins=bins, product_ids=product_ids, product=product))

    return jsonify(None)


@app.route('/histogram/inter-year/<location>/<ranch>/<pasture>')
@app.route('/histogram/inter-year/<location>/<ranch>/<pasture>/')
def interyear_histogram(location, ranch, pasture):
    start_year = request.args.get('start_year', None)
    end_year = request.args.get('end_year', None)
    start_date = request.args.get('start_date', None)
    end_date = request.args.get('end_date', None)
    product = request.args.get('product', 'biomass')

    for rangesat_dir in RANGESAT_DIRS:
        loc_path = _join(rangesat_dir, location)
        if exists(loc_path):
            _location = Location(loc_path)
            out_dir = _location.out_dir

            product_ids = _scenemeta_location_interyear(location, start_year, end_year, start_date, end_date)

            data = []
            for product_id in product_ids:

                if product in ['ndvi', 'nbr', 'nbr2', 'pixel_qa']:
                    fn = glob(_join(out_dir, product_id, '*{}.tif'.format(product)))

                elif product in ['biomass', 'fall_vi', 'summer_vi']:
                    fn = glob(_join(out_dir, product_id, 'biomass/*{}.tif'.format(product)))
                else:
                    return jsonify(None)

                if len(fn) != 1:
                    continue
                fn = fn[0]

                data.append(_location.extract_pixels(fn, ranch, pasture))

            counts, bins = np.histogram(data)
            counts = [int(x) for x in counts]
            bins = [float(x) for x in bins]

            return jsonify(dict(counts=counts, bins=bins, product_ids=product_ids, product=product))

    return jsonify(None)


if __name__ == '__main__':
    app.run(debug=True)
# rsync -av --progress --exclude temp rangesat-biomass/ rogerlew@rangesat.nkn.uidaho.edu:/var/www/rangesat-biomass
# rsync -av --progress geodata/rangesat/ rogerlew@rangesat.nkn.uidaho.edu:/geodata/rangesat
