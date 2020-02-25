#!/usr/bin/python3

from os.path import join as _join
from os.path import split as _split

from os.path import isdir, exists

from flask import Flask, jsonify, send_file, request, after_this_request
from glob import glob
from datetime import datetime
import os
import uuid
import numpy as np
from ast import literal_eval
from csv import DictWriter

from subprocess import Popen

import fiona
import rasterio
from rasterio.mask import raster_geometry_mask

from all_your_base import SCRATCH, RANGESAT_DIRS

from database import Location
from database.pasturestats import (
    query_scenes_coverage,
    query_pasture_stats,
    query_singleyear_pasture_stats,
    query_intrayear_pasture_stats,
    query_max_pasture_seasonal_pasture_stats,
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

from database.scenemeta import (
    scenemeta_location_all,
    scenemeta_location_latest,
    scenemeta_location_closest_date,
    scenemeta_location_interyear,
    scenemeta_location_intrayear
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
                'raster-processing/max-pasture-seasonal/<location>/<product:{ndvi,nbr,nbr2,biomass,fall_vi,summer_vi}>?year=<year>&start_date=<start_date>&end_date=<end_date>',
                'raster-processing/inter-year/<location>/<product:{ndvi,nbr,nbr2,biomass,fall_vi,summer_vi}>?start_year=<start_year>&end_year=<end_year>&start_date=<start_date>&end_date=<end_date>'],
            pasturestats=[
                'pasturestats/<location>/?ranch=<ranch>&pasture=<pasture>&acquisition_date=<acquisition_date>',
                'pasturestats/single-year/<location>/?ranch=<ranch>&pasture=<pasture>&year=<year>&start_date=<start_date>&end_date=<end_date>',
                'pasturestats/single-year-monthly/<location>/?ranch=<ranch>&pasture=<pasture>&year=<year>&start_date=<start_date>&end_date=<end_date>',
                'pasturestats/intra-year/<location>/?ranch=<ranch>&pasture=<pasture>&year=<year>&start_date=<start_date>&end_date=<end_date>',
                'pasturestats/max-pasture-seasonal/<location>/?ranch=<ranch>&pasture=<pasture>&year=<year>&start_date=<start_date>&end_date=<end_date>',
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



#@app.route('/scenemeta/<location>/latest')
#@app.route('/scenemeta/<location>/latest/')
#def scenemeta_location_latest(location):
#    return jsonify(_scenemeta_location_latest(location))


@app.route('/scenemeta/<location>')
@app.route('/scenemeta/<location>/')
def scenemeta_location(location):
    filter = request.args.get('filter', None)
    rowpath = request.args.get('rowpath', None)
    ls8_only = bool(request.args.get('ls8_only', False))

    if location.lower() == 'zumwalt' and rowpath is None:
        rowpath = '042028 043028'

    if rowpath == '*':
        rowpath = None

    pasture_coverage_threshold = request.args.get('pasture_coverage_threshold', None)
    if pasture_coverage_threshold is not None:
        pasture_coverage_threshold = float(pasture_coverage_threshold)

    if filter == 'latest':
        return jsonify(scenemeta_location_latest(location, rowpath=rowpath,
                                                 pasture_coverage_threshold=pasture_coverage_threshold,
                                                 ls8_only=ls8_only))
    elif filter == 'closest-date':
        target_date = request.args.get('target_date', None)
        return jsonify(scenemeta_location_closest_date(location, target_date=target_date,
                                                       rowpath=rowpath,
                                                       pasture_coverage_threshold=pasture_coverage_threshold,
                                                       ls8_only=ls8_only))
    elif filter == 'inter-year':
        start_year =request.args.get('start_year', None)
        end_year = request.args.get('end_year', None)
        start_date = request.args.get('start_date', '1-1')
        end_date = request.args.get('end_date', '12-31')
        return jsonify(scenemeta_location_interyear(location, start_year, end_year, start_date, end_date,
                                                    rowpath=rowpath,
                                                    pasture_coverage_threshold=pasture_coverage_threshold,
                                                    ls8_only=ls8_only))
    elif filter == 'intra-year':
        year =request.args.get('year', None)
        start_date = request.args.get('start_date', '1-1')
        end_date = request.args.get('end_date', '12-31')
        return jsonify(scenemeta_location_intrayear(location, year, start_date, end_date,
                                                    rowpath=rowpath,
                                                    pasture_coverage_threshold=pasture_coverage_threshold,
                                                    ls8_only=ls8_only))

    else:
        return jsonify(scenemeta_location_all(location,
                                              rowpath=rowpath,
                                              pasture_coverage_threshold=pasture_coverage_threshold,
                                              ls8_only=ls8_only))


@app.route('/scenemeta/<location>/<product_id>')
@app.route('/scenemeta/<location>/<product_id>/')
def scenemeta_location_product_id(location, product_id):

    for rangesat_dir in RANGESAT_DIRS:
        loc_path = _join(rangesat_dir, location)
        if exists(loc_path):
            _location = Location(loc_path)
            out_dir = _location.out_dir
            scn_cov_db_fn = location.scn_cov_db_fn
            ls_fn = glob(_join(out_dir, product_id))

            if len(ls_fn) != 1:
                return jsonify(None)

            ls_fn = ls_fn[0]

            ls = LandSatScene(ls_fn)
            meta = ls.summary_dict()
            meta['pasture_coverage_fraction'] = query_scenes_coverage(scn_cov_db_fn, [ls_fn])[0]
            return jsonify(meta)

    return jsonify(None)


@app.route('/raster/<location>/<product_id>/<product>')
@app.route('/raster/<location>/<product_id>/<product>/')
def raster(location, product_id, product):
    ranches = request.args.get('ranches', None)
    utm = request.args.get('utm', False)

    if product.lower() == 'ndti':
        product = 'nbr2'

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

@app.route('/raster-processing/max-pasture-seasonal/<location>/biomass')
@app.route('/raster-processing/max-pasture-seasonal/<location>/biomass/')
def max_seasonal_pasture_rasterprocessing(location):
    ranch = request.args.get('ranch', None)
    pasture = request.args.get('pasture', None)
    ls8_only = bool(request.args.get('ls8_only', False))
    year = request.args.get('year', None)
    start_date = request.args.get('start_date', '1-1')
    end_date = request.args.get('end_date', '12-31')
    measure = request.args.get('measure', 'biomass_mean_gpm')
    rowpath = request.args.get('', None)
    utm = request.args.get('utm', False)

    if location.lower() == 'zumwalt' and rowpath is None:
        rowpath = '042028'

    if rowpath == '*':
        rowpath = None

    file_name = 'pasturestats-max-pasture-seasonal_ranch={ranch},pasture={pasture},'\
                'year={year},start_date={start_date},end_date={end_date},measure={measure},'\
                'rowpath={rowpath},ls8_only={ls8_only},utm={utm}.tif'\
                .format(ranch=ranch, pasture=pasture,
                        year=year, start_date=start_date, end_date=end_date, measure=measure,
                        rowpath=rowpath, ls8_only=ls8_only, utm=utm)

    file_path = os.path.abspath(_join(SCRATCH, file_name))
    nodata = -9999.0
    ds = None
    _data = None

    for rangesat_dir in RANGESAT_DIRS:
        loc_path = _join(rangesat_dir, location)
        if exists(loc_path):
            _location = Location(loc_path)
            out_dir = _location.out_dir
            db_fn = _location.db_fn

            data = query_max_pasture_seasonal_pasture_stats(db_fn, ranch=ranch, pasture=pasture, year=year,
                                                 start_date=start_date, end_date=end_date, measure=measure,
                                                 key_delimiter=_location.key_delimiter)

            for i, d in enumerate(data):
                ranch = d['ranch']
                pasture = d['pasture']
                acquisition_date = d['acquisition_date']

                product_id = scenemeta_location_closest_date(location, acquisition_date, rowpath=rowpath, ls8_only=ls8_only)
                raster_fn = glob(_join(out_dir, product_id, 'biomass/*biomass.tif'))[0]
                indx = _location.get_pasture_indx(raster_fn, pasture, ranch)

                ds = rasterio.open(raster_fn)
                biomass = ds.read()
                if len(biomass.shape) == 3:
                    biomass = biomass[0, :, :]

                if i == 0:
                    _biomass = np.zeros(biomass.shape)

                assert _biomass.shape == biomass.shape, (_biomass.shape, biomass.shape)
                _biomass[indx] = biomass[indx]

            with rasterio.Env():
                profile = ds.profile
                profile.update(
                    dtype=rasterio.float32,
                    count=1,
                    nodata=nodata,
                    compress='lzw')

                with rasterio.open(file_path, 'w', **profile) as dst:
                    dst.write(_biomass.astype(rasterio.float32), 1)

            utm_dst_fn = file_path
            dst_fn = file_path.replace('.tif', '.wrs.tif')

            if not utm:
                try:
                    dst_vrt_fn = dst_fn.replace('.tif', '.vrt')

                    if exists(dst_vrt_fn):
                        os.remove(dst_vrt_fn)

                    cmd = ['gdalwarp', '-t_srs', 'EPSG:4326', '-of', 'vrt', utm_dst_fn, dst_vrt_fn]
                    p = Popen(cmd)
                    p.wait()

                    assert exists(dst_vrt_fn)
                except:
                    if exists(dst_vrt_fn):
                        os.remove(dst_vrt_fn)
                    raise

                try:
                    if exists(dst_fn):
                        os.remove(dst_fn)

                    cmd = ['gdal_translate', '-co', 'COMPRESS=LZW', '-of', 'GTiff', dst_vrt_fn, dst_fn]
                    p = Popen(cmd)
                    p.wait()

                    assert exists(dst_fn)
                except:
                    if exists(dst_fn):
                        os.remove(dst_fn)
                    raise

            @after_this_request
            def remove_file(response):
                try:
                    if exists(utm_dst_fn):
                        os.remove(utm_dst_fn)
                    if exists(dst_fn):
                        os.remove(dst_fn)
                    if exists(dst_vrt_fn):
                        os.remove(dst_vrt_fn)
                except Exception as error:
                    app.logger.error("Error removing or closing downloaded file handle", error)
                return response

            if utm:
                return send_file(utm_dst_fn, as_attachment=True, attachment_filename=file_name)
            else:
                return send_file(dst_fn, as_attachment=True, attachment_filename=file_name)


@app.route('/raster-processing/intra-year/<location>/<product>')
@app.route('/raster-processing/intra-year/<location>/<product>/')
def intrayear_rasterprocessing(location, product):
    ranches = request.args.get('ranches', None)
    year = request.args.get('year', None)
    start_date = request.args.get('start_date', '1-1')
    end_date = request.args.get('end_date', '12-31')
    rowpath = request.args.get('rowpath', None)
    if location.lower() == 'zumwalt' and rowpath is None:
        rowpath = '042028'

    if rowpath == '*':
        rowpath = None

    for rangesat_dir in RANGESAT_DIRS:
        loc_path = _join(rangesat_dir, location)
        if exists(loc_path):
            _location = Location(loc_path)
            out_dir = _location.out_dir

            product_ids = scenemeta_location_intrayear(location, year, start_date, end_date, rowpath=rowpath)

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
    start_year = request.args.get('start_year', None)
    end_year = request.args.get('end_year', None)
    start_date = request.args.get('start_date', '1-1')
    end_date = request.args.get('end_date', '12-31')
    rowpath = request.args.get('rowpath', None)
    if location.lower() == 'zumwalt' and rowpath is None:
        rowpath = '042028'

    if rowpath == '*':
        rowpath = None

    for rangesat_dir in RANGESAT_DIRS:
        loc_path = _join(rangesat_dir, location)
        if exists(loc_path):
            _location = Location(loc_path)
            out_dir = _location.out_dir

            product_ids = scenemeta_location_interyear(location, start_year, end_year, start_date, end_date, rowpath=rowpath)

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
    product_id2 = request.args.get('product_id2', scenemeta_location_latest(location))

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
                return jsonify('fn is none', product)

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
                return jsonify('fn2 is none', fn2, product, product_id2)
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
    if csv is not None:
        if isinstance(data, dict):
            _data = []
            for value in data.values():
                _data.extend(value)
            data = _data

        file_name = '%s.csv' % csv
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
    csv = request.args.get('csv', None)

    if csv is not None:
        csv = 'pasturestats_ranch={ranch},pasture={pasture},acquisition_date={acquisition_date}'\
              .format(ranch=ranch, pasture=pasture, acquisition_date=acquisition_date)

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
    start_date = request.args.get('start_date', '1-1')
    end_date = request.args.get('end_date', '12-31')
    agg_func = _get_agg_func(request.args.get('agg_func', 'mean'))
    csv = request.args.get('csv', None)

    if csv is not None:
        csv = 'pasturestats-single-year_ranch={ranch},pasture={pasture},'\
              'year={year},start_date={start_date},end_date={end_date},agg_func={agg_func}'\
              .format(ranch=ranch, pasture=pasture,
                      year=year, start_date=start_date, end_date=end_date, agg_func=agg_func.__name__)

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
    start_date = request.args.get('start_date', '1-1')
    end_date = request.args.get('end_date', '12-31')
    agg_func = _get_agg_func(request.args.get('agg_func', 'mean'))
    csv = request.args.get('csv', None)

    if csv is not None:
        csv = 'pasturestats-intra-year_ranch={ranch},pasture={pasture},'\
              'year={year},start_date={start_date},end_date={end_date},agg_func={agg_func}'\
              .format(ranch=ranch, pasture=pasture,
                      year=year, start_date=start_date, end_date=end_date, agg_func=agg_func.__name__)

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


@app.route('/pasturestats/max-pasture-seasonal/<location>')
@app.route('/pasturestats/max-pasture-seasonal/<location>/')
def max_seasonal_pasturestats(location):
    ranch = request.args.get('ranch', None)
    pasture = request.args.get('pasture', None)
    year = request.args.get('year', None)
    start_date = request.args.get('start_date', '1-1')
    end_date = request.args.get('end_date', '12-31')
    measure = request.args.get('measure', 'biomass_mean_gpm')
    csv = request.args.get('csv', None)

    if csv is not None:
        csv = 'pasturestats-max-pasture-seasonal_ranch={ranch},pasture={pasture},'\
              'year={year},start_date={start_date},end_date={end_date},measure={measure}'\
              .format(ranch=ranch, pasture=pasture,
                      year=year, start_date=start_date, end_date=end_date, measure=measure)

    for rangesat_dir in RANGESAT_DIRS:
        loc_path = _join(rangesat_dir, location)
        if exists(loc_path):
            _location = Location(loc_path)
            db_fn = _location.db_fn

            data = query_max_pasture_seasonal_pasture_stats(db_fn, ranch=ranch, pasture=pasture, year=year,
                                                 start_date=start_date, end_date=end_date, measure=measure,
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
    csv = request.args.get('csv', None)

    if csv is not None:
        csv = 'pasturestats-intra-year_ranch={ranch},pasture={pasture},'\
              'year={year},agg_func={agg_func}'\
              .format(ranch=ranch, pasture=pasture,
                      year=year, agg_func=agg_func.__name__)

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
    start_year = request.args.get('start_year', None)
    end_year = request.args.get('end_year', None)
    agg_func = _get_agg_func(request.args.get('agg_func', 'mean'))
    csv = request.args.get('csv', None)

    if csv is not None:
        csv = 'pasturestats-seasonal-progression_ranch={ranch},pasture={pasture},'\
              'start_year={start_year},end_year={end_year},agg_func={agg_func}'\
              .format(ranch=ranch, pasture=pasture,
                      start_year=start_year, end_year=end_year,
                      agg_func=agg_func.__name__)

    if agg_func is None:
        return jsonify(None)

    for rangesat_dir in RANGESAT_DIRS:
        loc_path = _join(rangesat_dir, location)
        if exists(loc_path):
            _location = Location(loc_path)
            db_fn = _location.db_fn

            data = query_seasonalprogression_pasture_stats(db_fn, ranch=ranch, pasture=pasture,
                                                           start_year=start_year, end_year=end_year,
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
    start_date = request.args.get('start_date', '1-1')
    end_date = request.args.get('end_date', '12-31')
    agg_func = _get_agg_func(request.args.get('agg_func', 'mean'))
    csv = request.args.get('csv', None)

    if csv is not None:
        csv = 'pasturestats-inter-year_ranch={ranch},pasture={pasture},'\
              'start_year={start_year},end_year={end_year},start_date={start_date},end_date={end_date},agg_func={agg_func}'\
              .format(ranch=ranch, pasture=pasture,
                      start_year=start_year, end_year=end_year,
                      start_date=start_date, end_date=end_date, agg_func=agg_func.__name__)

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
    start_date = request.args.get('start_date', '1-1')
    end_date = request.args.get('end_date', '12-31')
    agg_func = _get_agg_func(request.args.get('agg_func', 'mean'))
    csv = request.args.get('csv', None)

    if csv is not None:
        csv = 'pasturestats-multi-year_ranch={ranch},pasture={pasture},'\
              'start_year={start_year},end_year={end_year},start_date={start_date},end_date={end_date},agg_func={agg_func}'\
              .format(ranch=ranch, pasture=pasture,
                      start_year=start_year, end_year=end_year,
                      start_date=start_date, end_date=end_date, agg_func=agg_func.__name__)

    if agg_func is None:
        return jsonify(None)

    for rangesat_dir in RANGESAT_DIRS:
        loc_path = _join(rangesat_dir, location)
        if exists(loc_path):
            _location = Location(loc_path)
            db_fn = _location.db_fn

            data = query_multiyear_pasture_stats(db_fn, ranch=ranch, pasture=pasture,
                                                 start_year=start_year, end_year=end_year,
                                                 start_date=start_date, end_date=end_date, agg_func=agg_func,
                                                 key_delimiter=_location.key_delimiter)
            return _handle_pasturestat_request(data, csv)

    return jsonify(None)


def _process_gridmet_data(data, units):
    if 'pdsi' in data:
        del data['pdsi']

    for k in list(data.keys()):
        if data[k] is None:
            del data[k]

    if units.lower().startswith('e'):
        if 'pr' in data:
            data['pr (in)'] = data['pr']
            del data['pr']
        if 'cum_pr' in data:
            data['cum_pr (in)'] = data['cum_pr']
            del data['cum_pr']
        if 'pet' in data:
            data['pet (in)'] = data['pet']
            del data['pet']
        if 'pwd' in data:
            data['pwd (in)'] = data['pwd']
            del data['pwd']
        if 'tmmn' in data:
            data['tmmn (F)'] = data['tmmn']
            del data['tmmn']
        if 'tmmx' in data:
            data['tmmx (F)'] = data['tmmx']
            del data['tmmx']
    else:
        if 'pr' in data:
            data['pr (mm)'] = data['pr']
            del data['pr']
        if 'cum_pr' in data:
            data['cum_pr (mm)'] = data['cum_pr']
            del data['cum_pr']
        if 'pet' in data:
            data['pet (mm)'] = data['pet']
            del data['pet']
        if 'pwd' in data:
            data['pwd (mm)'] = data['pwd']
            del data['pwd']
        if 'tmmn' in data:
            data['tmmn (C)'] = data['tmmn']
            del data['tmmn']
        if 'tmmx' in data:
            data['tmmx (C)'] = data['tmmx']
            del data['tmmx']

    if 'months' in data:
        data['month'] = data['months']
        del data['months']


def _handle_gridmet_request(data, csv, units):
    if csv is not None:
        year = None

        if 'year' in data:
            year = data['year']
            del data['year']

        _process_gridmet_data(data, units)

        _data = [dict(zip(data, i)) for i in zip(*data.values())]

        if year is not None:
            for i in range(len(_data)):
                _data[i]['year'] = year

        file_name = '%s.csv' % csv
        file_path = _join(SCRATCH, file_name)
        fieldnames = list(_data[0].keys())

        with open(file_path, 'w') as fp:
            csv_wtr = DictWriter(fp, fieldnames=fieldnames)
            csv_wtr.writeheader()
            csv_wtr.writerows(_data)

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


def _handle_gridmet_multiyear_request(ddata, csv, units):
    file_name = '%s.csv' % csv
    file_path = _join(SCRATCH, file_name)
    fp = None

    if csv is not None:

        for year in ddata:
            data = ddata[year]

            _process_gridmet_data(data, units)

            _data = [dict(zip(data, i)) for i in zip(*data.values())]

            if fp is None:
                fieldnames = list(_data[0].keys())
                fp = open(file_path, 'w')
                csv_wtr = DictWriter(fp, fieldnames=fieldnames)
                csv_wtr.writeheader()

            csv_wtr.writerows(_data)

        fp.close()

        @after_this_request
        def remove_file(response):
            try:
                os.remove(file_path)
            except Exception as error:
                app.logger.error("Error removing or closing downloaded file handle", error)
            return response

        return send_file(file_path, as_attachment=True, attachment_filename=file_name)

    else:
        return jsonify(ddata)


@app.route('/gridmet/multi-year/<location>/<ranch>/<pasture>')
@app.route('/gridmet/multi-year/<location>/<ranch>/<pasture>/')
def gridmet_allyears_pasture(location, ranch, pasture):
    start_year = request.args.get('start_year', None)
    end_year = request.args.get('end_year', None)
    units = request.args.get('units', 'SI')
    csv = request.args.get('csv', None)

    if csv is not None:
        csv = 'gridmet-multi-year_ranch={ranch},pasture={pasture},'\
              'start_year={start_year},end_year={end_year},units={units}'\
              .format(ranch=ranch, pasture=pasture,
                      start_year=start_year, end_year=end_year, units=units)

    for rangesat_dir in RANGESAT_DIRS:
        loc_path = _join(rangesat_dir, location)
        if exists(loc_path):
            _location = Location(loc_path)
            ranch = ranch.replace("'", "~").replace(' ', '_')
            pasture = pasture.replace("'", "~").replace(' ', '_')
            d = load_gridmet_all_years(_join(_location.loc_path, 'gridmet', ranch, pasture),
                                       start_year, end_year, units)
            return _handle_gridmet_multiyear_request(d, csv, units)

    return jsonify(None)


@app.route('/gridmet/single-year/<location>/<ranch>/<pasture>')
@app.route('/gridmet/single-year/<location>/<ranch>/<pasture>/')
def gridmet_singleyear_pasture(location, ranch, pasture):
    year = request.args.get('year', datetime.now().year)
    year = int(year)
    units = request.args.get('units', 'SI')
    csv = request.args.get('csv', None)

    if csv is not None:
        csv = 'gridmet-single-year_ranch={ranch},pasture={pasture},'\
              'year={year},units={units}'\
              .format(ranch=ranch, pasture=pasture,
                      year=year, units=units)

    for rangesat_dir in RANGESAT_DIRS:
        loc_path = _join(rangesat_dir, location)
        if exists(loc_path):
            _location = Location(loc_path)
            ranch = ranch.replace("'", "~").replace(' ', '_')
            pasture = pasture.replace("'", "~").replace(' ', '_')
            d = load_gridmet_single_year(_join(_location.loc_path, 'gridmet', ranch, pasture),
                                         year, units)

            return _handle_gridmet_request(d, csv, units)

    return jsonify(None)


@app.route('/gridmet/single-year-monthly/<location>/<ranch>/<pasture>')
@app.route('/gridmet/single-year-monthly/<location>/<ranch>/<pasture>/')
def gridmet_singleyearmonthly_pasture(location, ranch, pasture):
    year = request.args.get('year', datetime.now().year)
    year = int(year)
    units = request.args.get('units', 'SI')
    csv = request.args.get('csv', None)

    if csv is not None:
        csv = 'gridmet-single-year-monthly={ranch},pasture={pasture},'\
              'year={year},units={units}'\
              .format(ranch=ranch, pasture=pasture,
                      year=year, units=units)

    for rangesat_dir in RANGESAT_DIRS:
        loc_path = _join(rangesat_dir, location)
        if exists(loc_path):
            _location = Location(loc_path)
            ranch = ranch.replace("'", "~").replace(' ', '_')
            pasture = pasture.replace("'", "~").replace(' ', '_')
            d = load_gridmet_single_year_monthly(_join(_location.loc_path, 'gridmet', ranch, pasture),
                                                 year, units)
            return _handle_gridmet_request(d, csv, units)

    return jsonify(None)


@app.route('/gridmet/annual-progression/<location>/<ranch>/<pasture>')
@app.route('/gridmet/annual-progression/<location>/<ranch>/<pasture>/')
def gridmet_annualprogression_pasture(location, ranch, pasture):
    start_year = request.args.get('start_year', None)
    end_year = request.args.get('end_year', None)
    units = request.args.get('units', 'SI')
    csv = request.args.get('csv', None)

    if csv is not None:
        csv = 'gridmet-annual-progression={ranch},pasture={pasture},'\
              'start_year={start_year},end_year={end_year},units={units}'\
              .format(ranch=ranch, pasture=pasture,
                      start_year=start_year, end_year=end_year, units=units)

    for rangesat_dir in RANGESAT_DIRS:
        loc_path = _join(rangesat_dir, location)
        if exists(loc_path):
            _location = Location(loc_path)
            ranch = ranch.replace("'", "~").replace(' ', '_')
            pasture = pasture.replace("'", "~").replace(' ', '_')
            d = load_gridmet_annual_progression(_join(_location.loc_path, 'gridmet', ranch, pasture),
                                                start_year, end_year, units)
            return _handle_gridmet_request(d, csv, units)

    return jsonify(None)


@app.route('/gridmet/annual-progression-monthly/<location>/<ranch>/<pasture>')
@app.route('/gridmet/annual-progression-monthly/<location>/<ranch>/<pasture>/')
def gridmet_annualprogression_monthly_pasture(location, ranch, pasture):
    start_year = request.args.get('start_year', None)
    end_year = request.args.get('end_year', None)
    units = request.args.get('units', 'SI')
    csv = request.args.get('csv', None)

    if csv is not None:
        csv = 'gridmet-annual-progression-monthly={ranch},pasture={pasture},'\
              'start_year={start_year},end_year={end_year},units={units}'\
              .format(ranch=ranch, pasture=pasture,
                      start_year=start_year, end_year=end_year, units=units)

    for rangesat_dir in RANGESAT_DIRS:
        loc_path = _join(rangesat_dir, location)
        if exists(loc_path):
            _location = Location(loc_path)
            ranch = ranch.replace("'", "~").replace(' ', '_')
            pasture = pasture.replace("'", "~").replace(' ', '_')
            d = load_gridmet_annual_progression_monthly(_join(_location.loc_path, 'gridmet', ranch, pasture),
                                                        start_year, end_year, units)
            return _handle_gridmet_request(d, csv, units)

    return jsonify(None)


@app.route('/histogram/single-scene/<location>/<ranch>/<pasture>')
@app.route('/histogram/single-scene/<location>/<ranch>/<pasture>/')
def histogram_singlescene_pasture(location, ranch, pasture):
    product_id = request.args.get('product_id', scenemeta_location_latest(location))
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
    start_date = request.args.get('start_date', '1-1')
    end_date = request.args.get('end_date', '12-31')
    product = request.args.get('product', 'biomass')

    for rangesat_dir in RANGESAT_DIRS:
        loc_path = _join(rangesat_dir, location)
        if exists(loc_path):
            _location = Location(loc_path)
            out_dir = _location.out_dir

            product_ids = scenemeta_location_intrayear(location, year, start_date, end_date)

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
    start_date = request.args.get('start_date', '1-1')
    end_date = request.args.get('end_date', '12-31')
    product = request.args.get('product', 'biomass')

    for rangesat_dir in RANGESAT_DIRS:
        loc_path = _join(rangesat_dir, location)
        if exists(loc_path):
            _location = Location(loc_path)
            out_dir = _location.out_dir

            product_ids = scenemeta_location_interyear(location, start_year, end_year, start_date, end_date)

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
