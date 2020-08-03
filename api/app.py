#!/usr/bin/python3

from os.path import join as _join
from os.path import split as _split

from os.path import isdir, exists

from flask import Flask, jsonify, send_file, request, after_this_request
from glob import glob
from datetime import datetime
import os
import traceback
import uuid
import numpy as np
from ast import literal_eval
from csv import DictWriter

from subprocess import Popen

import fiona
import rasterio
from rasterio.mask import raster_geometry_mask

from all_your_base import SCRATCH, RANGESAT_DIRS, isfloat

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
    query_multiyear_pasture_stats,
    query_singleyearmonthly_ranch_stats,
    query_seasonalprogression_ranch_stats,
    query_multiyear_ranch_stats
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


def exception_factory(msg='Error Handling Request',
                      stacktrace=None):
    if stacktrace is None:
        stacktrace = traceback.format_exc()

    return jsonify({'Success': False,
                    'Error': msg,
                    'StackTrace': stacktrace})


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
    try:
        queryset = []
        for rangesat_dir in RANGESAT_DIRS:
            queryset.extend(glob('{}/*'.format(rangesat_dir)))
        queryset = [_split(loc)[-1] for loc in queryset if isdir(loc)]
        return jsonify(dict(locations=queryset))

    except Exception:
        return exception_factory()


@app.route('/location/<location>')
@app.route('/location/<location>/')
def location(location):
    try:
        for rangesat_dir in RANGESAT_DIRS:
            loc_path = _join(rangesat_dir, location)
            if exists(loc_path):
                _location = Location(loc_path)
                return jsonify(_location.serialized())

        return jsonify(None)

    except:
        return exception_factory()


@app.route('/location/<location>/<ranch>')
@app.route('/location/<location>/<ranch>/')
def ranch(location, ranch):
    try:
        for rangesat_dir in RANGESAT_DIRS:
            loc_path = _join(rangesat_dir, location)
            if exists(loc_path):
                _location = Location(loc_path)
                return jsonify(_location.serialized_ranch(ranch))

        return jsonify(None)

    except Exception:
        return exception_factory()


@app.route('/location/<location>/<ranch>/<pasture>')
@app.route('/location/<location>/<ranch>/<pasture>/')
def pasture(location, ranch, pasture):
    try:
        for rangesat_dir in RANGESAT_DIRS:
            loc_path = _join(rangesat_dir, location)
            if exists(loc_path):
                _location = Location(loc_path)
                return jsonify(_location.serialized_pasture(ranch, pasture))

        return jsonify(None)

    except Exception:
        return exception_factory()


@app.route('/geojson/<location>')
@app.route('/geojson/<location>/')
def geojson_location(location):
    try:
        for rangesat_dir in RANGESAT_DIRS:
            loc_path = _join(rangesat_dir, location)
            if exists(loc_path):
                _location = Location(loc_path)
                return jsonify(_location.geojson_filter())

        return jsonify(None)

    except Exception:
        return exception_factory()


@app.route('/geojson/<location>/<ranch>')
@app.route('/geojson/<location>/<ranch>/')
def geojson_ranch(location, ranch):
    try:
        for rangesat_dir in RANGESAT_DIRS:
            loc_path = _join(rangesat_dir, location)
            if exists(loc_path):
                _location = Location(loc_path)
                return jsonify(_location.geojson_filter(ranch))

        return jsonify(None)

    except Exception:
        return exception_factory()


@app.route('/geojson/<location>/<ranch>/<pasture>')
@app.route('/geojson/<location>/<ranch>/<pasture>/')
def geojson_pasture(location, ranch, pasture):
    try:
        for rangesat_dir in RANGESAT_DIRS:
            loc_path = _join(rangesat_dir, location)
            if exists(loc_path):
                _location = Location(loc_path)
                return jsonify(_location.geojson_filter(ranch, pasture))

        return jsonify(None)

    except Exception:
        return exception_factory()



#@app.route('/scenemeta/<location>/latest')
#@app.route('/scenemeta/<location>/latest/')
#def scenemeta_location_latest(location):
#    return jsonify(_scenemeta_location_latest(location))


@app.route('/scenemeta/<location>')
@app.route('/scenemeta/<location>/')
def scenemeta_location(location):
    try:
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

    except Exception:
        return exception_factory()


@app.route('/scenemeta/<location>/<product_id>')
@app.route('/scenemeta/<location>/<product_id>/')
def scenemeta_location_product_id(location, product_id):
    try:

        for rangesat_dir in RANGESAT_DIRS:
            loc_path = _join(rangesat_dir, location)
            if exists(loc_path):
                _location = Location(loc_path)
                out_dir = _location.out_dir
                scn_cov_db_fn = _location.scn_cov_db_fn
                ls_fn = glob(_join(out_dir, product_id))

                if len(ls_fn) != 1:
                    return jsonify(None)

                ls_fn = ls_fn[0]

                ls = LandSatScene(ls_fn)
                meta = ls.summary_dict()
                meta['pasture_coverage_fraction'] = query_scenes_coverage(scn_cov_db_fn, [ls_fn])[0]
                return jsonify(meta)

        return jsonify(None)

    except Exception:
        return exception_factory()


@app.route('/raster/<location>/<product_id>/<product>')
@app.route('/raster/<location>/<product_id>/<product>/')
def raster(location, product_id, product):
    try:
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

    except Exception:
        return exception_factory()


@app.route('/raster-processing/max-pasture-seasonal/<location>/biomass')
@app.route('/raster-processing/max-pasture-seasonal/<location>/biomass/')
def max_seasonal_pasture_rasterprocessing(location):
    try:
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

    except Exception:
        return exception_factory()


@app.route('/raster-processing/intra-year/<location>/<product>')
@app.route('/raster-processing/intra-year/<location>/<product>/')
def intrayear_rasterprocessing(location, product):
    try:
        ranches = request.args.get('ranches', None)
        utm = request.args.get('utm', False)
        year = request.args.get('year', None)
        start_date = request.args.get('start_date', '1-1')
        end_date = request.args.get('end_date', '12-31')
        rowpath = request.args.get('rowpath', None)

        if ranches is not None:
            ranches = literal_eval(ranches)

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

                    fn = None
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
                        continue
                    fn = fn[0]

                    assert exists(fn.replace('.wgs.tif', '.tif'))

                    if ranches is None:
                        fns.append(fn)
                    else:
                        head, tail = _split(fn)
                        _fn = _join(SCRATCH, '%s_%s' %(product_id, tail))
                        _location.mask_ranches(fn.replace('.wgs.tif', '.tif'), ranches, _fn)
                        fns.append(_fn)

                file_name = 'agg_{}.tif'.format(product)
                if ranches is not None:
                    file_name = '%s_%s' % (','.join(ranches), file_name)
                if not utm:
                    file_name = file_name.replace('.tif', '.wgs.tif')

                file_path = os.path.abspath(_join(SCRATCH, file_name))

                @after_this_request
                def remove_file(response):
                    try:
                        os.remove(file_path)
                    except Exception as error:
                        app.logger.error("Error removing or closing downloaded file handle", error)

                    return response

                make_aggregated_rasters(fns, dst_fn=file_path)
                return send_file(file_path, as_attachment=True, attachment_filename=file_name)

        return jsonify(None)

    except Exception:
        return exception_factory()


# https://rangesat.nkn.uidaho.edu/api/raster-processing/inter-year/Zumwalt/biomass?start_year=2016&end_year=2018&start_date=5-1&end_date=8-30
@app.route('/raster-processing/inter-year/<location>/<product>')
@app.route('/raster-processing/inter-year/<location>/<product>/')
def interyear_rasterprocessing(location, product):
    try:
        ranches = request.args.get('ranches', None)
        utm = request.args.get('utm', False)
        start_year = request.args.get('start_year', None)
        end_year = request.args.get('end_year', None)
        start_date = request.args.get('start_date', '1-1')
        end_date = request.args.get('end_date', '12-31')
        rowpath = request.args.get('rowpath', None)

        if ranches is not None:
            ranches = literal_eval(ranches)

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

                    fn = None
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
                        continue
                    fn = fn[0]

                    if ranches is None:
                        fns.append(fn)
                    else:
                        head, tail = _split(fn)
                        _fn = _join(SCRATCH, '%s_%s' % (product_id, tail))
                        _location.mask_ranches(fn.replace('.wgs.tif', '.tif'), ranches, _fn)
                        fns.append(_fn)

                file_name = 'agg_{}.tif'.format(product)
                if ranches is not None:
                    file_name = '%s_%s' % (','.join(ranches), file_name)
                if not utm:
                    file_name = file_name.replace('.tif', '.wgs.tif')

                file_path = os.path.abspath(_join(SCRATCH, file_name))

                @after_this_request
                def remove_file(response):
                    try:
                        os.remove(file_path)
                    except Exception as error:
                        app.logger.error("Error removing or closing downloaded file handle", error)

                    return response

                make_aggregated_rasters(fns, dst_fn=file_path)
                return send_file(file_path, as_attachment=True, attachment_filename=file_name)

        return jsonify(None)

    except Exception:
        return exception_factory()


@app.route('/raster-processing/difference/<location>/<product>')
@app.route('/raster-processing/difference/<location>/<product>/')
def raster_difference(location, product):

    try:
        ranches = request.args.get('ranches', None)
        utm = request.args.get('utm', False)
        product_id = request.args.get('product_id', None)
        product_id2 = request.args.get('product_id2', scenemeta_location_latest(location))

        if ranches is not None:
            ranches = literal_eval(ranches)

        assert product_id is not None
        assert product_id2 is not None

        for rangesat_dir in RANGESAT_DIRS:
            loc_path = _join(rangesat_dir, location)
            if exists(loc_path):
                _location = Location(loc_path)
                out_dir = _location.out_dir

                fn = None
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
                    continue
                fn = fn[0]

                if ranches is not None:
                    _fn = os.path.abspath(_join(SCRATCH, '%s.tif' % uuid.uuid4()))
                    _location.mask_ranches(fn.replace('.wgs.tif', '.tif'), ranches, _fn)
                    fn = _fn

                fn2 = None
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
                    _fn2 = os.path.abspath(_join(SCRATCH, '%s.tif' % uuid.uuid4()))
                    _location.mask_ranches(fn2.replace('.wgs.tif', '.tif'), ranches, _fn2)
                    fn2 = _fn2

                if not utm:
                    file_name = '{product_id2}-{product_id}__{product}.wgs.tif'\
                                .format(product_id=product_id, product_id2=product_id2, product=product)
                else:
                    file_name = '{product_id2}-{product_id}__{product}.tif'\
                                .format(product_id=product_id, product_id2=product_id2, product=product)

                if ranches is not None:
                    file_name = '%s_%s' % (','.join(ranches), file_name)

                fn3 = os.path.abspath(_join(SCRATCH, '%s.tif' % uuid.uuid4()))
                make_raster_difference(fn, fn2, fn3)

                @after_this_request
                def remove_file(response):
                    try:
                        os.remove(fn3)
                    except Exception as error:
                        app.logger.error("Error removing or closing downloaded file handle", error)
                    return response

                return send_file(fn3, as_attachment=True, attachment_filename=file_name)

        return jsonify(None)

    except Exception:
        return exception_factory()


def _handle_pasturestat_request(data, csv, units='si', drop=None, additions=None):

    if units != 'si':
        if isinstance(data, dict):
            _data = {}
            for k, v in data.items():
                _data[k] = []
                for i, _v in enumerate(v):
                    _data[k].append({})
                    for key, value in _v.items():
                        if drop is not None:
                            if key in drop:
                                continue

                        if value is None:
                            _data[k][i][key.replace('gpm', 'lbperacre').replace('kg', 'lb')] = value
                            continue

                        if 'gpm' in key:
                            value *= 8.92179
                        if 'kg' in key:
                            value *= 2.204623

                        _data[k][i][key.replace('gpm', 'lbperacre').replace('kg', 'lb')] = value
        else:
            _data = []
            for row in data:
                _row = {}
                for key, value in row.items():
                    if drop is not None:
                        if key in drop:
                            continue

                    if 'gpm' in key:
                        value *= 8.92179
                    if 'kg' in key:
                        value *= 2.204623
                    key = key.replace('gpm', 'lbperacre').replace('kg', 'lb')
                    _row[key] = value
                _data.append(_row)
    else:
        if drop is None:
            _data = data
        else:
            if isinstance(data, dict):
                _data = data
                for key in drop:
                    del _data[key]
            else:
                _data = []
                for row in data:
                    for key in row:
                        del row[key]
                    _data.append(row)

    if additions is not None:
        _d = {}
        for key, addition in additions.items():
            _d[key] = addition['value']

        if isinstance(_data, dict):
            for k in _data:
                for i in range(len(_data[k])):
                    _data[k][i].update(_d)
        else:
            for i in range(len(data)):
                _data[i].update(_d)

        

    if csv is not None:
        if isinstance(_data, dict):
            __data = []
            for k, v in _data.items():
                __data.extend(v)
            _data = __data

        file_name = '%s.csv' % csv
        file_path = _join(SCRATCH, file_name)
        fieldnames = list(_data[0].keys())

        if units != 'si':
            fieldnames = [f.replace('gpm', 'lbperacre').replace('kg', 'lb') for f in fieldnames]

        if 'year' in fieldnames:
            fieldnames.remove('year')
            fieldnames.insert(0, 'year')

        if 'pasture' in fieldnames:
            fieldnames.remove('pasture')
            fieldnames.insert(0, 'pasture')

        if 'ranch' in fieldnames:
            fieldnames.remove('ranch')
            fieldnames.insert(0, 'ranch')

        if additions is not None:
            for key, addition in additions.items():
                fieldnames.remove(key)
                fieldnames.insert(addition['index'], key)

        # return jsonify(dict(fieldnames=fieldnames, data=_data))

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
        return jsonify(_data)


@app.route('/pasturestats/<location>')
@app.route('/pasturestats/<location>/')
def pasturestats(location):
    try:
        ranch = request.args.get('ranch', None)
        pasture = request.args.get('pasture', None)
        acquisition_date = request.args.get('acquisition_date', None)
        csv = request.args.get('csv', None)
        units = request.args.get('units', 'si')
        aggregate_by_ranch = bool(request.args.get('aggregate_by_ranch', False))


        if csv is not None:
            csv = 'pasturestats_ranch={ranch},pasture={pasture},acquisition_date={acquisition_date}'\
                  .format(ranch=ranch, pasture=pasture, acquisition_date=acquisition_date)

        for rangesat_dir in RANGESAT_DIRS:
            loc_path = _join(rangesat_dir, location)
            if exists(loc_path):
                _location = Location(loc_path)
                db_fn = _location.db_fn

                data = query_pasture_stats(db_fn, ranch=ranch, pasture=pasture, acquisition_date=acquisition_date)
                return _handle_pasturestat_request(data, csv, units,
                                           aggregate_by_ranch=aggregate_by_ranch)

        return jsonify(None)

    except Exception:
        return exception_factory()


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


def _date_formatter(date_str):

    return date_str.replace('12-', 'Dec ') \
                    .replace('11-', 'Nov') \
                    .replace('10-', 'Oct ') \
                    .replace('09-', 'Sep ') \
                    .replace('08-', 'Aug ') \
                    .replace('07-', 'Jul ') \
                    .replace('06-', 'Jun ') \
                    .replace('05-', 'May ') \
                    .replace('04-', 'Apr ') \
                    .replace('03-', 'Mar ') \
                    .replace('02-', 'Feb ') \
                    .replace('01-', 'Jan ') \
                    .replace('9-', 'Sep ') \
                    .replace('8-', 'Aug ') \
                    .replace('7-', 'Jul ') \
                    .replace('6-', 'Jun ') \
                    .replace('5-', 'May ') \
                    .replace('4-', 'Apr ') \
                    .replace('3-', 'Mar ') \
                    .replace('2-', 'Feb ') \
                    .replace('1-', 'Jan ')


def _date_validator(date_str):
    return ((any([date_str.startswith('%i-' % i) for i in range(1, 13)]) or
             any([date_str.startswith('%02i-' % i) for i in range(1, 13)])) and
            (any([date_str.endswith('-%i' % i) for i in range(1, 32)]) or
             any([date_str.endswith('-%02i' % i) for i in range(1, 32)])))


@app.route('/pasturestats/single-year/<location>')
@app.route('/pasturestats/single-year/<location>/')
def singleyear_pasturestats(location):
    """
    https://rangesat.org/api/pasturestats/single-year/Zumwalt/?ranch=TNC&pasture=A1&year=2018&csv=True

    :param location:
    :return:
    """
    try:
        ranch = request.args.get('ranch', None)
        pasture = request.args.get('pasture', None)
        year = request.args.get('year', None)
        start_date = request.args.get('start_date', '1-1')
        end_date = request.args.get('end_date', '12-31')
        agg_func = _get_agg_func(request.args.get('agg_func', 'mean'))
        csv = request.args.get('csv', None)
        units = request.args.get('units', 'si')
        drop = request.args.get('drop', None)

        if drop is not None:
            drop = drop.split(';')

        assert _date_validator(start_date), start_date
        assert _date_validator(end_date), end_date

        period = '{start_date} - {end_date}'.format(start_date=_date_formatter(start_date),
                                                    end_date=_date_formatter(end_date))

        additions = dict(date_period=dict(value=period, index=2))

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
                return _handle_pasturestat_request(data, csv, units=units, drop=drop, additions=additions)

        return jsonify(None)

    except Exception:
        return exception_factory()


@app.route('/pasturestats/intra-year/<location>')
@app.route('/pasturestats/intra-year/<location>/')
def intrayear_pasturestats(location):
    """
    https://rangesat.org/api/pasturestats/intra-year/Zumwalt/?ranch=TNC&pasture=A1&year=2018&csv=True

    :param location:
    :return:
    """
    try:
        ranch = request.args.get('ranch', None)
        pasture = request.args.get('pasture', None)
        year = request.args.get('year', None)
        start_date = request.args.get('start_date', '1-1')
        end_date = request.args.get('end_date', '12-31')
        agg_func = _get_agg_func(request.args.get('agg_func', 'mean'))
        csv = request.args.get('csv', None)
        units = request.args.get('units', 'si')
        drop = request.args.get('drop', None)

        if drop is not None:
            drop = drop.split(';')

        assert _date_validator(start_date), start_date
        assert _date_validator(end_date), end_date

        period = '{start_date} - {end_date}'.format(start_date=_date_formatter(start_date),
                                                    end_date=_date_formatter(end_date))

        additions = dict(date_period=dict(value=period, index=2))

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
                return _handle_pasturestat_request(data, csv, units=units, drop=drop, additions=additions)

        return jsonify(None)

    except Exception:
        return exception_factory()


@app.route('/pasturestats/max-pasture-seasonal/<location>')
@app.route('/pasturestats/max-pasture-seasonal/<location>/')
def max_seasonal_pasturestats(location):
    """
    https://rangesat.org/api/pasturestats/max-pasture-seasonal/Zumwalt/?ranch=TNC&pasture=A1&year=2019&csv=True

    :param location:
    :return:
    """
    try:
        ranch = request.args.get('ranch', None)
        pasture = request.args.get('pasture', None)
        year = request.args.get('year', None)
        start_date = request.args.get('start_date', '1-1')
        end_date = request.args.get('end_date', '12-31')
        measure = request.args.get('measure', 'biomass_mean_gpm')
        csv = request.args.get('csv', None)
        units = request.args.get('units', 'si')
        drop = request.args.get('drop', None)

        if year is None:
            year = datetime.now().year

        if drop is not None:
            drop = drop.split(';')

        assert _date_validator(start_date), start_date
        assert _date_validator(end_date), end_date

        period = '{start_date} - {end_date}'.format(start_date=_date_formatter(start_date),
                                                    end_date=_date_formatter(end_date))

        additions = dict(year=dict(value=year, index=2),
                         date_period=dict(value=period, index=2))

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
                return _handle_pasturestat_request(data, csv, units=units, drop=drop, additions=additions)

        return jsonify(None)

    except Exception:
        return exception_factory()


@app.route('/pasturestats/single-year-monthly/<location>')
@app.route('/pasturestats/single-year-monthly/<location>/')
def singleyearmonthly_pasturestats(location):
    """
    TODO: weight ranch pasturestats by area
    TODO: build gridmet route to get climates for the ranch
    TODO: zumwalt 2020 scenes
    :param location:
    :return:
    """
    try:
        ranch = request.args.get('ranch', None)
        pasture = request.args.get('pasture', None)
        year = request.args.get('year', None)
        agg_func = _get_agg_func(request.args.get('agg_func', 'mean'))
        csv = request.args.get('csv', None)
        units = request.args.get('units', 'si')
        drop = request.args.get('drop', None)

        if year is None:
            year = datetime.now().year

        if drop is not None:
            drop = drop.split(';')

        if csv is not None:
            csv = 'pasturestats-intra-year_ranch={ranch},pasture={pasture},'\
                  'year={year},agg_func={agg_func}'\
                  .format(ranch=ranch, pasture=pasture,
                          year=year, agg_func=agg_func.__name__)

        additions = dict(year=dict(value=year, index=2))

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
                return _handle_pasturestat_request(data, csv, units=units, drop=drop, additions=additions)

        return jsonify(None)

    except Exception:
        return exception_factory()


@app.route('/ranchstats/single-year-monthly/<location>')
@app.route('/ranchstats/single-year-monthly/<location>/')
def singleyearmonthly_ranchstats(location):
    """
    TODO: weight ranch pasturestats by area
    TODO: build gridmet route to get climates for the ranch
    TODO: zumwalt 2020 scenes
    :param location:
    :return:
    """
    try:
        ranch = request.args.get('ranch', None)
        year = request.args.get('year', None)
        agg_func = _get_agg_func(request.args.get('agg_func', 'mean'))
        csv = request.args.get('csv', None)
        units = request.args.get('units', 'si')
        drop = request.args.get('drop', None)

        if year is None:
            year = datetime.now().year

        if drop is not None:
            drop = drop.split(';')

        if csv is not None:
            csv = 'ranchstats-intra-year_ranch={ranch},year={year},agg_func={agg_func}'\
                  .format(ranch=ranch, pasture=pasture, year=year, agg_func=agg_func.__name__)

        additions = dict(year=dict(value=year, index=2))

        if agg_func is None:
            return jsonify(None)

        for rangesat_dir in RANGESAT_DIRS:
            loc_path = _join(rangesat_dir, location)
            if exists(loc_path):
                _location = Location(loc_path)
                db_fn = _location.db_fn

                data = query_singleyearmonthly_ranch_stats(db_fn, ranch=ranch, year=year,
                                                             agg_func=agg_func,
                                                             key_delimiter=_location.key_delimiter)
                return _handle_pasturestat_request(data, csv, units=units, drop=drop, additions=additions)

        return jsonify(None)

    except Exception:
        return exception_factory()


@app.route('/pasturestats/seasonal-progression/<location>')
@app.route('/pasturestats/seasonal-progression/<location>/')
def seasonalprogression_pasturestats(location):
    """
    https://rangesat.org/api/pasturestats/seasonal-progression/Zumwalt/?ranch=TNC&pasture=A1&start_year=2000&end_year=2019&csv=True

    :param location:
    :return:
    """
    try:
        ranch = request.args.get('ranch', None)
        pasture = request.args.get('pasture', None)
        start_year = request.args.get('start_year', None)
        end_year = request.args.get('end_year', None)
        agg_func = _get_agg_func(request.args.get('agg_func', 'mean'))
        csv = request.args.get('csv', None)
        units = request.args.get('units', 'si')
        drop = request.args.get('drop', None)

        if drop is not None:
            drop = drop.split(';')

        yr_period = '{start_year} - {end_year}'.format(start_year=start_year,
                                                       end_year=end_year)

        additions = dict(year_period=dict(value=yr_period, index=2))

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
                return _handle_pasturestat_request(data, csv, units=units, drop=drop, additions=additions)

        return jsonify(None)

    except Exception:
        return exception_factory()


@app.route('/ranchstats/seasonal-progression/<location>')
@app.route('/ranchstats/seasonal-progression/<location>/')
def seasonalprogression_ranchstats(location):
    """
    https://rangesat.org/api/ranchstats/seasonal-progression/Zumwalt/?ranch=TNC&pasture=A1&start_year=2000&end_year=2019&csv=True

    :param location:
    :return:
    """
    try:
        ranch = request.args.get('ranch', None)
        start_year = request.args.get('start_year', None)
        end_year = request.args.get('end_year', None)
        agg_func = _get_agg_func(request.args.get('agg_func', 'mean'))
        csv = request.args.get('csv', None)
        units = request.args.get('units', 'si')
        drop = request.args.get('drop', None)

        if drop is not None:
            drop = drop.split(';')

        yr_period = '{start_year} - {end_year}'.format(start_year=start_year,
                                                       end_year=end_year)

        additions = dict(year_period=dict(value=yr_period, index=2))

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

                data = query_seasonalprogression_ranch_stats(db_fn, ranch=ranch,
                                                               start_year=start_year, end_year=end_year,
                                                               agg_func=agg_func, key_delimiter=_location.key_delimiter)
                return _handle_pasturestat_request(data, csv, units=units, drop=drop, additions=additions)

        return jsonify(None)

    except Exception:
        return exception_factory()


@app.route('/pasturestats/inter-year/<location>')
@app.route('/pasturestats/inter-year/<location>/')
def interyear_pasturestats(location):
    """
    https://rangesat.org/api/pasturestats/inter-year/Zumwalt/?ranch=TNC&pasture=A1&start_year=2000&end_year=2019&csv=True

    :param location:
    :return:
    """
    try:
        ranch = request.args.get('ranch', None)
        pasture = request.args.get('pasture', None)
        start_year = request.args.get('start_year', None)
        end_year = request.args.get('end_year', None)
        start_date = request.args.get('start_date', '1-1')
        end_date = request.args.get('end_date', '12-31')
        agg_func = _get_agg_func(request.args.get('agg_func', 'mean'))
        csv = request.args.get('csv', None)
        units = request.args.get('units', 'si')
        drop = request.args.get('drop', None)

        if drop is not None:
            drop = drop.split(';')

        assert _date_validator(start_date), start_date
        assert _date_validator(end_date), end_date

        period = '{start_date} - {end_date}'.format(start_date=_date_formatter(start_date),
                                                    end_date=_date_formatter(end_date))

        yr_period = '{start_year} - {end_year}'.format(start_year=start_year,
                                                       end_year=end_year)

        additions = dict(date_period=dict(value=period, index=2),
                         year_period=dict(value=yr_period, index=2))

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
                return _handle_pasturestat_request(data, csv, units=units, drop=drop, additions=additions)

        return jsonify(None)

    except Exception:
        return exception_factory()


# https://rangesat.nkn.uidaho.edu/api/pasturestats/multi-year/Zumwalt/?ranch=TNC&pasture=A1&start_year=1984&end_year=2019&start_date=05-15&end_date=07-15&agg_func=mean&units=en&drop=ndvi_mean;ndvi_sd;ndvi_10pct;ndvi_75pct;ndvi_90pct;ndvi_ci90;nbr_sd;nbr_mean;nbr_10pct;nbr_75pct;nbr_90pct;nbr_ci90;nbr2_mean;nbr2_sd;nbr2_10pct;nbr2_75pct;nbr2_90pct;nbr2_ci90;summer_vi_mean_gpm;fall_vi_mean_gpm

@app.route('/pasturestats/multi-year/<location>')
@app.route('/pasturestats/multi-year/<location>/')
def multiyear_pasturestats(location):
    """
    https://rangesat.org/api/pasturestats/multi-year/Zumwalt/?ranch=TNC&pasture=A1&start_year=2000&end_year=2019&csv=True

    :param location:
    :return:
    """
    try:
        ranch = request.args.get('ranch', None)
        pasture = request.args.get('pasture', None)
        start_year = request.args.get('start_year', None)
        end_year = request.args.get('end_year', None)
        start_date = request.args.get('start_date', '1-1')
        end_date = request.args.get('end_date', '12-31')
        agg_func = _get_agg_func(request.args.get('agg_func', 'mean'))
        csv = request.args.get('csv', None)
        units = request.args.get('units', 'si')
        drop = request.args.get('drop', None)

        if drop is not None:
            drop = drop.split(';')

        assert _date_validator(start_date), start_date
        assert _date_validator(end_date), end_date

        period = '{start_date} - {end_date}'.format(start_date=_date_formatter(start_date),
                                                    end_date=_date_formatter(end_date))

        additions = dict(period=dict(value=period, index=3))

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
                return _handle_pasturestat_request(data, csv, units=units, drop=drop, additions=additions)

        return jsonify(None)

    except Exception:
        return exception_factory()

@app.route('/ranchstats/multi-year/<location>')
@app.route('/ranchstats/multi-year/<location>/')
def multiyear_ranchstats(location):
    """
    https://rangesat.org/api/ranchstats/multi-year/Zumwalt/?ranch=TNC&start_year=2000&end_year=2019&csv=True

    :param location:
    :return:
    """
    try:
        ranch = request.args.get('ranch', None)
        start_year = request.args.get('start_year', None)
        end_year = request.args.get('end_year', None)
        start_date = request.args.get('start_date', '1-1')
        end_date = request.args.get('end_date', '12-31')
        agg_func = _get_agg_func(request.args.get('agg_func', 'mean'))
        csv = request.args.get('csv', None)
        units = request.args.get('units', 'si')
        drop = request.args.get('drop', None)

        if drop is not None:
            drop = drop.split(';')

        assert _date_validator(start_date), start_date
        assert _date_validator(end_date), end_date

        period = '{start_date} - {end_date}'.format(start_date=_date_formatter(start_date),
                                                    end_date=_date_formatter(end_date))

        additions = dict(period=dict(value=period, index=3))

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

                data = query_multiyear_ranch_stats(db_fn, ranch=ranch,
                                                     start_year=start_year, end_year=end_year,
                                                     start_date=start_date, end_date=end_date, agg_func=agg_func,
                                                     key_delimiter=_location.key_delimiter)
                return _handle_pasturestat_request(data, csv, units=units, drop=drop, additions=additions)

        return jsonify(None)

    except Exception:
        return exception_factory()


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


@app.route('/gridmet/multi-year/<location>/<ranch>')
@app.route('/gridmet/multi-year/<location>/<ranch>/')
@app.route('/gridmet/multi-year/<location>/<ranch>/<pasture>')
@app.route('/gridmet/multi-year/<location>/<ranch>/<pasture>/')
def gridmet_allyears_pasture(location, ranch, pasture=None):
    try:
        start_year = request.args.get('start_year', None)
        end_year = request.args.get('end_year', None)
        units = request.args.get('units', 'SI')
        csv = request.args.get('csv', None)

        if csv is not None:
            if pasture is not None:
                csv = 'gridmet-multi-year_ranch={ranch},pasture={pasture},'\
                      'start_year={start_year},end_year={end_year},units={units}'\
                      .format(ranch=ranch, pasture=pasture,
                              start_year=start_year, end_year=end_year, units=units)
            else:
                csv = 'gridmet-multi-year_ranch={ranch},'\
                      'start_year={start_year},end_year={end_year},units={units}'\
                      .format(ranch=ranch,
                              start_year=start_year, end_year=end_year, units=units)

        for rangesat_dir in RANGESAT_DIRS:
            loc_path = _join(rangesat_dir, location)
            if exists(loc_path):
                _location = Location(loc_path)
                ranch = ranch.replace("'", "~").replace(' ', '_')
                if pasture is None:
                    pasture = _location.representative_pasture
                pasture = pasture.replace("'", "~").replace(' ', '_')
                d = load_gridmet_all_years(_join(_location.loc_path, 'gridmet', ranch, pasture),
                                           start_year, end_year, units)
                return _handle_gridmet_multiyear_request(d, csv, units)

        return jsonify(None)

    except Exception:
        return exception_factory()


@app.route('/gridmet/single-year/<location>/<ranch>')
@app.route('/gridmet/single-year/<location>/<ranch>/')
@app.route('/gridmet/single-year/<location>/<ranch>/<pasture>')
@app.route('/gridmet/single-year/<location>/<ranch>/<pasture>/')
def gridmet_singleyear_pasture(location, ranch, pasture=None):
    try:
        year = request.args.get('year', datetime.now().year)
        year = int(year)
        units = request.args.get('units', 'SI')
        csv = request.args.get('csv', None)

        if csv is not None:
            if pasture is not None:
                csv = 'gridmet-single-year_ranch={ranch},pasture={pasture},'\
                      'year={year},units={units}'\
                      .format(ranch=ranch, pasture=pasture,
                              year=year, units=units)
            else:
                csv = 'gridmet-single-year_ranch={ranch},'\
                      'year={year},units={units}'\
                      .format(ranch=ranch,
                              year=year, units=units)

        for rangesat_dir in RANGESAT_DIRS:
            loc_path = _join(rangesat_dir, location)
            if exists(loc_path):
                _location = Location(loc_path)
                ranch = ranch.replace("'", "~").replace(' ', '_')
                if pasture is None:
                    pasture = _location.representative_pasture
                pasture = pasture.replace("'", "~").replace(' ', '_')
                directory = _join(_location.loc_path, 'gridmet', ranch, pasture)
                d = load_gridmet_single_year(directory, year, units)

                return _handle_gridmet_request(d, csv, units)

        return jsonify(None)

    except Exception:
        return exception_factory()


@app.route('/gridmet/single-year-monthly/<location>/<ranch>')
@app.route('/gridmet/single-year-monthly/<location>/<ranch>/')
@app.route('/gridmet/single-year-monthly/<location>/<ranch>/<pasture>')
@app.route('/gridmet/single-year-monthly/<location>/<ranch>/<pasture>/')
def gridmet_singleyearmonthly_pasture(location, ranch, pasture=None):
    try:
        year = request.args.get('year', datetime.now().year)
        year = int(year)
        units = request.args.get('units', 'SI')
        csv = request.args.get('csv', None)

        if csv is not None:
            if pasture is not None:
                csv = 'gridmet-single-year-monthly={ranch},pasture={pasture},'\
                      'year={year},units={units}'\
                      .format(ranch=ranch, pasture=pasture,
                              year=year, units=units)
            else:
                csv = 'gridmet-single-year-monthly={ranch},'\
                      'year={year},units={units}'\
                      .format(ranch=ranch,
                              year=year, units=units)

        for rangesat_dir in RANGESAT_DIRS:
            loc_path = _join(rangesat_dir, location)
            if exists(loc_path):
                _location = Location(loc_path)
                ranch = ranch.replace("'", "~").replace(' ', '_')
                if pasture is None:
                    pasture = _location.representative_pasture
                pasture = pasture.replace("'", "~").replace(' ', '_')
                d = load_gridmet_single_year_monthly(_join(_location.loc_path, 'gridmet', ranch, pasture),
                                                     year, units)
                return _handle_gridmet_request(d, csv, units)

        return jsonify(None)

    except Exception:
        return exception_factory()


@app.route('/gridmet/annual-progression/<location>/<ranch>')
@app.route('/gridmet/annual-progression/<location>/<ranch>/')
@app.route('/gridmet/annual-progression/<location>/<ranch>/<pasture>')
@app.route('/gridmet/annual-progression/<location>/<ranch>/<pasture>/')
def gridmet_annualprogression_pasture(location, ranch, pasture=None):
    try:
        start_year = request.args.get('start_year', None)
        end_year = request.args.get('end_year', None)
        units = request.args.get('units', 'SI')
        csv = request.args.get('csv', None)

        if csv is not None:
            if pasture is not None:
                csv = 'gridmet-annual-progression={ranch},pasture={pasture},'\
                      'start_year={start_year},end_year={end_year},units={units}'\
                      .format(ranch=ranch, pasture=pasture,
                              start_year=start_year, end_year=end_year, units=units)
            else:
                csv = 'gridmet-annual-progression={ranch},'\
                      'start_year={start_year},end_year={end_year},units={units}'\
                      .format(ranch=ranch,
                              start_year=start_year, end_year=end_year, units=units)

        for rangesat_dir in RANGESAT_DIRS:
            loc_path = _join(rangesat_dir, location)
            if exists(loc_path):
                _location = Location(loc_path)
                ranch = ranch.replace("'", "~").replace(' ', '_')
                if pasture is None:
                    pasture = _location.representative_pasture
                pasture = pasture.replace("'", "~").replace(' ', '_')
                d = load_gridmet_annual_progression(_join(_location.loc_path, 'gridmet', ranch, pasture),
                                                    start_year, end_year, units)
                return _handle_gridmet_request(d, csv, units)

        return jsonify(None)

    except Exception:
        return exception_factory()


@app.route('/gridmet/annual-progression-monthly/<location>/<ranch>')
@app.route('/gridmet/annual-progression-monthly/<location>/<ranch>/')
@app.route('/gridmet/annual-progression-monthly/<location>/<ranch>/<pasture>')
@app.route('/gridmet/annual-progression-monthly/<location>/<ranch>/<pasture>/')
def gridmet_annualprogression_monthly_pasture(location, ranch, pasture=None):
    try:
        start_year = request.args.get('start_year', None)
        end_year = request.args.get('end_year', None)
        units = request.args.get('units', 'SI')
        csv = request.args.get('csv', None)

        if csv is not None:
            if pasture is not None:
                csv = 'gridmet-annual-progression-monthly={ranch},pasture={pasture},'\
                      'start_year={start_year},end_year={end_year},units={units}'\
                      .format(ranch=ranch, pasture=pasture,
                              start_year=start_year, end_year=end_year, units=units)
            else:
                csv = 'gridmet-annual-progression-monthly={ranch},'\
                      'start_year={start_year},end_year={end_year},units={units}'\
                      .format(ranch=ranch,
                              start_year=start_year, end_year=end_year, units=units)

        for rangesat_dir in RANGESAT_DIRS:
            loc_path = _join(rangesat_dir, location)
            if exists(loc_path):
                _location = Location(loc_path)
                ranch = ranch.replace("'", "~").replace(' ', '_')
                if pasture is None:
                    pasture = _location.representative_pasture
                pasture = pasture.replace("'", "~").replace(' ', '_')
                d = load_gridmet_annual_progression_monthly(_join(_location.loc_path, 'gridmet', ranch, pasture),
                                                            start_year, end_year, units)
                return _handle_gridmet_request(d, csv, units)

        return jsonify(None)

    except Exception:
        return exception_factory()


@app.route('/histogram/single-scene-bypasture/<location>/<ranch>')
@app.route('/histogram/single-scene-bypasture/<location>/<ranch>/')
def histogram_singlescene_bypasture(location, ranch):
    try:
        product_id = request.args.get('product_id', scenemeta_location_latest(location))
        product = request.args.get('product', 'biomass')
        _bins = request.args.get('bins', 10)
        _bins = literal_eval(_bins)

        res = []
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

                data = _location.extract_pixels_by_pasture(fn, ranch)

                for key in data:
                    _ranch, _pasture = key
                    _data, _total_px = data[key]
                    counts, bins = np.histogram(_data, bins=_bins)
                    _masked = _total_px - int(np.sum(counts))
                    counts = [int(x) for x in counts]
                    bins = [float(x) for x in bins]

                    res.append(dict(ranch=_ranch, pasture=_pasture,
                                    counts=counts, bins=bins, total_px=_total_px, masked=_masked))

                return jsonify(res)

        return jsonify(None)

    except Exception:
        return exception_factory()


@app.route('/histogram/single-scene/<location>/<ranch>')
@app.route('/histogram/single-scene/<location>/<ranch>/')
@app.route('/histogram/single-scene/<location>/<ranch>/<pasture>')
@app.route('/histogram/single-scene/<location>/<ranch>/<pasture>/')
def histogram_singlescene_pasture(location, ranch, pasture=None):
    try:
        product_id = request.args.get('product_id', scenemeta_location_latest(location))
        product = request.args.get('product', 'biomass')
        _bins = request.args.get('bins', 10)
        _bins = literal_eval(_bins)

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

                data, total_px = _location.extract_pixels(fn, ranch, pasture)
                counts, bins = np.histogram(data, bins=_bins)
                _masked = total_px - int(np.sum(counts))
                counts = [int(x) for x in counts]
                bins = [float(x) for x in bins]

                d = dict(ranch=ranch, counts=counts, bins=bins, product_id=product_id, product=product,
                         total_px=total_px, masked=_masked)
                if pasture is not None:
                    d['pasture'] = pasture

                return jsonify(d)

        return jsonify(None)

    except Exception:
        return exception_factory()




@app.route('/histogram/intra-year/<location>/<ranch>')
@app.route('/histogram/intra-year/<location>/<ranch>/')
@app.route('/histogram/intra-year/<location>/<ranch>/<pasture>')
@app.route('/histogram/intra-year/<location>/<ranch>/<pasture>/')
def intrayear_histogram(location, ranch, pasture=None):
    try:
        year = request.args.get('year', None)
        start_date = request.args.get('start_date', '1-1')
        end_date = request.args.get('end_date', '12-31')
        product = request.args.get('product', 'biomass')
        _bins = request.args.get('bins', 10)
        _bins = literal_eval(_bins)

        for rangesat_dir in RANGESAT_DIRS:
            loc_path = _join(rangesat_dir, location)
            if exists(loc_path):
                _location = Location(loc_path)
                out_dir = _location.out_dir

                product_ids = scenemeta_location_intrayear(location, year, start_date, end_date)

                data = []
                total_px = 0
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

                    _data, _total_px = _location.extract_pixels(fn, ranch, pasture)
                    data.append(_data)
                    total_px += _total_px

                counts, bins = np.histogram(data, bins=_bins)
                _masked = total_px - int(np.sum(counts))
                counts = [int(x) for x in counts]
                bins = [float(x) for x in bins]

                d = dict(ranch=ranch, counts=counts, bins=bins, product_ids=product_ids, product=product,
                                    total_px=total_px, masked=_masked)
                if pasture is not None:
                    d['pasture'] = pasture

                return jsonify(d)

        return jsonify(None)

    except Exception:
        return exception_factory()


@app.route('/histogram/inter-year/<location>/<ranch>')
@app.route('/histogram/inter-year/<location>/<ranch>/')
@app.route('/histogram/inter-year/<location>/<ranch>/<pasture>')
@app.route('/histogram/inter-year/<location>/<ranch>/<pasture>/')
def interyear_histogram(location, ranch, pasture=None):
    try:
        start_year = request.args.get('start_year', None)
        end_year = request.args.get('end_year', None)
        start_date = request.args.get('start_date', '1-1')
        end_date = request.args.get('end_date', '12-31')
        product = request.args.get('product', 'biomass')
        _bins = request.args.get('bins', 10)
        _bins = literal_eval(_bins)

        for rangesat_dir in RANGESAT_DIRS:
            loc_path = _join(rangesat_dir, location)
            if exists(loc_path):
                _location = Location(loc_path)
                out_dir = _location.out_dir

                product_ids = scenemeta_location_interyear(location, start_year, end_year, start_date, end_date)

                data = []
                total_px = 0
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

                    _data, _total_px = _location.extract_pixels(fn, ranch, pasture)
                    data.append(_data)
                    total_px += _total_px

                counts, bins = np.histogram(data, bins=_bins)
                _masked = total_px - int(np.sum(counts))
                counts = [int(x) for x in counts]
                bins = [float(x) for x in bins]
                d = dict(ranch=ranch, counts=counts, bins=bins, product_ids=product_ids, product=product,
                                    total_px=sum(total_px, masked=_masked))

                if pasture is not None:
                    d['pasture'] = pasture

                return jsonify(d)

        return jsonify(None)

    except Exception:
        return exception_factory()



if __name__ == '__main__':
    app.run(debug=True)
# rsync -av --progress --exclude temp rangesat-biomass/ rogerlew@rangesat.nkn.uidaho.edu:/var/www/rangesat-biomass
# rsync -av --progress geodata/rangesat/ rogerlew@rangesat.nkn.uidaho.edu:/geodata/rangesat
