#!/usr/bin/python3

from os.path import join as _join
from os.path import split as _split

from os.path import isdir, exists

from flask import Flask, jsonify, send_file, request
from glob import glob
from datetime import datetime, date

import numpy as np

from database import Location
from database.pasturestats import (
    query_pasture_stats,
    query_intrayear_pasture_stats,
    query_interyear_pasture_stats,
    query_multiyear_pasture_stats
)

from database.gridmet import (
    load_gridmet_all_years,
    load_gridmet_single_year,
    load_gridmet_annual_progression
)

from biomass.landsat import LandSatScene


app = Flask(__name__)

if exists('/geodata'):
    GEODATA_DIR = '/geodata'
else:
    GEODATA_DIR = '/Users/roger/geodata'

RANGESAT_DIR = _join(GEODATA_DIR, 'rangesat')


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
            pasturestats=[
                'pasturestats/<location>/?ranch=<ranch>&pasture=<pasture>&acquisition_date=<acquisition_date>',
                'pasturestats/intra-year/<location>/?ranch=<ranch>&pasture=<pasture>&year=<year>&start_date=<start_date>&end_date=<end_date>',
                'pasturestats/inter-year/<location>/?ranch=<ranch>&pasture=<pasture>&start_year=<start_year>&end_year=<end_year>&start_date=<start_date>&end_date=<end_date>',
                'pasturestats/multi-year/<location>/?ranch=<ranch>&pasture=<pasture>&start_year=<start_year>&end_year=<end_year>&start_date=<start_date>&end_date=<end_date>'],
            gridmet=[
                'gridmet/single-year/<location>/<ranch>/<pasture>?year=<year>',
                'gridmet/multi-year/<location>/<ranch>/<pasture>?start_year=<start_year>&end_year=<end-year>',
                'gridmet/annual-year/<location>/<ranch>/<pasture>?start_year=<start_year>&end_year=<end-year>'],
            histogram=[
                'histogram/single-scene/<location>/<ranch>/<pasture>?product_id=<product_id>&product=<product>',
                'histogram/intra-year/<location>/<ranch>/<pasture>?year=<year>&start_date=<start_date>&end_date=<end_date>&product=<product>',
                'histogram/inter-year/<location>/<ranch>/<pasture>?start_year=<start_year>&end_year=<end_year>&start_date=<start_date>&end_date=<end_date>&product=<product>']

       ))


@app.route('/location')
def locations():
    queryset = glob('{}/*'.format(RANGESAT_DIR))
    queryset = [_split(loc)[-1] for loc in queryset if isdir(loc)]
    return jsonify(dict(locations=queryset))


@app.route('/location/<location>')
@app.route('/location/<location>/')
def location(location):
    loc_path = _join(RANGESAT_DIR, location)
    if exists(loc_path):
        _location = Location(loc_path)
        return jsonify(_location.serialized())
    else:
        return jsonify(None)


@app.route('/location/<location>/<ranch>')
@app.route('/location/<location>/<ranch>/')
def ranch(location, ranch):
    loc_path = _join(RANGESAT_DIR, location)
    if exists(loc_path):
        _location = Location(loc_path)
        return jsonify(_location.serialized_ranch(ranch))
    else:
        return jsonify(None)


@app.route('/location/<location>/<ranch>/<pasture>')
@app.route('/location/<location>/<ranch>/<pasture>/')
def pasture(location, ranch, pasture):
    loc_path = _join(RANGESAT_DIR, location)
    if exists(loc_path):
        _location = Location(loc_path)
        return jsonify(_location.serialized_pasture(ranch, pasture))
    else:
        return jsonify(None)


@app.route('/geojson/<location>')
@app.route('/geojson/<location>/')
def geojson_location(location):
    loc_path = _join(RANGESAT_DIR, location)
    if exists(loc_path):
        _location = Location(loc_path)
        return jsonify(_location.geojson_filter())
    else:
        return jsonify(None)


@app.route('/geojson/<location>/<ranch>')
@app.route('/geojson/<location>/<ranch>/')
def geojson_ranch(location, ranch):
    loc_path = _join(RANGESAT_DIR, location)
    if exists(loc_path):
        _location = Location(loc_path)
        return jsonify(_location.geojson_filter(ranch))
    else:
        return jsonify(None)


@app.route('/geojson/<location>/<ranch>/<pasture>')
@app.route('/geojson/<location>/<ranch>/<pasture>/')
def geojson_pasture(location, ranch, pasture):
    loc_path = _join(RANGESAT_DIR, location)
    if exists(loc_path):
        _location = Location(loc_path)
        return jsonify(_location.geojson_filter(ranch, pasture))
    else:
        return jsonify(None)


def _scenemeta_location_latest(location):
    loc_path = _join(RANGESAT_DIR, location)
    if exists(loc_path):
        _location = Location(loc_path)
        out_dir = _location.out_dir
        ls_fns = glob(_join(out_dir, '*/*pixel_qa.tif'))
        ls_fns = [_split(fn)[0] for fn in ls_fns]
        ls_fns = [_split(fn)[-1] for fn in ls_fns]
        dates = [int(fn.split('_')[4]) for fn in ls_fns]
        return ls_fns[np.argmax(dates)]
    else:
        return None


def _scenemeta_location_intrayear(location, year, start_date, end_date):

    loc_path = _join(RANGESAT_DIR, location)
    if exists(loc_path):
        _location = Location(loc_path)
        out_dir = _location.out_dir
        ls_fns = glob(_join(out_dir, '*/*pixel_qa.tif'))
        ls_fns = [_split(fn)[0] for fn in ls_fns]
        ls_fns = [_split(fn)[-1] for fn in ls_fns]
        dates = [fn.split('_')[4] for fn in ls_fns]
        dates = [date(int(d[:4]), int(d[4:6]), int(d[6:8])) for d in dates]
        _start_date = date(*map(int, '{}-{}'.format(year, start_date).split('-')))
        _end_date = date(*map(int, '{}-{}'.format(year, end_date).split('-')))
        mask = [_start_date < d < _end_date for d in dates]

        fns = []
        for fn, m in zip(ls_fns, mask):
            if not m:
                continue

            fns.append(fn)

        return fns
    else:
        return None


def _scenemeta_location_interyear(location, start_year, end_year, start_date, end_date):

    loc_path = _join(RANGESAT_DIR, location)
    if exists(loc_path):
        _location = Location(loc_path)
        out_dir = _location.out_dir
        ls_fns = glob(_join(out_dir, '*/*pixel_qa.tif'))
        ls_fns = [_split(fn)[0] for fn in ls_fns]
        ls_fns = [_split(fn)[-1] for fn in ls_fns]
        dates = [fn.split('_')[4] for fn in ls_fns]
        dates = [date(int(d[:4]), int(d[4:6]), int(d[6:8])) for d in dates]
        start_year = int(start_year)
        end_year = int(end_year)
        mask = [start_year < d.year < end_year for d in dates]

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

            fns.append(fn)

        return fns
    else:
        return None


#@app.route('/scenemeta/<location>/latest')
#@app.route('/scenemeta/<location>/latest/')
#def scenemeta_location_latest(location):
#    return jsonify(_scenemeta_location_latest(location))


@app.route('/scenemeta/<location>')
@app.route('/scenemeta/<location>/')
def scenemeta_location(location):
    loc_path = _join(RANGESAT_DIR, location)
    if exists(loc_path):
        _location = Location(loc_path)
        out_dir = _location.out_dir
        ls_fns = glob(_join(out_dir, '*/*pixel_qa.tif'))
        ls_fns = [_split(fn)[0] for fn in ls_fns]
        ls_fns = [_split(fn)[-1] for fn in ls_fns]

        return jsonify(ls_fns)

    else:
        return jsonify(None)


@app.route('/scenemeta/<location>/<product_id>')
@app.route('/scenemeta/<location>/<product_id>/')
def scenemeta_location_product_id(location, product_id):
    loc_path = _join(RANGESAT_DIR, location)
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

    else:
        return jsonify(None)


@app.route('/raster/<location>/<product_id>/<product>')
@app.route('/raster/<location>/<product_id>/<product>/')
def raster(location, product_id, product):
    loc_path = _join(RANGESAT_DIR, location)
    _location = Location(loc_path)
    out_dir = _location.out_dir

    fn = []
    if product in ['ndvi', 'nbr', 'nbr2', 'pixel_qa']:
        fn = glob(_join(out_dir, product_id, '*{}.tif'.format(product)))

    elif product in ['biomass', 'fall_vi', 'summer_vi']:
        fn = glob(_join(out_dir, product_id, 'biomass/*{}.tif'.format(product)))

    if len(fn) != 1:
        return jsonify(None)
    fn = fn[0]

    return send_file(fn)


@app.route('/pasturestats/<location>')
@app.route('/pasturestats/<location>/')
def pasturestats(location):
    ranch = request.args.get('ranch', None)
    pasture = request.args.get('pasture', None)
    acquisition_date = request.args.get('acquisition_date', None)

    loc_path = _join(RANGESAT_DIR, location)
    _location = Location(loc_path)
    db_fn = _location.db_fn

    return jsonify(query_pasture_stats(db_fn, ranch=ranch, pasture=pasture, acquisition_date=acquisition_date))


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


@app.route('/pasturestats/intra-year/<location>')
@app.route('/pasturestats/intra-year/<location>/')
def intrayear_pasturestats(location):
    ranch = request.args.get('ranch', None)
    pasture = request.args.get('pasture', None)
    year = request.args.get('year', None)
    start_date = request.args.get('start_date', None)
    end_date = request.args.get('end_date', None)
    agg_func = _get_agg_func(request.args.get('agg_func', 'mean'))

    if agg_func is None:
        return jsonify(None)

    loc_path = _join(RANGESAT_DIR, location)
    _location = Location(loc_path)
    db_fn = _location.db_fn

    return jsonify(query_intrayear_pasture_stats(db_fn, ranch=ranch, pasture=pasture, year=year,
                                                 start_date=start_date, end_date=end_date, agg_func=agg_func))


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

    if agg_func is None:
        return jsonify(None)

    loc_path = _join(RANGESAT_DIR, location)
    _location = Location(loc_path)
    db_fn = _location.db_fn

    return jsonify(query_interyear_pasture_stats(db_fn, ranch=ranch, pasture=pasture,
                                                 start_year=start_year, end_year=end_year,
                                                 start_date=start_date, end_date=end_date, agg_func=agg_func))


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

    if agg_func is None:
        return jsonify(None)

    loc_path = _join(RANGESAT_DIR, location)
    _location = Location(loc_path)
    db_fn = _location.db_fn

    return jsonify(query_multiyear_pasture_stats(db_fn, ranch=ranch, pasture=pasture,
                                                 start_year=start_year, end_year=end_year,
                                                 start_date=start_date, end_date=end_date, agg_func=agg_func))


@app.route('/gridmet/multi-year/<location>/<ranch>/<pasture>')
@app.route('/gridmet/multi-year/<location>/<ranch>/<pasture>/')
def gridmet_allyears_pasture(location, ranch, pasture):
    start_year = request.args.get('start_year', None)
    end_year = request.args.get('end_year', None)

    loc_path = _join(RANGESAT_DIR, location)
    if exists(loc_path):
        _location = Location(loc_path)
        ranch = ranch.replace("'", "~").replace(' ', '_')
        pasture = pasture.replace("'", "~").replace(' ', '_')
        d = load_gridmet_all_years(_join(_location.loc_path, 'gridmet', ranch, pasture), start_year, end_year)
        return jsonify(d)
    else:
        return jsonify(None)


@app.route('/gridmet/single-year/<location>/<ranch>/<pasture>')
@app.route('/gridmet/single-year/<location>/<ranch>/<pasture>/')
def gridmet_singleyear_pasture(location, ranch, pasture):
    year = request.args.get('year', datetime.now().year)
    year = int(year)

    loc_path = _join(RANGESAT_DIR, location)
    if exists(loc_path):
        _location = Location(loc_path)
        ranch = ranch.replace("'", "~").replace(' ', '_')
        pasture = pasture.replace("'", "~").replace(' ', '_')
        d = load_gridmet_single_year(_join(_location.loc_path, 'gridmet', ranch, pasture), year)
        return jsonify(d)
    else:
        return jsonify(None)


@app.route('/gridmet/annual-progression/<location>/<ranch>/<pasture>')
@app.route('/gridmet/annual-progression/<location>/<ranch>/<pasture>/')
def gridmet_annualprogression_pasture(location, ranch, pasture):
    start_year = request.args.get('start_year', None)
    end_year = request.args.get('end_year', None)

    loc_path = _join(RANGESAT_DIR, location)
    if exists(loc_path):
        _location = Location(loc_path)
        ranch = ranch.replace("'", "~").replace(' ', '_')
        pasture = pasture.replace("'", "~").replace(' ', '_')
        d = load_gridmet_annual_progression(_join(_location.loc_path, 'gridmet', ranch, pasture), start_year, end_year)
        return jsonify(d)
    else:
        return jsonify(None)

@app.route('/histogram/single-scene/<location>/<ranch>/<pasture>')
@app.route('/histogram/single-scene/<location>/<ranch>/<pasture>/')
def histogram_singlescene_pasture(location, ranch, pasture):
    product_id = request.args.get('product_id', _scenemeta_location_latest(location))
    product = request.args.get('product', 'biomass')

    loc_path = _join(RANGESAT_DIR, location)
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


@app.route('/histogram/intra-year/<location>/<ranch>/<pasture>')
@app.route('/histogram/intra-year/<location>/<ranch>/<pasture>/')
def intrayear_histogram(location, ranch, pasture):
    year = request.args.get('year', None)
    start_date = request.args.get('start_date', None)
    end_date = request.args.get('end_date', None)
    product = request.args.get('product', 'biomass')

    loc_path = _join(RANGESAT_DIR, location)
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


@app.route('/histogram/inter-year/<location>/<ranch>/<pasture>')
@app.route('/histogram/inter-year/<location>/<ranch>/<pasture>/')
def interyear_histogram(location, ranch, pasture):
    start_year = request.args.get('start_year', None)
    end_year = request.args.get('end_year', None)
    start_date = request.args.get('start_date', None)
    end_date = request.args.get('end_date', None)
    product = request.args.get('product', 'biomass')

    loc_path = _join(RANGESAT_DIR, location)
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


if __name__ == '__main__':
    app.run(debug=True)
# rsync -av --progress --exclude temp rangesat-biomass/ rogerlew@rangesat.nkn.uidaho.edu:/var/www/rangesat-biomass
