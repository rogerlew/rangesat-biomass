# Copyright: University of Idaho (2019)
# Author:    Roger Lew (rogerlew@uidaho.edu)
# Date:      4/20/2019
# License:   BSD-3 Clause

import sys
import yaml
import os
import shutil
import multiprocessing
import csv
import random

from glob import glob
from time import time
from math import sqrt

from os.path import join as _join
from os.path import exists as _exists
from os.path import split as _split

import pyproj
import fiona
from rasterio.mask import raster_geometry_mask

import numpy as np
from osgeo import osr

from landsat import LandSatScene


class SatModelPars(object):
    def __init__(self, satellite, ndvi_threshold, summer_int,
                 summer_slp, fall_int, fall_slp, required_coverage, minimum_area_ha):
        self.satellite = satellite
        self.ndvi_threshold = ndvi_threshold
        self.summer_int = summer_int
        self.summer_slp = summer_slp
        self.fall_int = fall_int
        self.fall_slp = fall_slp
        self.required_coverage = required_coverage
        self.minimum_area_ha = minimum_area_ha


class ModelPars(object):
    def __init__(self, name, sat_pars):
        self.name = name
        self._sat_pars = sat_pars

    def __getitem__(self, sat):
        return self._sat_pars[sat]


def wkt_2_proj4(wkt):
    srs = osr.SpatialReference()
    srs.ImportFromWkt(wkt)
    return srs.ExportToProj4().strip()


wgs84_proj4 = '+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs'


def get_sf_wgs_bounds(_sf_fn):
    """
    returns the bbox of the features in a fiona.shapefile
    """
    _sf = fiona.open(_sf_fn, 'r')
    bboxs = []
    for feature in _sf:
        bboxs.append(fiona.bounds(feature))

    bboxs = np.array(bboxs)
    e, s, w, n = [np.min(bboxs[:, 0]), np.min(bboxs[:, 1]),
                  np.max(bboxs[:, 2]), np.max(bboxs[:, 3])]

    proj_wkt = open(_sf_fn.replace('.shp', '') + '.prj').read()
    sf_proj4 = wkt_2_proj4(proj_wkt)
    sf_proj = pyproj.Proj(sf_proj4)
    wgs_proj = pyproj.Proj(wgs84_proj4)
    e, s = pyproj.transform(sf_proj, wgs_proj, e, s)
    w, n = pyproj.transform(sf_proj, wgs_proj, w, n)

    return e, s, w, n


def process_scene(scn_fn, verbose=True, cellsize=30):
    global models, out_dir, sf, bbox

    if verbose:
        print(scn_fn, out_dir)

    #
    # Load and crop LandSat Scene
    #
    ls = LandSatScene(scn_fn).clip(bbox, out_dir)
    sat = ls.satellite

    #
    # Build data mask
    #
    # load the masks the pixel_qa band and aerosol_mask
    # true where not valid
    aerosol_mask = ls.threshold_aerosol()
    qa_notclear = ls.qa_notclear
    qa_snow = ls.qa_snow
    qa_water = ls.qa_water

    # build a composite mask
    qa_mask = aerosol_mask + qa_notclear + qa_snow + qa_water
    qa_mask = qa_mask > 0
    not_qa_mask = np.logical_not(qa_mask)

    #
    # Build the biomass model
    #
    _nbr = np.ma.array(ls.nbr, mask=qa_mask)
    _nbr2 = np.ma.array(ls.nbr2, mask=qa_mask)

    # `models` contains a list of Models. e.g. Shrub and Herbaceous.
    # Each Model has parameters for multiple satellites.
    # Here we construct grids for each of the Model in the models list.

    # summer vegetation is based on NBR. It is applied when NDVI is greater than the ndvi_threshold
    # specified in the Model parameters.
    summer_mask = {m.name: ls.threshold_ndvi(m[sat].ndvi_threshold, mask=qa_mask) for m in models}
    summer_vi = {m.name: summer_mask[m.name] *
                 np.clip(m[sat].summer_int + m[sat].summer_slp * _nbr, a_min=0, a_max=None)
                 for m in models}

    # fall is based on NBR2. It is applied when NDVI is less than or equal to the ndvi_threshold
    fall_mask = {m.name: np.logical_not(summer_mask[m.name]) for m in models}
    fall_vi = {m.name: fall_mask[m.name] *
               np.clip(m[sat].fall_int + m[sat].fall_slp * _nbr2, a_min=0, a_max=None)
               for m in models}

    # biomass is the sum of fall and summer
    biomass = {m.name: summer_vi[m.name] + fall_vi[m.name] for m in models}

    #
    # Export the grids to a "biomass" subdirectory of the cropped landsat scene.
    #
    biomass_dir = _join(ls.basedir, 'biomass')
    if not _exists(biomass_dir):
        os.makedirs(biomass_dir)

    for name, data in biomass.items():
        ls.dump(data, _join(biomass_dir, '%s_biomass.tif' % name))

    for name, data in fall_vi.items():
        ls.dump(data, _join(biomass_dir, '%s_fall_vi.tif' % name))

    for name, data in summer_vi.items():
        ls.dump(data, _join(biomass_dir, '%s_summer_vi.tif' % name))

    #
    # Iterate over each pasture and determine the biomass, etc. for each model
    #
    res = []  # becomes a list of dictionary objects for each pasture
    valid_pastures_cnt = 0
    for feature in sf:
        key = feature['properties']['PastID_Key']
        features = [feature['geometry']]

        # true where valid
        pasture_mask, _, _ = raster_geometry_mask(ls.template_ds, features)
        not_pasture_mask = np.logical_not(pasture_mask)

        total_px = np.sum(not_pasture_mask)
        snow_px = np.sum(np.ma.array(qa_snow, mask=pasture_mask))
        water_px = np.sum(np.ma.array(qa_water, mask=pasture_mask))
        aerosol_px = np.sum(np.ma.array(aerosol_mask, mask=pasture_mask))
        valid_px = np.sum(np.ma.array(not_qa_mask, mask=pasture_mask))

        # catch the case where all the pasture grid cells are masked
        if isinstance(valid_px, np.ma.core.MaskedConstant):
            valid_px = 0

        # not really sure why this happens, it is very infrequent
        if not total_px > 0:
            coverage = 0.0
        else:
            coverage = float(valid_px) / float(total_px)

        area_ha = total_px * cellsize * cellsize * 0.0001

        model_stats = {}  # dictionary of dictionaries for each model
        for m in models:
            m_sat = m[sat]

            d = {'model': m.name, 'biomass_mean_gpm': None, 'biomass_ci90_gpm': None,
                 'biomass_10pct_gpm': None, 'biomass_75pct_gpm': None, 'biomass_90pct_gpm': None,
                 'biomass_total_kg': None, 'biomass_sd_gpm': None, 'summer_vi_mean_gpm': None,
                 'fall_vi_mean_gpm': None, 'fraction_summer': None}

            if coverage > m_sat.required_coverage and area_ha > m_sat.minimum_area_ha:
                # get masked array of the biomass
                pasture_biomass = np.ma.array(biomass[m.name], mask=pasture_mask)

                # calculate the average biomass of each pixel in grams/meter^2
                d['biomass_mean_gpm'] = np.mean(pasture_biomass)

                # calculate the total estimated biomass based on the area of the pasture
                d['biomass_total_kg'] = d['biomass_mean_gpm'] * area_ha * 10

                # determine the 10th, 75th and 90th percentiles of the distribution
                percentiles = np.quantile(pasture_biomass, [0.1, 0.75, 0.9])
                d['biomass_10pct_gpm'] = percentiles[0]
                d['biomass_75pct_gpm'] = percentiles[1]
                d['biomass_90pct_gpm'] = percentiles[2]

                # calculate the standard deviation of biomass in the pasture
                d['biomass_sd_gpm'] = np.std(pasture_biomass)

                # calculate a 90% confidence interval for biomass_mean_gpm
                d['biomass_ci90_gpm'] = 1.645 * (d['biomass_sd_gpm'] / sqrt(valid_px))

                # calculate summer and winter mean gpms
                d['summer_vi_mean_gpm'] = np.mean(np.ma.array(summer_vi[m.name], mask=pasture_mask))
                d['fall_vi_mean_gpm'] = np.mean(np.ma.array(fall_vi[m.name], mask=pasture_mask))

                # calculate the fraction of the pasture that is above the ndvi_threshold
                d['fraction_summer'] = np.sum(np.ma.array(summer_mask[m.name], mask=pasture_mask))
                d['fraction_summer'] /= float(total_px)

                # keep track of the number of valid pastures models as a quality measure for the scene
                # this can be more than the number of pastures if there is more than 1 model
                valid_pastures_cnt += 1

            # store the model results
            model_stats[m.name] = d

        # store the pasture results
        res.append(dict(product_id=ls.product_id, key=key,
                        total_px=total_px, area_ha=area_ha,
                        snow_px=snow_px, water_px=water_px,
                        aerosol_px=aerosol_px, valid_px=valid_px,
                        coverage=coverage, model_stats=model_stats))

    #
    # Package and return the results
    #

    # get a summary dictionary of the landsat scene
    ls_summary = ls.summary_dict()
    ls_summary['valid_pastures'] = valid_pastures_cnt

    return dict(res=res, ls_summary=ls_summary)

#
# INITIALIZE GLOBAL VARIABLES
#


cfg_fn = sys.argv[-1]
assert cfg_fn.endswith('.yaml'), "Is %s a config file?" % cfg_fn

with open(cfg_fn) as fp:
    _d = yaml.safe_load(fp)

_models = _d['models']
models = []
for _m in _models:
    _satellite_pars = {}
    for pars in _m['satellite_pars']:
        _satellite_pars[pars['satellite']] = SatModelPars(**pars)
    models.append(ModelPars(_m['name'], _satellite_pars))

# open shape file and determine the
sf_fn = _d['sf_fn']
sf_fn = os.path.abspath(sf_fn)
sf = fiona.open(sf_fn, 'r')
bbox = get_sf_wgs_bounds(sf_fn)

landsat_scene_directory = _d['landsat_scene_directory']
wrs_blacklist = _d.get('wrs_blacklist', None)

out_dir = _d['out_dir']

if __name__ == '__main__':
    t0 = time()

    if _exists(out_dir):
        shutil.rmtree(out_dir)

    os.makedirs(out_dir)

    # find all the scenes
    fns = glob(_join(landsat_scene_directory, '*'))
    fns = [fn for fn in fns if os.path.isdir(fn) and _split(fn)[-1].startswith('L')]
    if wrs_blacklist is not None:
        fns = [fn for fn in fns if os.path.isdir(fn) and _split(fn)[-1][4:10] not in wrs_blacklist]

    random.shuffle(fns)

    # run the model
    p = multiprocessing.Pool(multiprocessing.cpu_count())
    results = p.map(process_scene, fns)
    sf.close()

    # export the results
    fp = open(_join(out_dir, 'pasture_stats.csv'), 'w', newline='')

    fieldnames = ['product_id', 'key', 'total_px', 'snow_px', 'water_px',
                  'aerosol_px', 'valid_px', 'coverage', 'area_ha',
                  'model', 'biomass_mean_gpm', 'biomass_ci90_gpm',
                  'biomass_10pct_gpm', 'biomass_75pct_gpm', 'biomass_90pct_gpm',
                  'biomass_total_kg', 'biomass_sd_gpm', 'summer_vi_mean_gpm',
                  'fall_vi_mean_gpm', 'fraction_summer',
                  'product_id', 'satellite', 'acquisition_date',
                  'wrs', 'bounds', 'valid_pastures']

    writer = csv.DictWriter(fp, fieldnames=fieldnames)
    writer.writeheader()
    for _res_d in results:  # scene
        _res = _res_d['res']
        _ls_summary = _res_d['ls_summary']

        for _pasture in _res:  # pasture
            _model_stats = _pasture['model_stats']
            del _pasture['model_stats']

            for _model, _model_d in _model_stats.items():
                _model_d.update(_pasture)
                _model_d.update(_ls_summary)
                writer.writerow(_model_d)

    fp.close()

    print('processed %i scenes in %f seconds' % (len(fns), time() - t0))
