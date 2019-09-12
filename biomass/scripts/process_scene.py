import sys
import yaml
import os
import shutil
import multiprocessing
import csv
import random
import tarfile

from glob import glob
from time import time

from os.path import join as _join
from os.path import exists as _exists
from os.path import split as _split

import fiona
import rasterio

sys.path.append(os.path.abspath('../../'))
from biomass.landsat import LandSatScene
from biomass.rangesat_biomass import ModelPars, SatModelPars, BiomassModel
from biomass.all__your_base import get_sf_wgs_bounds


def extract(tar_fn, dst):
    tar = tarfile.open(tar_fn)
    tar.extractall(path=dst)
    tar.close()


def process_scene(scn_fn, verbose=True):
    global models, out_dir, sf, bbox, sf_feature_properties_key

    assert '.tar.gz' in scn_fn
    if verbose:
        print(scn_fn, out_dir)

    print('extracting...')
    scn_path = scn_fn.replace('.tar.gz', '')
    extract(scn_fn, scn_path)

    # Load and crop LandSat Scene
    print('load')
    _ls = LandSatScene(scn_path)

    print('clip')
    ls =_ls.clip(bbox, out_dir)

    print('ls.basedir', ls.basedir)
    # Build biomass model
    bio_model = BiomassModel(ls, models)

    # Export grids
    bio_model.export_grids(biomass_dir=_join(ls.basedir, 'biomass'))

    # Analyze pastures
    res = bio_model.analyze_pastures(sf, sf_feature_properties_key)

    # get a summary dictionary of the landsat scene
    ls_summary = ls.summary_dict()

    return dict(res=res, ls_summary=ls_summary)


def dump_pasture_stats(results, dst_fn):
    with open(dst_fn, 'w', newline='') as _fp:
        fieldnames = ['product_id', 'key', 'total_px', 'snow_px', 'water_px',
                      'aerosol_px', 'valid_px', 'coverage', 'area_ha',
                      'model', 'biomass_mean_gpm', 'biomass_ci90_gpm',
                      'biomass_10pct_gpm', 'biomass_75pct_gpm', 'biomass_90pct_gpm',
                      'biomass_total_kg', 'biomass_sd_gpm', 'summer_vi_mean_gpm',
                      'fall_vi_mean_gpm', 'fraction_summer',
                      'product_id', 'satellite', 'acquisition_date',
                      'wrs', 'bounds', 'valid_pastures_cnt']

        writer = csv.DictWriter(_fp, fieldnames=fieldnames)
        writer.writeheader()
        for _res_d in results:  # scene
            _res = _res_d['res']
            _ls_summary = _res_d['ls_summary']

            for _pasture in _res:  # pasture
                _model_stats = _pasture['model_stats']
                del _pasture['model_stats']

                for _model, _model_d in _model_stats.items():
                    _model_d = _model_d.asdict()
                    _model_d.update(_pasture)
                    _model_d.update(_ls_summary)
                    writer.writerow(_model_d)

#
# INITIALIZE GLOBAL VARIABLES
#
# This variables need to be in the global scope so that they
# work with multiprocessing.


if __name__ == '__main__':

    cfg_fn = sys.argv[-2]
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

    # open shape file and determine the bounds
    sf_fn = _d['sf_fn']
    sf_fn = os.path.abspath(sf_fn)
    sf = fiona.open(sf_fn, 'r')
    bbox = get_sf_wgs_bounds(sf_fn)

    landsat_scene_directory = _d['landsat_scene_directory']
    wrs_blacklist = _d.get('wrs_blacklist', None)

    sf_feature_properties_key = _d.get('sf_feature_properties_key', 'key')

    out_dir = _d['out_dir']

    scene_fn = sys.argv[-1]
    res = process_scene(scene_fn)

    prefix = _split(scene_fn)[-1].replace('.tar.gz', '')
    dump_pasture_stats([res], _join(out_dir, '%s_pasture_stats.csv' % prefix))
