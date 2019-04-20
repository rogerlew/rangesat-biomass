
import sys
import yaml
import os
import shutil
import multiprocessing
import csv
import random

from glob import glob
from time import time

from os.path import join as _join
from os.path import exists as _exists
from os.path import split as _split

import fiona

sys.path.append(os.path.abspath('../../'))
from biomass.landsat import LandSatScene
from biomass.rangesat_biomass import ModelPars, SatModelPars, get_sf_wgs_bounds, BiomassModel


def process_scene(scn_fn, verbose=True, cellsize=30):
    global models, out_dir, sf, bbox, sf_feature_properties_key

    if verbose:
        print(scn_fn, out_dir)

    # Load and crop LandSat Scene
    ls = LandSatScene(scn_fn).clip(bbox, out_dir)

    # Build biomass model
    bio_model = BiomassModel(ls, models)

    # Export grids
    bio_model.export_grids(biomass_dir=_join(ls.basedir, 'biomass'))

    # Analyze pastures
    res = bio_model.analyze_pastures(sf, sf_feature_properties_key)

    # get a summary dictionary of the landsat scene
    ls_summary = ls.summary_dict()

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

sf_feature_properties_key = _d.get('sf_feature_properties_key', 'key')

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
    pool = multiprocessing.Pool(multiprocessing.cpu_count())
    results = pool.map(process_scene, fns)
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
                _model_d = _model_d.asdict()
                _model_d.update(_pasture)
                _model_d.update(_ls_summary)
                writer.writerow(_model_d)

    fp.close()

    print('processed %i scenes in %f seconds' % (len(fns), time() - t0))
