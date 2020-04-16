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
from all_your_base import get_sf_wgs_bounds, GEODATA_DIRS, SCRATCH
import subprocess

from time import time


GEODATA = GEODATA_DIRS[0]


def reprocess_scene(scn_fn):
    p = subprocess.Popen(['python3', 'reprocess_scene.py', cfg_fn, scn_fn],
                         stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    p.wait()

    scn_path = scn_fn.replace('.tar.gz', '')
    if _exists(SCRATCH):
        scn_path = _join(SCRATCH, _split(scn_path)[-1])
    if _exists(scn_path):
        shutil.rmtree(scn_path)


def is_processed(fn):
    global out_dir
    _fn = _split(fn)[-1].split('-')[0]
    res = glob(_join(out_dir, '{}_*_{}_{}_*_{}_{}'
               .format(_fn[:4], _fn[4:10], _fn[10:18], _fn[18:20], _fn[20:22])))
    return len(res) > 0

#
# INITIALIZE GLOBAL VARIABLES
#
# This variables need to be in the global scope so that they
# work with multiprocessing.


cfg_fn = sys.argv[-1]
assert cfg_fn.endswith('.yaml'), "Is %s a config file?" % cfg_fn

with open(cfg_fn) as fp:
    yaml_txt = fp.read()
    yaml_txt = yaml_txt.replace('{GEODATA}', GEODATA)
    _d = yaml.safe_load(yaml_txt)

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
wrs_whitelist = _d.get('wrs_whitelist', None)
years = _d.get('years', None)
if years is not None:
    years = [int(yr) for yr in years]

sf_feature_properties_key = _d.get('sf_feature_properties_key', 'key')

out_dir = _d['out_dir']

if __name__ == '__main__':
    t0 = time()
    use_multiprocessing = False


    # find all the scenes
    fns = glob(_join(out_dir, 'L*'))
    fns = [fn for fn in fns if os.path.isdir(fn)]

    print(fns)

    if use_multiprocessing:
        # run the model
        pool = multiprocessing.Pool(multiprocessing.cpu_count())
        _results = pool.map(reprocess_scene, fns)
    else:
        _results = []
        n = len(fns)
        for i, fn in enumerate(fns):
            t0 = time()
            print('{}\t{} of {}...'.format(fn, i+1, n), end='')
            _results.append(reprocess_scene(fn))
            print(time()-t0)

    sf.close()
    print('processed %i scenes in %f seconds' % (len(fns), time() - t0))
