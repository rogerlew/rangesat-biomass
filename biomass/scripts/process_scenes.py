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
import subprocess

def process_scene(scn_fn):
    p = subprocess.Popen(['C:\python37x64\python.exe', 'process_scene.py', cfg_fn, scn_fn], stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    p.wait()
    shutil.rmtree(scn_fn.replace('.tar.gz', ''))

#
# INITIALIZE GLOBAL VARIABLES
#
# This variables need to be in the global scope so that they
# work with multiprocessing.


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

# open shape file and determine the bounds
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
    use_multiprocessing = True
    if _exists(out_dir):
        shutil.rmtree(out_dir)

    os.makedirs(out_dir)

    # find all the scenes
    fns = glob(_join(landsat_scene_directory, '*.tar.gz'))
    #fns = [fn for fn in fns if os.path.isdir(fn) and _split(fn)[-1].startswith('L')]
    if wrs_blacklist is not None:
        fns = [fn for fn in fns if _split(fn)[-1][4:10] not in wrs_blacklist]

    random.shuffle(fns)

    if use_multiprocessing:
        # run the model
        pool = multiprocessing.Pool(multiprocessing.cpu_count())
        _results = pool.map(process_scene, fns)
    else:
        _results = []
        for fn in fns[:5]:
            _results.append(process_scene(fn))

    sf.close()
    print('processed %i scenes in %f seconds' % (len(fns), time() - t0))
