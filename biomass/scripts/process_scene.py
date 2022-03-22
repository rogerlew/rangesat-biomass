import sys
import yaml
import os
import shutil
from subprocess import Popen
import multiprocessing
import csv
import random
import tarfile
# from subprocess import Popen
from glob import glob
from time import time

from pathlib import Path

from os.path import join as _join
from os.path import exists as _exists
from os.path import split as _split

import fiona
import rasterio

sys.path.append(os.path.abspath('../../'))
sys.path.insert(0, '/Users/roger/rangesat-biomass')

from biomass.landsat import LandSatScene, get_gz_scene_bounds
from biomass.rangesat_biomass import ModelPars, SatModelPars, BiomassModel
from all_your_base import get_sf_wgs_bounds, bounds_intersect, bounds_contain, SCRATCH


def extract(tar_fn, dst):
    print(tar_fn, dst)

    # cmd = ['tar', '-xvf', tar_fn, '-C', dst]
    # p = Popen(cmd)
    # p.wait()

    tar = tarfile.open(tar_fn)
    tar.extractall(path=dst)
    tar.close()


def process_scene(scn_fn, verbose=True):
    global models, out_dir, sf, bbox, sf_feature_properties_key, sf_feature_properties_delimiter

    assert '.tar.gz' in scn_fn
    if verbose:
        print(scn_fn, out_dir)

    print('extracting...')
    scn_path = scn_fn.replace('.tar.gz', '')
    if _exists(SCRATCH):
        scn_path = _join(SCRATCH, _split(scn_path)[-1])
    extract(scn_fn, scn_path)

    # Load and crop LandSat Scene
    print('load')
    _ls = LandSatScene(scn_path)

    try:
        print('clip')
        ls = _ls.clip(bbox, out_dir)
    except:
        ls = None
        _ls = None
        shutil.rmtree(scn_path)
        Path(_join(out_dir, '.{}'.format(_split(scn_path)[-1]))).touch()
        raise

    _ls.dump_rgb(_join(ls.basedir, 'rgb.tif'), gamma=1.5)

    print('ls.basedir', ls.basedir)
    # Build biomass model
    bio_model = BiomassModel(ls, models)

    # Export grids
    print('exporting grids')
    bio_model.export_grids(biomass_dir=_join(ls.basedir, 'biomass'))

    # Analyze pastures
    print('analyzing pastures')
    res = bio_model.analyze_pastures(sf, sf_feature_properties_key, sf_feature_properties_delimiter)

    # get a summary dictionary of the landsat scene
    print('compiling summary')
    ls_summary = ls.summary_dict()

    print('reprojecting scene')
    scn_dir = _join(out_dir, _ls.product_id)
    reproject_scene(scn_dir)

#    ls = None
#    _ls = None
#    shutil.rmtree(scn_path)

    return dict(res=res, ls_summary=ls_summary)


def _contains_any(target, matches):
    for match in matches:
        if match in target:
            return True

    return False


def reproject_scene(scn_dir):
    fns = glob(_join(scn_dir, '*.tif'))
    fns = [fn for fn in fns if _contains_any(fn, ['rgb', 'ndvi'])]
    fns.extend(glob(_join(scn_dir, '*/*.tif')))
    fns = [fn for fn in fns if not fn.endswith('.wgs.tif')]
    for fn in fns:
        reproject_raster(fn)


def reproject_raster(src):
    dst = src[:-4] + '.wgs.vrt'
    dst2 = src[:-4] + '.wgs.tif'

    if _exists(dst):
        os.remove(dst)
    if _exists(dst2):
        os.remove(dst2)

    cmd = ['gdalwarp', '-t_srs', 'EPSG:4326', '-of', 'vrt', src, dst]
    p = Popen(cmd)
    p.wait()

    cmd = ['gdal_translate', '-co', 'COMPRESS=LZW', '-of', 'GTiff', dst, dst2]
    p = Popen(cmd)
    p.wait()
    assert _exists(dst)


def dump_pasture_stats(results, dst_fn):
    with open(dst_fn, 'w', newline='') as _fp:
        fieldnames = ['product_id', 'key', 'total_px', 'snow_px', 'water_px',
                      'aerosol_px', 'valid_px', 'coverage', 'area_ha',
                      'model', 'biomass_mean_gpm', 'biomass_ci90_gpm',
                      'biomass_10pct_gpm', 'biomass_50pct_gpm', 'biomass_75pct_gpm', 'biomass_90pct_gpm',
                      'biomass_total_kg', 'biomass_sd_gpm', 'summer_vi_mean_gpm',
                      'fall_vi_mean_gpm', 'fraction_summer',
                      'product_id', 'satellite', 'acquisition_date',
                      'wrs', 'bounds', 'wgs_bounds', 'valid_pastures_cnt',
                      'ndvi_mean', 'ndvi_sd', 'ndvi_10pct', 'ndvi_50pct', 'ndvi_75pct', 'ndvi_90pct', 'ndvi_ci90',
                      'nbr_mean', 'nbr_sd', 'nbr_10pct', 'nbr_50pct',  'nbr_75pct', 'nbr_90pct', 'nbr_ci90',
                      'nbr2_mean', 'nbr2_sd', 'nbr2_10pct', 'nbr2_50pct', 'nbr2_75pct', 'nbr2_90pct', 'nbr2_ci90']
        
        writer = csv.DictWriter(_fp, fieldnames=fieldnames)
        writer.writeheader()
        for _res_d in results:  # scene
            _res = _res_d['res']
            _ls_summary = _res_d['ls_summary']

            for _pasture in _res:  # pasture
                _model_stats = _pasture['model_stats']
                _ls_stats = _pasture['ls_stats']
                del _pasture['model_stats']
                del _pasture['ls_stats']

                for _model, _model_d in _model_stats.items():
                    _model_d = _model_d.asdict()
                    _model_d.update(_pasture)
                    _model_d.update(_ls_summary)
                    _model_d.update(_ls_stats)
                    writer.writerow(_model_d)

#
# INITIALIZE GLOBAL VARIABLES
#
# This variables need to be in the global scope so that they
# work with multiprocessing.


if __name__ == '__main__':

    from all_your_base import GEODATA_DIRS
    GEODATA = GEODATA_DIRS[0]

    cfg_fn = sys.argv[-2]
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

    sf_feature_properties_key = _d.get('sf_feature_properties_key', 'key')
    sf_feature_properties_delimiter = _d.get('sf_feature_properties_delimiter', '+')

    out_dir = _d['out_dir']

    scene_fn = sys.argv[-1]

    scn_bounds = get_gz_scene_bounds(scene_fn)
    if not bounds_contain(bbox, scn_bounds):
        print('bounds do not intersect', bbox, scn_bounds)
        Path(_join(out_dir, '.{}'.format(_split(scene_fn.replace('.tar.gz', ''))[-1]))).touch()
        sys.exit()

    res = process_scene(scene_fn)

    prefix = os.path.basename(os.path.normpath(scene_fn)).replace('.tar.gz', '')

    dump_pasture_stats([res], _join(out_dir, '%s_pasture_stats.csv' % prefix))

