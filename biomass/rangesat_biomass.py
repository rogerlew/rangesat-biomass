# Copyright: University of Idaho (2019)
# Author:    Roger Lew (rogerlew@uidaho.edu)
# Date:      4/20/2019
# License:   BSD-3 Clause

import os
from math import sqrt

from os.path import join as _join
from os.path import exists as _exists
import warnings

import rasterio
from rasterio.mask import raster_geometry_mask

from fiona.transform import transform_geom

import numpy as np
from .landsat import LandSatScene


def _quantile(a, q):
    if type(a) is np.ma.masked_array:
        _a = a[~a.mask]
        if len(_a) == 0:
            return [None for x in q]
        return np.quantile(_a, q)

    return np.quantile(a, q)


class SatModelPars(object):
    def __init__(self, satellite, discriminate_threshold, summer_int,
                 summer_slp, fall_int, fall_slp, required_coverage, minimum_area_ha,
                 summer_index='nbr', fall_index='ndti', discriminate_index='ndvi', log_transformed_estimate=False):

        self.satellite = satellite
        self.discriminate_threshold = discriminate_threshold
        self.discriminate_index = discriminate_index
        self.summer_int = summer_int
        self.summer_slp = summer_slp
        self.summer_index = summer_index
        self.fall_int = fall_int
        self.fall_slp = fall_slp
        self.fall_index = fall_index
        self.required_coverage = required_coverage
        self.minimum_area_ha = minimum_area_ha
        self.log_transformed_estimate = log_transformed_estimate


class ModelPars(object):
    def __init__(self, name, sat_pars):
        self.name = name
        self._sat_pars = sat_pars

    def __getitem__(self, sat):
        return self._sat_pars[sat]


class ModelStat(object):
    def __init__(self, model=None, biomass_mean_gpm=None, biomass_ci90_gpm=None,
                 biomass_10pct_gpm=None, biomass_75pct_gpm=None, biomass_90pct_gpm=None,
                 biomass_total_kg=None, biomass_sd_gpm=None, summer_vi_mean_gpm=None,
                 fall_vi_mean_gpm=None, fraction_summer=None):
        self.model = model
        self.biomass_mean_gpm = biomass_mean_gpm
        self.biomass_ci90_gpm = biomass_ci90_gpm
        self.biomass_10pct_gpm = biomass_10pct_gpm
        self.biomass_75pct_gpm = biomass_75pct_gpm
        self.biomass_90pct_gpm = biomass_90pct_gpm
        self.biomass_total_kg = biomass_total_kg
        self.biomass_sd_gpm = biomass_sd_gpm
        self.summer_vi_mean_gpm = summer_vi_mean_gpm
        self.fall_vi_mean_gpm = fall_vi_mean_gpm
        self.fraction_summer = fraction_summer

    def asdict(self):
        return dict(model=self.model,
                    biomass_mean_gpm=self.biomass_mean_gpm,
                    biomass_ci90_gpm=self.biomass_ci90_gpm,
                    biomass_10pct_gpm=self.biomass_10pct_gpm,
                    biomass_75pct_gpm=self.biomass_75pct_gpm,
                    biomass_90pct_gpm=self.biomass_90pct_gpm,
                    biomass_total_kg=self.biomass_total_kg,
                    biomass_sd_gpm=self.biomass_sd_gpm,
                    summer_vi_mean_gpm=self.summer_vi_mean_gpm,
                    fall_vi_mean_gpm=self.fall_vi_mean_gpm,
                    fraction_summer=self.fraction_summer)


class BiomassModel(object):
    def __init__(self, ls: LandSatScene, models: ModelPars, verbose=False):

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

        if verbose:
            print('not_qa_mask', np.sum(not_qa_mask))

        #
        # Build the biomass models
        #
        summer_mask = {}
        summer_vi = {}
        fall_vi = {}
        biomass = {}

        for m in models:

            # summer
            summer_index = np.ma.array(ls.get_index(m[sat].summer_index), mask=qa_mask)
            if m[sat].summer_slp < 0 and m[sat].summer_int == 0:
                a_min = None
            else:
                a_min = 0.0
            summer_vi[m.name] = not_qa_mask * (m[sat].summer_int + m[sat].summer_slp * summer_index)
            if a_min is not None:
                summer_vi[m.name] = np.clip(summer_vi[m.name], a_min=a_min, a_max=None)

            if m[sat].log_transformed_estimate:
                summer_vi[m.name] = np.exp(summer_vi[m.name])

            # fall
            fall_index = np.ma.array(ls.get_index(m[sat].fall_index), mask=qa_mask)
            if m[sat].fall_slp < 0 and m[sat].fall_int == 0:
                a_min = None
            else:
                a_min = 0.0
            fall_vi[m.name] = not_qa_mask * (m[sat].fall_int + m[sat].fall_slp * fall_index)
            if a_min is not None:
                fall_vi[m.name] = np.clip(fall_vi[m.name], a_min=a_min, a_max=None)

            if m[sat].log_transformed_estimate:
                fall_vi[m.name] = np.exp(fall_vi[m.name])

            # biomass
            has_threshold = not str(m[sat].discriminate_index).lower().startswith('none')
            if has_threshold:
                summer_mask[m.name] = ls.threshold(m[sat].discriminate_index, m[sat].discriminate_threshold, qa_mask)
                biomass[m.name] = summer_mask[m.name] * summer_vi[m.name] + np.logical_not(summer_mask[m.name]) * fall_vi[m.name]
            else:
                biomass[m.name] = summer_vi[m.name] + fall_vi[m.name]
                summer_mask[m.name] = None

            if verbose:
                print('summer')
                print(m[sat].summer_index)
                print(m[sat].summer_int)
                print(m[sat].summer_slp)
                print('summer_index', np.mean(summer_index))
                print('summer_vi[m.name]', np.mean(summer_vi[m.name]))
                print(m[sat].fall_index)
                print(m[sat].fall_int)
                print(m[sat].fall_slp)
                print('fall_index', np.mean(fall_index))
                print('fall_vi[m.name]', np.mean(fall_vi[m.name]))
                print('biomass[m.name]', np.mean(biomass[m.name]))

        self.ls = ls
        self.aerosol_mask = aerosol_mask
        self.qa_notclear = qa_notclear
        self.qa_snow = qa_snow
        self.qa_water = qa_water
        self.qa_mask = qa_mask
        self.not_qa_mask = not_qa_mask
        self.summer_mask = summer_mask
        self.summer_vi = summer_vi
        self.fall_vi = fall_vi
        self.biomass = biomass
        self.models = models

        self.ndvi = np.ma.array(ls.ndvi, mask=qa_mask)
        self.nbr = np.ma.array(ls.nbr, mask=qa_mask)
        self.nbr2 = np.ma.array(ls.nbr2, mask=qa_mask)

    def export_grids(self, biomass_dir, dtype=rasterio.float32):
        """
        Export the grids to a "biomass" subdirectory of the cropped landsat scene.

        :param biomass_dir:
        :return:
        """
        ls = self.ls
        biomass = self.biomass
        fall_vi = self.fall_vi
        summer_vi = self.summer_vi

        if not _exists(biomass_dir):
            os.makedirs(biomass_dir)

        for name, data in biomass.items():
            data = np.array(np.round(data), dtype=dtype)
            ls.dump(data, _join(biomass_dir, '%s_biomass.tif' % name), dtype=dtype)

        for name, data in fall_vi.items():
            data = np.array(np.round(data), dtype=dtype)
            ls.dump(data, _join(biomass_dir, '%s_fall_vi.tif' % name), dtype=dtype)

        for name, data in summer_vi.items():
            data = np.array(np.round(data), dtype=dtype)
            ls.dump(data, _join(biomass_dir, '%s_summer_vi.tif' % name), dtype=dtype)

        ls_dir = _join(os.path.abspath(biomass_dir), os.path.pardir)
        data = np.array(np.round(self.ndvi), dtype=dtype)
        ls.dump(data, _join(ls_dir, '%s_ndvi.tif' % ls.product_id), dtype=dtype)


    def analyze_pastures(self, sf, sf_feature_properties_key, sf_feature_properties_delimiter='+'):
        """
        Iterate over each pasture and determine the biomass, etc. for each model

        :param sf:
        :return:
        """
        

        ls = self.ls
        sat = self.ls.satellite
        cellsize = ls.cellsize
        qa_snow = self.qa_snow
        qa_water = self.qa_water
        aerosol_mask = self.aerosol_mask
        not_qa_mask = self.not_qa_mask
        biomass = self.biomass
        models = self.models
        summer_vi = self.summer_vi
        fall_vi = self.fall_vi
        summer_mask = self.summer_mask

        res = []  # becomes a list of dictionary objects for each pasture
        valid_pastures_cnt = 0
        for feature in sf:
            key = feature['properties'][sf_feature_properties_key]
            features = [transform_geom(sf.crs_wkt, ls.proj4, feature['geometry'])]

            # true where valid
            pasture_mask, _, _ = raster_geometry_mask(ls.template_ds, features)
            not_pasture_mask = np.logical_not(pasture_mask)

            if np.sum(not_pasture_mask) == 0:
                warnings.warn('{} in {} has zero pixels in mask'.format(key, ls.product_id))

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

            pasture, ranch = key.split(sf_feature_properties_delimiter)
            if pasture in 'ABCDEFGHIJKLMNO':
                area_ha = 2.0
                coverage = 1.0

            model_stats = {}  # dictionary of dictionaries for each model
            for m in models:
                m_sat = m[sat]

                d = ModelStat(model=m.name)

                if coverage > m_sat.required_coverage and area_ha > m_sat.minimum_area_ha:

                    # get masked array of the biomass
                    pasture_biomass = np.ma.array(biomass[m.name], mask=pasture_mask)

                    # calculate the average biomass of each pixel in grams/meter^2
                    d.biomass_mean_gpm = np.mean(pasture_biomass)

                    # calculate the total estimated biomass based on the area of the pasture
                    d.biomass_total_kg = d.biomass_mean_gpm * area_ha * 10

                    # determine the 10th, 75th and 90th percentiles of the distribution
                    percentiles = _quantile(pasture_biomass, [0.1, 0.75, 0.9])
                    d.biomass_10pct_gpm = percentiles[0]
                    d.biomass_75pct_gpm = percentiles[1]
                    d.biomass_90pct_gpm = percentiles[2]

                    # calculate the standard deviation of biomass in the pasture
                    d.biomass_sd_gpm = np.std(pasture_biomass)

                    # calculate a 90% confidence interval for biomass_mean_gpm
                    d.biomass_ci90_gpm = 1.645 * (d.biomass_sd_gpm / sqrt(valid_px))

                    # calculate summer and winter mean gpms
                    d.summer_vi_mean_gpm = np.mean(np.ma.array(summer_vi[m.name], mask=pasture_mask))
                    d.fall_vi_mean_gpm = np.mean(np.ma.array(fall_vi[m.name], mask=pasture_mask))

                    # calculate the fraction of the pasture that is above the ndvi_threshold
                    if summer_mask[m.name] is not None:
                        d.fraction_summer = np.sum(np.ma.array(summer_mask[m.name], mask=pasture_mask))
                        d.fraction_summer /= float(total_px)
                    else:
                        d.fraction_summer = None

                    # keep track of the number of valid pastures models as a quality measure for the scene
                    # this can be more than the number of pastures if there is more than 1 model
                    valid_pastures_cnt += 1

                # store the model results
                model_stats[m.name] = d

            ls_stats = {}
            _ndvi = np.ma.array(self.ndvi, mask=pasture_mask)
            ls_stats['ndvi_mean'] = np.mean(_ndvi)
            ls_stats['ndvi_sd'] = np.std(_ndvi)
            _ndvi_percentiles = _quantile(_ndvi, [0.1, 0.75, 0.9])
            ls_stats['ndvi_10pct'] = _ndvi_percentiles[0]
            ls_stats['ndvi_75pct'] = _ndvi_percentiles[1]
            ls_stats['ndvi_90pct'] = _ndvi_percentiles[2]
            ls_stats['ndvi_ci90'] = 1.645 * (ls_stats['ndvi_sd'] / sqrt(valid_px))
        
            _nbr = np.ma.array(self.nbr, mask=pasture_mask)
            ls_stats['nbr_mean'] = np.mean(_nbr)
            ls_stats['nbr_sd'] = np.std(_nbr)
            _nbr_percentiles = _quantile(_nbr, [0.1, 0.75, 0.9])
            ls_stats['nbr_10pct'] = _nbr_percentiles[0]
            ls_stats['nbr_75pct'] = _nbr_percentiles[1]
            ls_stats['nbr_90pct'] = _nbr_percentiles[2]
            ls_stats['nbr_ci90'] = 1.645 * (ls_stats['nbr_sd'] / sqrt(valid_px))

            _nbr2 = np.ma.array(self.nbr2, mask=pasture_mask)
            ls_stats['nbr2_mean'] = np.mean(_nbr2)
            ls_stats['nbr2_sd'] = np.std(_nbr2)
            _nbr2_percentiles = _quantile(_nbr2, [0.1, 0.75, 0.9])
            ls_stats['nbr2_10pct'] = _nbr2_percentiles[0]
            ls_stats['nbr2_75pct'] = _nbr2_percentiles[1]
            ls_stats['nbr2_90pct'] = _nbr2_percentiles[2]
            ls_stats['nbr2_ci90'] = 1.645 * (ls_stats['nbr2_sd'] / sqrt(valid_px))

            # store the pasture results
            res.append(dict(product_id=ls.product_id, key=key,
                            total_px=total_px, area_ha=area_ha,
                            snow_px=snow_px, water_px=water_px,
                            aerosol_px=aerosol_px, valid_px=valid_px,
                            coverage=coverage, valid_pastures_cnt=valid_pastures_cnt,
                            model_stats=model_stats,
                            ls_stats=ls_stats))

        return res
