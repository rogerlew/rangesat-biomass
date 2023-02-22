from glob import glob
import sys
import os
from os.path import split as _split
from os.path import join as _join
from os.path import exists as _exists
from pprint import pprint
import fiona
from fiona.transform import transform_geom
import yaml
import pyproj
import numpy as np
import json
from subprocess import Popen

import rasterio
from rasterio.mask import raster_geometry_mask

from osgeo import gdal
from osgeo import osr

sys.path.insert(0, os.path.abspath('../'))

from all_your_base import GEODATA_DIRS, rat_extract


def wkt_2_proj4(wkt):
    srs = osr.SpatialReference()
    srs.ImportFromWkt(wkt)
    return srs.ExportToProj4().strip()


wgs84_proj4 = '+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs'

GEODATA = GEODATA_DIRS[0]


class Location(object):
    def __init__(self, loc_path):
        cfg_fn = glob(_join(loc_path, '*.yaml'))
        assert len(cfg_fn) == 1, cfg_fn
        cfg_fn = cfg_fn[0]
        with open(cfg_fn) as fp:
            yaml_txt = fp.read()
            yaml_txt = yaml_txt.replace('{GEODATA}', GEODATA)
            _d = yaml.safe_load(yaml_txt)

        self.cfg_fn = cfg_fn
        self._d = _d
        self.rangesat_dir, self.location = _split(loc_path)
        self.loc_path = loc_path

        geojson = glob('{}/*.geojson'.format(loc_path))
        assert len(geojson) == 1, geojson
        self.geojson = geojson[0]

        sf_fn = _join(loc_path, _d['sf_fn'])
        sf_feature_properties_key = _d['sf_feature_properties_key']
        sf_fn = os.path.abspath(sf_fn)
        sf = fiona.open(sf_fn, 'r')

        reverse_key = self.reverse_key

        pastures = {}
        for feature in sf:
            properties = feature['properties']
            key = properties[sf_feature_properties_key].replace(' ', '_')
            
            pasture, ranch = key.split(self.key_delimiter)
            if reverse_key:
                ranch, pasture = pasture, ranch

            if ranch.lower() not in [p.lower() for p in pastures]:
                pastures[ranch] = set()

            pastures[ranch].add(pasture)

        self.pastures = pastures

    @property
    def sf_fn(self):
        return self._d['sf_fn']

    @property
    def sf_feature_properties_key(self):
        return self._d['sf_feature_properties_key']

    @property
    def key_delimiter(self):
        return self._d.get('sf_feature_properties_delimiter', '+')

    def representative_pasture(self, ranch):
        if ranch is not None:
            ranch = ranch.replace(' ', '_')

        if ranch not in self.pastures:
            return None

        area_ha, bbox = self.shape_inspection(ranch)
        pastures = sorted(list(self.pastures[ranch]))

        center_x = (bbox[0] + bbox[2]) / 2.0
        center_y = (bbox[1] + bbox[2]) / 2.0

        distance = 1e38
        rep = None
        for pasture in pastures:
            p_x, p_y = self.serialized_pasture(ranch, pasture)['centroid']
            _distance = (center_x - p_x) ** 2.0 + (center_y - p_y) ** 2.0

            if _distance < distance:
                rep = pasture
                distance = _distance

        return rep

    @property
    def models(self):
        return self._d['models']

    @property
    def out_dir(self):
        return _join(self.loc_path, self._d['out_dir'])

    @property
    def mask_dir(self):
        return _join(self.loc_path, 'raster_masks')

    @property
    def db_fn(self):
        return _join(self.loc_path, self._d['out_dir'], 'sqlite3.db')

    @property
    def scn_cov_db_fn(self):
        return _join(self.loc_path, self._d['out_dir'], 'scenemeta_coverage.db')

    @property
    def ranches(self):
        return sorted(list(self.pastures.keys()))

    def serialized(self):
        area_ha, bbox = self.shape_inspection()

        d = dict(location=self.location, models=self.models, ranches=self.ranches,
                 area_ha=area_ha, bbox=bbox)
        return d


    def extract_pixels_opt(self, raster_fn, ranch=None, pasture=None):

        if ranch is not None:
            ranch = ranch.replace(' ', '_')

        if pasture is not None:
            pasture = pasture.replace(' ', '_')

        ds = rasterio.open(raster_fn)

        if ranch is None and pasture is None:
            x = ds.read(1, masked=True)
            return x.compressed(), x.count() 

        ds_proj4 = ds.crs.to_proj4()
        prj = ('utm', 'wgs')[ds.crs.is_geographic]

        mask_dir = self.mask_dir
        mask_fn = _join(self.mask_dir, f'{ranch}.{prj}.tif')

        ms = rasterio.open(mask_fn)

        # true where valid
        pasture_mask = ms.read(1, masked=True)
        x = np.ma.array(ds.read(1, masked=True), mask=pasture_mask)
        return x.compressed().tolist(), int(x.count())


    def extract_pixels(self, raster_fn, ranch=None, pasture=None):

        if ranch is not None:
            ranch = ranch.replace(' ', '_')

        if pasture is not None:
            pasture = pasture.replace(' ', '_')

        if pasture is None:
           raise NotImplementedError("Cannot process request")

        ds = rasterio.open(raster_fn)
        ds_proj4 = ds.crs.to_proj4()

        loc_path = self.loc_path
        _d = self._d

        sf_fn = _join(loc_path, _d['sf_fn'])
        sf_feature_properties_key = _d['sf_feature_properties_key']
        sf_fn = os.path.abspath(sf_fn)
        sf = fiona.open(sf_fn, 'r')

        features = []
        for feature in sf:
            properties = feature['properties']
            key = properties[sf_feature_properties_key].replace(' ', '_')

            _pasture, _ranch = key.split(self.key_delimiter)
            if self.reverse_key:
                _ranch, _pasture = _pasture, _ranch
    
            if ranch is not None:
                if _ranch.lower() != ranch.lower():
                    continue

            _features = transform_geom(sf.crs_wkt, ds_proj4, feature['geometry'])

            if pasture is None:
                features.append(_features)
            elif _pasture.lower() == pasture.lower():
                features.append(_features)

        data = ds.read(1, masked=True)

        if 'biomass' in raster_fn:
            data = np.ma.masked_values(data, 0)

        pasture_mask, _, _ = raster_geometry_mask(ds, features)

        x = np.ma.MaskedArray(data, mask=pasture_mask)
        return  x.compressed().tolist(), int(x.count())

    def extract_pixels_by_pasture_opt(self, raster_fn, ranch):

        if ranch is not None:
            ranch = ranch.replace(' ', '_')

        ds = rasterio.open(raster_fn)
        ds_proj4 = ds.crs.to_proj4()
        prj = ('utm', 'wgs')[ds.crs.is_geographic]

        loc_path = self.loc_path
        _d = self._d

        data = ds.read(1, masked=True)

        mask_dir = self.mask_dir
        ranch_mask_fn = _join(self.mask_dir, f'{ranch}.{prj}.tif')
        ranch_ms = rasterio.open(ranch_mask_fn)
        ranch_indx = np.where(ranch_ms.read(1) == 0)

        data = data[ranch_indx]

        mask_fn = _join(self.mask_dir, f'{ranch}.pastures.{prj}.tif')
        ms = rasterio.open(mask_fn)

        # true where valid
        pastures_mask = ms.read(1, masked=True)
        pastures_mask = pastures_mask[ranch_indx]

        rat = rat_extract(mask_fn)

#        if 'biomass' in raster_fn:
#            data = np.ma.masked_values(data, 0)

        _data = {}
        for px, row in rat.items():
            _pasture = row['PASTURE']
            x = data[np.where(pastures_mask == px)].tolist()
            _data[(ranch, _pasture)] = x, len(x)

        return _data

    def extract_pixels_by_pasture(self, raster_fn, ranch=None):

        if ranch is not None:
            ranch = ranch.replace(' ', '_')

        ds = rasterio.open(raster_fn)
        ds_proj4 = ds.crs.to_proj4()

        loc_path = self.loc_path
        _d = self._d

        sf_fn = _join(loc_path, _d['sf_fn'])
        sf_feature_properties_key = _d['sf_feature_properties_key']
        sf_fn = os.path.abspath(sf_fn)
        sf = fiona.open(sf_fn, 'r')

        data = ds.read(1, masked=True)

#        if 'biomass' in raster_fn:
#            data = np.ma.masked_values(data, 0)

        _data = {}
        for feature in sf:
            properties = feature['properties']
            key = properties[sf_feature_properties_key].replace(' ', '_')

            _pasture, _ranch = key.split(self.key_delimiter)
            if self.reverse_key:
                _ranch, _pasture = _pasture, _ranch
  
            if ranch is not None:
                if _ranch.lower() != ranch.lower():
                    continue

            _features = transform_geom(sf.crs_wkt, ds_proj4, feature['geometry'])
            pasture_mask, _, _ = raster_geometry_mask(ds, [_features])

            x = np.ma.MaskedArray(data, mask=pasture_mask)
            _data[(_ranch, _pasture)] = x.compressed().tolist(), int(x.count())

        return _data

    def get_pasture_indx(self, raster_fn, pasture, ranch):

        if ranch is not None:
            ranch = ranch.replace(' ', '_')

        if pasture is not None:
            pasture = pasture.replace(' ', '_')

        ds = rasterio.open(raster_fn)
        ds_proj4 = ds.crs.to_proj4()

        target_ranch = ranch.replace(' ', '_').lower().strip()
        target_pasture = pasture.replace(' ', '_').lower().strip()

        loc_path = self.loc_path
        _d = self._d

        sf_fn = _join(loc_path, _d['sf_fn'])
        sf_feature_properties_key = _d['sf_feature_properties_key']
        sf_fn = os.path.abspath(sf_fn)
        sf = fiona.open(sf_fn, 'r')

        features = []
        for feature in sf:
            properties = feature['properties']
            _key = properties[sf_feature_properties_key].replace(' ', '_')

            _pasture, _ranch = _key.split(self.key_delimiter)
            if self.reverse_key:
                _ranch, _pasture = _pasture, _ranch

            if _ranch.lower() == target_ranch and _pasture.lower() == target_pasture:
                _features = transform_geom(sf.crs_wkt, ds_proj4, feature['geometry'])
                features.append(_features)

        if len(features) == 0:
            raise KeyError((ranch, pasture))

        pasture_mask, _, _ = raster_geometry_mask(ds, features)
        indx = np.where(pasture_mask == False)
        return indx

    @property
    def reverse_key(self):
        return self._d.get('reverse_key', False)

    def make_pastures_mask(self, raster_fn, ranch, dst_fn, nodata=-9999):
        """

        :param raster_fn: utm raster
        :param ranches:
        :param dst_fn:
        :param nodata:
        :return:
        """

        assert _exists(_split(dst_fn)[0])
        assert dst_fn.endswith('.tif')

        ds = rasterio.open(raster_fn)
        ds_proj4 = ds.crs.to_proj4()

        loc_path = self.loc_path
        _d = self._d

        sf_fn = _join(loc_path, _d['sf_fn'])
        sf_feature_properties_key = _d['sf_feature_properties_key']
        sf_fn = os.path.abspath(sf_fn)
        sf = fiona.open(sf_fn, 'r')

        reverse_key = self.reverse_key
        pastures = {}
        pastures_mask = np.zeros(ds.shape, dtype=np.uint16)

        for feature in sf:
            properties = feature['properties']
            key = properties[sf_feature_properties_key].replace(' ', '_')

            if not reverse_key:
                _pasture, _ranch = key.split(self.key_delimiter)
            else:
                _ranch, _pasture = key.split(self.key_delimiter)

            if _ranch.lower() != ranch.lower():
                continue

            if _pasture not in pastures:
                pastures[_pasture] = len(pastures) + 1

            # true where valid
            _features = transform_geom(sf.crs_wkt, ds_proj4, feature['geometry'])
            _mask, _, _ = raster_geometry_mask(ds, [_features])
            k = pastures[_pasture]

            # update pastures_mask
            pastures_mask[np.where(_mask == False)] = k

        utm_dst_fn = ''
        try:
            head, tail = _split(dst_fn)
            utm_dst_fn = _join(head, tail.replace('.tif', '.utm.tif'))
            dst_vrt_fn = _join(head, tail.replace('.tif', '.wgs.vrt'))
            dst_wgs_fn = _join(head, tail.replace('.tif', '.wgs.tif'))


            with rasterio.Env():
                profile = ds.profile
                dtype = rasterio.uint16
                profile.update(
                    count=1,
                    dtype=rasterio.uint16,
                    nodata=nodata,
                    compress='lzw')

                with rasterio.open(utm_dst_fn, 'w', **profile) as dst:
                    dst.write(pastures_mask.astype(dtype), 1)

            assert _exists(utm_dst_fn)
        except:
            raise

        try:

            if _exists(dst_vrt_fn):
                os.remove(dst_vrt_fn)

            cmd = ['gdalwarp', '-t_srs', 'EPSG:4326', '-of', 'vrt', utm_dst_fn, dst_vrt_fn]
            p = Popen(cmd)
            p.wait()

            assert _exists(dst_vrt_fn)
        except:
            if _exists(dst_vrt_fn):
                os.remove(dst_vrt_fn)
            raise

        try:
            if _exists(dst_fn):
                os.remove(dst_fn)

            cmd = ['gdal_translate', '-co', 'COMPRESS=LZW', '-of', 'GTiff', dst_vrt_fn, dst_wgs_fn]
            p = Popen(cmd)
            p.wait()

            assert _exists(dst_wgs_fn)
        except:
            if _exists(dst_wgs_fn):
                os.remove(dst_wgs_fn)
            raise

        if _exists(dst_vrt_fn):
            os.remove(dst_vrt_fn)


        for OUTPUT_RASTER in (dst_wgs_fn, utm_dst_fn):
            # https://gdal.org/python/osgeo.gdal.RasterAttributeTable-class.html
            # https://gdal.org/python/osgeo.gdalconst-module.html
            ds = gdal.Open(OUTPUT_RASTER)
            rb = ds.GetRasterBand(1)

	        # Create and populate the RAT
            rat = gdal.RasterAttributeTable()
            rat.CreateColumn('VALUE', gdal.GFT_Integer, gdal.GFU_Generic)
            rat.CreateColumn('PASTURE', gdal.GFT_String, gdal.GFU_Generic)
             
            for i, (pasture, key) in enumerate(pastures.items()):
                rat.SetValueAsInt(i, 0, key)
                rat.SetValueAsString(i, 1, pasture)

            # Associate with the band
            rb.SetDefaultRAT(rat)

            # Close the dataset and persist the RAT
            ds = None


    def make_ranch_mask(self, raster_fn, ranches, dst_fn, nodata=-9999):
        """

        :param raster_fn: utm raster
        :param ranches:
        :param dst_fn:
        :param nodata:
        :return:
        """

        assert _exists(_split(dst_fn)[0])
        assert dst_fn.endswith('.tif')

        ds = rasterio.open(raster_fn)
        ds_proj4 = ds.crs.to_proj4()

        loc_path = self.loc_path
        _d = self._d

        sf_fn = _join(loc_path, _d['sf_fn'])
        sf_feature_properties_key = _d['sf_feature_properties_key']
        sf_fn = os.path.abspath(sf_fn)
        sf = fiona.open(sf_fn, 'r')

        reverse_key = self.reverse_key

        features = []
        for feature in sf:
            properties = feature['properties']
            key = properties[sf_feature_properties_key].replace(' ', '_')

            if not reverse_key:
                _pasture, _ranch = key.split(self.key_delimiter)
            else:
                _ranch, _pasture = key.split(self.key_delimiter)

            if any(_ranch.lower() == r.lower() for r in ranches):
                _features = transform_geom(sf.crs_wkt, ds_proj4, feature['geometry'])
                features.append(_features)

        utm_dst_fn = ''
        try:
            # true where valid
            pasture_mask, _, _ = raster_geometry_mask(ds, features)

            head, tail = _split(dst_fn)
            utm_dst_fn = _join(head, tail.replace('.tif', '.utm.tif'))
            dst_vrt_fn = _join(head, tail.replace('.tif', '.wgs.vrt'))
            dst_wgs_fn = _join(head, tail.replace('.tif', '.wgs.tif'))


            with rasterio.Env():
                profile = ds.profile
                dtype = rasterio.uint8
                profile.update(
                    count=1,
                    dtype=rasterio.uint8,
                    nodata=nodata,
                    compress='lzw')

                with rasterio.open(utm_dst_fn, 'w', **profile) as dst:
                    dst.write(pasture_mask.astype(dtype), 1)

            assert _exists(utm_dst_fn)
        except:
            raise

        try:

            if _exists(dst_vrt_fn):
                os.remove(dst_vrt_fn)

            cmd = ['gdalwarp', '-t_srs', 'EPSG:4326', '-of', 'vrt', utm_dst_fn, dst_vrt_fn]
            p = Popen(cmd)
            p.wait()

            assert _exists(dst_vrt_fn)
        except:
            if _exists(dst_vrt_fn):
                os.remove(dst_vrt_fn)
            raise

        try:
            if _exists(dst_fn):
                os.remove(dst_fn)

            cmd = ['gdal_translate', '-co', 'COMPRESS=LZW', '-of', 'GTiff', dst_vrt_fn, dst_wgs_fn]
            p = Popen(cmd)
            p.wait()

            assert _exists(dst_wgs_fn)
        except:
            if _exists(dst_wgs_fn):
                os.remove(dst_wgs_fn)
            raise

        if _exists(dst_vrt_fn):
            os.remove(dst_vrt_fn)

    def mask_ranch_opt(self, raster_fn, ranch, dst_fn, nodata=-9999, crop=False):

        assert _exists(_split(dst_fn)[0])
        assert dst_fn.endswith('.tif')

        ds = rasterio.open(raster_fn)
        ds_proj4 = ds.crs.to_proj4()
        prj = ('utm', 'wgs')[ds.crs.is_geographic]

        mask_dir = self.mask_dir
        mask_fn = _join(self.mask_dir, f'{ranch}.{prj}.tif')

        ms = rasterio.open(mask_fn)

        # true where valid
        pasture_mask = ms.read(1, masked=True)
        data = np.ma.array(ds.read(1, masked=True), mask=pasture_mask)
        if not crop:
            if isinstance(data, np.ma.core.MaskedArray):
                data.fill_value = nodata
                _data = data.filled()
            else:
                _data = data

            with rasterio.Env():
                profile = ds.profile
                dtype = profile.get('dtype')
                profile.update(
                    count=1,
                    nodata=nodata,
                    compress='lzw')

            with rasterio.open(dst_fn, 'w', **profile) as dst:
                dst.write(_data.astype(dtype), 1)
        else:
            not_pasture_mask = np.logical_not(pasture_mask)
            rows = np.any(not_pasture_mask, axis=1)
            cols = np.any(not_pasture_mask, axis=0)
            rmin, rmax = np.where(rows)[0][[0, -1]]
            cmin, cmax = np.where(cols)[0][[0, -1]]

            data = data[rmin:rmax, cmin:cmax]
            if isinstance(data, np.ma.core.MaskedArray):
                data.fill_value = nodata
                _data = data.filled()
            else:
                _data = data

            #trans = rasteriio.Affine.translation(,0)
            out_transform = rasterio.Affine(ds.transform.a, 
                                            ds.transform.b, 
                                            ds.transform.c + cmin * ds.transform.a, 
                                            ds.transform.d, 
                                            ds.transform.e, 
                                            ds.transform.f + rmin * ds.transform.e) 

            with rasterio.Env():
                profile = ds.profile
                dtype = profile.get('dtype')
                profile.update(
                    count=1,
                    width=cmax-cmin,
                    height=rmax-rmin,
                    transform=out_transform,
                    nodata=nodata,
                    compress='lzw')

            with rasterio.open(dst_fn, 'w', **profile) as dst:
                dst.write(_data.astype(dtype), 1)

        assert _exists(dst_fn)

        return dst_fn

    def mask_ranches(self, raster_fn, ranches, dst_fn, nodata=-9999):
        """

        :param raster_fn: utm raster
        :param ranches:
        :param dst_fn:
        :param nodata:
        :return:
        """

        assert _exists(_split(dst_fn)[0])
        assert dst_fn.endswith('.tif')

        def is_mappable_of_floats(x):
            try:
                float(x[0])
                return True
            except:
                return False

        def coords_3d_to_2d(coords):
            _coords = []
            for coord in coords:
                if is_mappable_of_floats(coord):
                    _coords.append((coord[0], coord[1]))
                else:
                    _coords.append(coords_3d_to_2d(coord))
            return _coords 

        ds = rasterio.open(raster_fn)
        ds_proj4 = ds.crs.to_proj4()

        loc_path = self.loc_path
        _d = self._d

        sf_fn = _join(loc_path, _d['sf_fn'])
        sf_feature_properties_key = _d['sf_feature_properties_key']
        sf_fn = os.path.abspath(sf_fn)
        sf = fiona.open(sf_fn, 'r')

        reverse_key = self.reverse_key

        features = []
        for feature in sf:
            properties = feature['properties']
            key = properties[sf_feature_properties_key].replace(' ', '_')

            if not reverse_key:
                _pasture, _ranch = key.split(self.key_delimiter)
            else:
                _ranch, _pasture = key.split(self.key_delimiter)

            if any(_ranch.lower() == r.lower() for r in ranches):
                _features = transform_geom(sf.crs_wkt, ds_proj4, feature['geometry'])
                features.append(_features)

        utm_dst_fn = ''
        try:
            features = [
		    {
			"type": g["type"],
			"coordinates": coords_3d_to_2d(g["coordinates"]),
		    }
		    for g in features
		]


            # true where valid
            pasture_mask, _, _ = raster_geometry_mask(ds, features)
            data = np.ma.array(ds.read(1, masked=True), mask=pasture_mask)

            head, tail = _split(dst_fn)
            utm_dst_fn = _join(head, '_utm_' + tail)

            if isinstance(data, np.ma.core.MaskedArray):
                data.fill_value = nodata
                _data = data.filled()
            else:
                _data = data

            with rasterio.Env():
                profile = ds.profile
                dtype = profile.get('dtype')
                profile.update(
                    count=1,
                    nodata=nodata,
                    compress='lzw')

                with rasterio.open(utm_dst_fn, 'w', **profile) as dst:
                    dst.write(_data.astype(dtype), 1)

            assert _exists(utm_dst_fn)
        except:
            if _exists(utm_dst_fn):
                os.remove(utm_dst_fn)
            raise

        dst_vrt_fn = ''
        try:
            dst_vrt_fn = dst_fn.replace('.tif', '.vrt')

            if _exists(dst_vrt_fn):
                os.remove(dst_vrt_fn)

            cmd = ['gdalwarp', '-t_srs', 'EPSG:4326', '-of', 'vrt', utm_dst_fn, dst_vrt_fn]
            p = Popen(cmd)
            p.wait()

            assert _exists(dst_vrt_fn)
        except:
            if _exists(dst_vrt_fn):
                os.remove(dst_vrt_fn)
            raise

        try:
            if _exists(dst_fn):
                os.remove(dst_fn)

            cmd = ['gdal_translate', '-co', 'COMPRESS=LZW', '-of', 'GTiff', dst_vrt_fn, dst_fn]
            p = Popen(cmd)
            p.wait()

            assert _exists(dst_fn)
        except:
            if _exists(dst_fn):
                os.remove(dst_fn)
            raise

        if _exists(utm_dst_fn):
            os.remove(utm_dst_fn)

        if _exists(dst_vrt_fn):
            os.remove(dst_vrt_fn)

    def shape_inspection(self, ranch=None):

        if ranch is not None:
            ranch = ranch.replace(' ', '_')

        loc_path = self.loc_path
        _d = self._d

        sf_fn = _join(loc_path, _d['sf_fn'])
        sf_feature_properties_key = _d['sf_feature_properties_key']
        sf_fn = os.path.abspath(sf_fn)
        sf = fiona.open(sf_fn, 'r')

        area_ha = {}
        bboxs = []
        for feature in sf:
            properties = feature['properties']
            key = properties[sf_feature_properties_key].replace(' ', '_')

            _pasture, _ranch = key.split(self.key_delimiter)
            if self.reverse_key:
                _ranch, _pasture = _pasture, _ranch

            if ranch is None or _ranch.lower() == ranch.lower():
                bboxs.append(fiona.bounds(feature))

                if key not in area_ha:
                    area_ha[key] = properties.get('Hectares', 0.0)

        bboxs = np.array(bboxs)
        e, s, w, n = [np.min(bboxs[:, 0]), np.min(bboxs[:, 1]),
                      np.max(bboxs[:, 2]), np.max(bboxs[:, 3])]

        proj_wkt = open(sf_fn.replace('.shp', '') + '.prj').read()
        sf_proj4 = wkt_2_proj4(proj_wkt)
        sf_proj = pyproj.Proj(sf_proj4)
        wgs_proj = pyproj.Proj(wgs84_proj4)
        _e, _s = pyproj.transform(sf_proj, wgs_proj, e, s)
        _w, _n = pyproj.transform(sf_proj, wgs_proj, w, n)
        bbox = _e, _n, _w, _s

        return float(np.sum(list(area_ha.values()))), bbox

    def serialized_ranch(self, ranch):

        if ranch is not None:
            ranch = ranch.replace(' ', '_')

        if ranch not in self.pastures:
            return None

        area_ha, bbox = self.shape_inspection(ranch)

        return dict(location=self.location, models=self.models, ranch=ranch,
                    pastures=sorted(list(self.pastures[ranch])),
                    area_ha=area_ha, bbox=bbox)

    def pasture_inspection(self, ranch, pasture):

        if ranch is not None:
            ranch = ranch.replace(' ', '_')

        if pasture is not None:
            pasture = pasture.replace(' ', '_')


        loc_path = self.loc_path
        _d = self._d

        sf_fn = _join(loc_path, _d['sf_fn'])
        sf_feature_properties_key = _d['sf_feature_properties_key']
        sf_fn = os.path.abspath(sf_fn)
        sf = fiona.open(sf_fn, 'r')

        coordinates = None
        properties = None
        for feature in sf:
            properties = feature['properties']
            key = properties[sf_feature_properties_key].replace(' ', '_')

            _pasture, _ranch = key.split(self.key_delimiter)
            if self.reverse_key:
                _ranch, _pasture = _pasture, _ranch

            if _pasture.lower() == pasture.lower() and _ranch.lower() == ranch.lower():
                properties = feature['properties']
                coordinates = feature['geometry']['coordinates']
                break

        area_ha = properties.get('Hectares', None)

        _coords = []

        for coords in coordinates:
            for point in coords:
                if isinstance(point, tuple):
                    _coords.append(point)
                else:
                    _coords.extend(point)

        _coords = np.array(_coords)

        _centroid = np.mean(_coords, axis=0)
        e = _centroid[0]
        n = _centroid[1]

        proj_wkt = open(sf_fn.replace('.shp', '') + '.prj').read()
        sf_proj4 = wkt_2_proj4(proj_wkt)
        sf_proj = pyproj.Proj(sf_proj4)
        wgs_proj = pyproj.Proj(wgs84_proj4)
        centroid = pyproj.transform(sf_proj, wgs_proj, e, n)

        return area_ha, centroid

    def serialized_pasture(self, ranch, pasture):
        if ranch is not None:
            ranch = ranch.replace(' ', '_')

        if pasture is not None:
            pasture = pasture.replace(' ', '_')

        if ranch not in self.pastures:
            return None

        if pasture not in self.pastures[ranch]:
            return None

        area_ha, centroid = self.pasture_inspection(ranch, pasture)

        return dict(location=self.location, models=self.models, ranch=ranch,
                    pasture=pasture, area_ha=area_ha, centroid=centroid)

    def geojson_filter(self, ranch=None, pasture=None):
        _d = self._d
        sf_feature_properties_key = _d['sf_feature_properties_key']

        with open(self.geojson) as fp:
            js = json.load(fp)

        if ranch is None and pasture is None:
            return js

        if ranch is not None:
            ranch = ranch.replace(' ', '_')

        if pasture is not None:
            pasture = pasture.replace(' ', '_')

        _features = []
        for f in js['features']:
            key = f['properties'][sf_feature_properties_key].replace(' ', '_')
            _pasture, _ranch = key.split(self.key_delimiter)
            # Don't reverse here, because the shape file should already have pasture+ranch keys
            #
#            if self.reverse_key:
#                _ranch, _pasture = _pasture, _ranch


            if (ranch is None or _ranch.lower() == ranch.lower()) and \
               (pasture is None or _pasture.lower() == pasture.lower()):
                _features.append(f)

        js['features'] = _features

        return js

