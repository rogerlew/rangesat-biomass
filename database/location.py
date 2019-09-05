from glob import glob
import os
from os.path import split as _split
from os.path import join as _join
from pprint import pprint
import fiona
import yaml
import pyproj
import numpy as np
import json

from osgeo import osr


def wkt_2_proj4(wkt):
    srs = osr.SpatialReference()
    srs.ImportFromWkt(wkt)
    return srs.ExportToProj4().strip()


wgs84_proj4 = '+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs'


class Location(object):
    def __init__(self, loc_path):
        print('loc_path', loc_path)
        print(_join(loc_path, '*.yaml'))
        cfg_fn = glob(_join(loc_path, '*.yaml'))
        assert len(cfg_fn) == 1, cfg_fn
        cfg_fn = cfg_fn[0]
        with open(cfg_fn) as fp:
            _d = yaml.safe_load(fp)

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

        pastures = {}
        for feature in sf:
            properties = feature['properties']
            key = properties[sf_feature_properties_key].replace(' ', '_')

            pasture, ranch = key.split('+')

            if ranch.lower() not in [p.lower() for p in pastures]:
                pastures[ranch] = set()

            pastures[ranch].add(pasture)

        self.pastures = pastures

    @property
    def models(self):
        return self._d['models']

    @property
    def out_dir(self):
        return _join(self.loc_path, self._d['out_dir'])

    @property
    def db_fn(self):
        return _join(self.loc_path, self._d['out_dir'], 'sqlite3.db')

    @property
    def ranches(self):
        return sorted(list(self.pastures.keys()))

    def serialized(self):
        area_ha, bbox = self.shape_inspection()

        d = dict(location=self.location, models=self.models, ranches=self.ranches,
                 area_ha=area_ha, bbox=bbox)
        return d

    def shape_inspection(self, ranch=None):
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

            _pasture, _ranch = key.split('+')

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
        if ranch not in self.pastures:
            return None

        area_ha, bbox = self.shape_inspection(ranch)

        return dict(location=self.location, models=self.models, ranch=ranch,
                    pastures=sorted(list(self.pastures[ranch])),
                    area_ha=area_ha, bbox=bbox)

    def pasture_inspection(self, ranch, pasture):
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

            _pasture, _ranch = key.split('+')

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
        if ranch not in self.pastures:
            return None

        if pasture not in self.pastures[ranch]:
            return None

        area_ha, centroid = self.pasture_inspection(ranch, pasture)

        return dict(location=self.location, models=self.models, ranch=ranch,
                    pasture=pasture, area_ha=area_ha, centroid=centroid)

    def geojson_filter(self, ranch=None, pasture=None):
        print(ranch, pasture)
        sf_feature_properties_key = self._d['sf_feature_properties_key']

        with open(self.geojson) as fp:
            if ranch is None and pasture is None:
                return fp.read()
            js = json.load(fp)

        _features = []
        for f in js['features']:
            key = f['properties'][sf_feature_properties_key].replace(' ', '_')
            _pasture, _ranch = key.split('+')

            if (ranch is None or _ranch.lower() == ranch.lower()) and \
               (pasture is None or _pasture.lower() == pasture.lower()):
                _features.append(f)

        js['features'] = _features

        return json.dumps(js)

