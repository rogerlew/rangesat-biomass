
from typing import Tuple, List, Dict, Union
from mgrs import MGRS
import os
from os.path import join as _join
from os.path import split as _split
from os.path import exists as _exists
import datetime
import requests
import htmllistparse
from urllib.request import urlopen
from pyhdf.SD import SD, SDC
from osgeo import gdal, osr

from subprocess import Popen

import numpy as np

import utm

wgs84_proj4 = '+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs'

gdal.UseExceptions()


def isint(x):
    try:
        return float(int(x)) == float(x)
    except:
        return False


class HLSManager(object):
    def __init__(self, datadir='/geodata/hls'):
        self._m = MGRS()
        self.datadir = datadir

    def query(self, mgrs, sat='L', year=None, version='v1.4', startdate=None, enddate=None):
        sat = sat.upper()
        assert sat in 'LS'

        if year is None:
            year = datetime.datetime.now().year

        assert isint(year)
        year = int(year)

        zone = mgrs[:2]
        grid = mgrs[2]
        aa_x, aa_y = tuple(mgrs[3:5])

        url = 'https://hls.gsfc.nasa.gov/data/{version}/{sat}30/{year}/{zone}/{grid}/{aa_x}/{aa_y}/'\
              .format(version=version,
                      sat=sat,
                      year=year,
                      zone=zone,
                      grid=grid,
                      aa_x=aa_x, aa_y=aa_y)

        cwd, listing = htmllistparse.fetch_listing(url)

        listing = [item.name for item in listing if item.name.endswith('hdf')]

        if startdate is not None:
            startdate_m, startdate_d = map(int, startdate.split('-'))
            start_jd = (datetime.date(year, startdate_m, startdate_d) - datetime.date(year, 1, 1)).days + 1
            listing = [name for name in listing if int(name.split('.')[3][4:]) >= start_jd]

        if enddate is not None:
            enddate_m, enddate_d = map(int, enddate.split('-'))
            enddate_jd = (datetime.date(year, enddate_m, enddate_d) - datetime.date(year, 1, 1)).days + 1
            listing = [name for name in listing if int(name.split('.')[3][4:]) <= enddate_jd]

        return listing

    def identify_mgrs_from_point(self, lng=None, lat=None):
        return self._m.toMGRS(latitude=lat, longitude=lng, MGRSPrecision=0)

    def identify_mgrs_from_bbox(self, bbox):
        l, t, r, b = bbox
        assert l < r
        assert b < t

        delta = 0.01

        mgrss = set()
        for lng in np.arange(l, r, delta):
            mgrss.add(self._m.toMGRS(latitude=t, longitude=lng, MGRSPrecision=0))
            mgrss.add(self._m.toMGRS(latitude=b, longitude=lng, MGRSPrecision=0))

        for lat in np.arange(b, t, delta):
            mgrss.add(self._m.toMGRS(latitude=lat, longitude=l, MGRSPrecision=0))
            mgrss.add(self._m.toMGRS(latitude=lat, longitude=r, MGRSPrecision=0))

        return tuple(mgrss)

    def get_identifier_path(self, identifier):
        assert identifier.startswith('HLS')
        assert identifier.endswith('.hdf')

        datadir = self.datadir
        _identifier = identifier[:-4].split('.')
        sat = _identifier[1]
        zone = _identifier[2][1:3]
        grid = _identifier[2][3]
        aa_x, aa_y = _identifier[2][4], _identifier[2][5]
        _date = _identifier[3]
        year = _date[:4]

        return _join(datadir, sat, year, zone, grid, aa_x, aa_y, identifier)

    def is_acquired(self, identifier):

        assert identifier.startswith('HLS')
        assert identifier.endswith('.hdf')

        identifier_path = self. get_identifier_path(identifier)

        return _exists(identifier_path)

    def retrieve(self, identifier, skip_acquired=True):
        datadir = self.datadir

        assert identifier.startswith('HLS')
        assert identifier.endswith('.hdf')

        _identifier = identifier[:-4].split('.')
        sat = _identifier[1]
        zone = _identifier[2][1:3]
        grid = _identifier[2][3]
        aa_x, aa_y = _identifier[2][4], _identifier[2][5]
        _date = _identifier[3]
        year = _date[:4]
        version = '.'.join(_identifier[4:])

        if skip_acquired:
            if self.is_acquired(identifier):
                return

        url = 'https://hls.gsfc.nasa.gov/data/{version}/{sat}/{year}/{zone}/{grid}/{aa_x}/{aa_y}/{identifier}'\
              .format(version=version,
                      sat=sat,
                      year=year,
                      zone=zone,
                      grid=grid,
                      aa_x=aa_x, aa_y=aa_y,
                      identifier=identifier)

        out_dir = _join(datadir, sat, year, zone, grid, aa_x, aa_y)

        if not _exists(out_dir):
            os.makedirs(out_dir)

        identifier_path = _join(out_dir, identifier)

        output = urlopen(url, timeout=60)
        with open(identifier_path, 'wb') as fp:
            fp.write(output.read())

        output = urlopen(url + '.hdr', timeout=60)
        with open(identifier_path + '.hdr', 'wb') as fp:
            fp.write(output.read())

    def get_hls(self, identifier):
        if not self.is_acquired(identifier):
            identifier_path = self.retrieve(identifier)

        identifier_path =self.get_identifier_path(identifier)

        assert _exists(identifier_path), identifier_path

        return HLS(identifier_path)


class HLS(object):
    def __init__(self, identifier):
        _identifier = _split(identifier)
        self.identifier = _identifier[-1]
        path = identifier

        assert _exists(path)
        self.path = path
        self.file = file = SD(path, SDC.READ)
        _variables = {}
        for short_name in file.datasets().keys():
            band = file.select(short_name)
            attrs = band.attributes()
            if 'long_name' in attrs:
                _variables[attrs['long_name']] = short_name
            elif 'QA description' in attrs:
                _variables['QA'] = short_name
            else:
                raise NotImplementedError()

        self._variables = _variables

    @property
    def sat(self):
        return self.identifier.split('.')[1][0]

    @property
    def variables(self):
        return list(self._variables.keys())

    @property
    def acquisition_date(self):
        _date = self.identifier.split('.')[3]
        year = _date[:4]
        jd = _date[4:]
        return datetime.date(year=int(year), month=1, day=1) + datetime.timedelta(days=int(jd) - 1)

    @property
    def ncols(self):
        return int(self.file.attributes()['NCOLS'])

    @property
    def nrows(self):
        return int(self.file.attributes()['NROWS'])

    @property
    def ulx(self):
        return float(self.file.attributes()['ULX'])

    @property
    def uly(self):
        return float(self.file.attributes()['ULY'])

    @property
    def spatial_resolution(self):
        return float(self.file.attributes()['SPATIAL_RESOLUTION'])

    @property
    def transform(self):
        return [self.ulx, self.spatial_resolution, 0.0, self.uly, 0.0, -self.spatial_resolution]

    @property
    def _tileid_key(self):
        key = 'TILE_ID'
        if self.sat.startswith('L'):
            key = 'SENTINEL2_TILEID'
        return key

    @property
    def utm_zone(self):
        return int(self.identifier[9:11])

    @property
    def grid(self):
        return self.identifier[8]

    @property
    def is_north(self):
        grid = self.grid
        assert grid in 'ABCDEFGHJKLMNPQRSTUVWXYZ'
        return grid in 'NPQRSTUVWXYZ'

    @property
    def hdr_fn(self):
        return self.path + '.hdr'

    @property
    def geog_cs(self):
        horizontal_cs_name = self.file.attributes()['HORIZONTAL_CS_NAME']

        if 'WGS84' in horizontal_cs_name:
            return 'WGS84'
        elif 'NAD27' in horizontal_cs_name:
            return 'NAD27'
        else:
            raise NotImplementedError()

    def _get_band_add_offset(self, band):
        if band not in self._variables:
            return 0.0

        _band = self.file.select(self._variables[band])
        _attrs = _band.attributes()
        try:
            return float(_attrs['add_offset'])
        except KeyError:
            return None

    def _get_band_scale_factor(self, band):
        if band not in self._variables:
            return 0.0001

        _band = self.file.select(self._variables[band])
        _attrs = _band.attributes()
        try:
            return float(_attrs['scale_factor'])
        except KeyError:
            return None

    def _get_band_fill_value(self, band):
        if band not in self._variables:
            return -1000

        _band = self.file.select(self._variables[band])
        _attrs = _band.attributes()
        dtype = self._get_band_dtype(band)
        try:
            return dtype(_attrs['_FillValue'])
        except KeyError:
            return None

    def _get_band_dtype(self, band):
        if band not in self._variables:
            return np.int16

        _band = self.file.select(self._variables[band])
        return getattr(np, str(_band.get().dtype))

    def _unpack_band(self, band):
        _band = self.file.select(self._variables[band])
        _attrs = _band.attributes()
        add_offset = self._get_band_add_offset(band)
        scale_factor = self._get_band_scale_factor(band)
        fill_value = self._get_band_fill_value(band)

        _data = _band.get()
        if fill_value is not None:
            _data = np.ma.masked_values(_data, fill_value)

        if add_offset is not None and scale_factor is not None:
            _data = (_data - add_offset) * scale_factor

        return _data

    @property
    def red(self):
        return self._unpack_band('Red')

    @property
    def green(self):
        return self._unpack_band('Green')

    @property
    def blue(self):
        return self._unpack_band('Blue')

    @property
    def nir(self):
        if self.sat.startswith('L'):
            # 845 - 885 nm
            # https://landsat.gsfc.nasa.gov/landsat-8/landsat-8-bands/
            return self._unpack_band('NIR')

        elif self.sat.startswith('S'):
            # https://en.wikipedia.org/wiki/Sentinel-2
            # central wavelength of 864.7 and 21 nm bandwidth
            return self._unpack_band('NIR_Narrow')

    @property
    def swir1(self):
        return self._unpack_band('SWIR1')

    @property
    def swir2(self):
        return self._unpack_band('SWIR2')

    @property
    def tirs1(self):
        return self._unpack_band('TIRS1')

    @property
    def tirs2(self):
        return self._unpack_band('TIRS2')

    @property
    def qa(self):
        return self._unpack_band('QA')

    @property
    def ndvi(self):
        nir = self.nir
        red = self.red

        return (nir - red) / (nir + red)

    @property
    def tasseled_cap_greenness(self):
        return -0.2941 * self.blue + \
               -0.2430 * self.green + \
               -0.5424 * self.red + \
                0.7276 * self.nir + \
                0.0713 * self.swir1 + \
               -0.1608 * self.swir2

    @property
    def tasseled_cap_brightness(self):
        return 0.3029 * self.blue + \
               0.2786 * self.green + \
               0.4733 * self.red + \
               0.5599 * self.nir + \
               0.5080 * self.swir1 + \
               0.1872 * self.swir2


    @property
    def tasseled_cap_wetness(self):
        return +0.1511 * self.blue + \
                0.1973 * self.green + \
                0.3283 * self.red + \
                0.3407 * self.nir + \
               -0.7117 * self.swir1 + \
               -0.4559 * self.swir2

    @property
    def sr(self):
        return self.nir / self.red

    @property
    def rdvi(self):
        nir = self.nir
        red = self.red
        return (nir - red) / np.sqrt(nir + red)

    @property
    def mtvii(self):
        nir = self.nir
        red = self.red
        green = self.green
        return 1.2 * (1.2 * (nir - green) - 2.5 * (red - green))

    @property
    def psri(self):
        return (self.red - self.green) / self.nir

    @property
    def ci(self):
        return self.swir1 - self.green

    @property
    def nci(self):
        swir1 = self.swir1
        green = self.green
        return (swir1 - green) / (swir1 + green)

    @property
    def rci(self):
        return self.swir1 / self.red

    @property
    def ndci(self):
        swir1 = self.swir1
        red = self.red
        return (swir1 - red) / (swir1 + red)

    @property
    def satvi(self):
        swir1 = self.swir1
        swir2 = self.swir2
        red = self.red
        return ((swir1 - red) / ((swir1 + red) + 0.5)) * (1 + 0.5) - (swir2 / 2)

    @property
    def sf(self):
        return self.swir2 / self.nir

    @property
    def ndii7(self):
        swir2 = self.swir2
        nir = self.nir
        return (nir - swir2) / (nir + swir2)

    @property
    def ndwi(self):
        swir1 = self.swir1
        nir = self.nir
        return (nir - swir1) / (nir + swir1)

    @property
    def sti(self):
        return self.swir1 / self.swir2

    @property
    def swir_ratio(self):
        return self.swir2 / self.swir1

    @property
    def rgb(self, red_gamma=1.03, blue_gamma=0.925):
        red = np.array(self.red, dtype=np.float64) / 1.0
        green = np.array(self.green, dtype=np.float64) / 1.0
        blue = np.array(self.blue, dtype=np.float64) / 1.0

        red = np.power(red, 1.0/red_gamma)
        blue = np.power(blue, 1.0/blue_gamma)

        red = np.clip(red, a_min=0.0, a_max=1.0)
        green = np.clip(green, a_min=0.0, a_max=1.0)
        blue = np.clip(blue, a_min=0.0, a_max=1.0)

        return np.stack((red, green, blue), axis=0)

    def proj4(self):
        srs = osr.SpatialReference()
        srs.SetUTM(self.utm_zone, (0, 1)[self.is_north])
        srs.SetWellKnownGeogCS(self.geog_cs)
        return srs.ExportToProj4()

    def export_band(self, band, as_float=True, compress=True, out_dir=None, force_utm_zone=None):

        if out_dir is None:
            out_dir = _split(self.path)[0]

        if as_float:
            _data = getattr(self, band)
            dtype = np.float32
        else:
            add_offset = self._get_band_add_offset(band)
            scale_factor = self._get_band_scale_factor(band)
            dtype = self._get_band_dtype(band)

            _data = getattr(self, band)
            _data = np.ma.array((_data / scale_factor) - add_offset, dtype=dtype)

        fill_value = self._get_band_fill_value(band)

        gdal_type = {np.float32: gdal.GDT_Float32,
                     np.float64: gdal.GDT_Float64,
                     np.int16: gdal.GDT_Int16,
                     np.uint8: gdal.GDT_Byte}[dtype]

        driver = gdal.GetDriverByName('GTiff')
        fname = tmp_fname = '{}-{}.tif'.format(self.identifier[:-4], band)
        fname = _join(out_dir, fname)

        if compress:
            tmp_fname = fname[:-4] + '.tmp.tif'
        tmp_fname = _join(out_dir, tmp_fname)

        if _exists(tmp_fname):
            os.remove(tmp_fname)

        ds = driver.Create(tmp_fname, self.nrows, self.ncols, 1, gdal_type)
        ds.SetGeoTransform(self.transform)
        srs = osr.SpatialReference()
        srs.SetUTM(self.utm_zone, (0, 1)[self.is_north])
        srs.SetWellKnownGeogCS(self.geog_cs)
        ds.SetProjection(srs.ExportToWkt())
        _band = ds.GetRasterBand(1)
        _band.WriteArray(_data)

        if fill_value is not None:
            _band.SetNoDataValue(fill_value)

        ds = None

        if force_utm_zone is not None:
            if int(force_utm_zone) == int(self.utm_zone):
                tmp2_fname = fname[:-4] + '.tmp2.tif'

                utm_proj4 = "+proj=utm +zone={zone} +{hemisphere} +datum=WGS84 +ellps=WGS84" \
                    .format(zone=force_utm_zone, hemisphere=('south', 'north')[self.is_north])

                cmd = ['gdal_translate', '-t_srs', '"{}"'.format(utm_proj4), tmp_fname, tmp2_fname]

                _log = open(tmp2_fname + '.err', 'w')
                p = Popen(cmd, stdout=_log, stderr=_log)
                p.wait()
                _log.close()

                if _exists(tmp2_fname):
                    os.remove(tmp2_fname + '.err')
                    os.remove(tmp_fname)

                tmp_fname = tmp2_fname

        if compress:
            cmd = ['gdal_translate', '-co', 'compress=DEFLATE', '-co', 'zlevel=9', tmp_fname, fname]

            _log = open(fname + '.err', 'w')
            p = Popen(cmd, stdout=_log, stderr=_log)
            p.wait()
            _log.close()

            if _exists(fname):
                os.remove(fname + '.err')
                os.remove(tmp_fname)

        return fname

    def merge_and_crop(self, others, bands, bbox, as_float=False, out_dir=None, verbose=True):

        ul_x, ul_y, lr_x, lr_y = bbox

        assert ul_x < lr_x
        assert ul_y < lr_y

        # determine UTM coordinate system of top left corner
        ul_e, ul_n, utm_number, utm_letter = utm.from_latlon(latitude=ul_y, longitude=ul_x)

        # bottom right
        lr_e, lr_n, _, _ = utm.from_latlon(latitude=ul_y, longitude=ul_x,
                                           force_zone_number=utm_number,
                                           force_zone_letter=utm_letter)

        utm_proj4 = "+proj=utm +zone={zone} +{hemisphere} +datum=WGS84 +ellps=WGS84" \
            .format(zone=utm_number, hemisphere=('south', 'north')[ul_y > 0])

        if out_dir is None:
            out_dir = _split(self.path)[0]

        acquisition_date = self.acquisition_date
        sat = self.sat
        proj4 = self.proj4

        for other in others:
            assert acquisition_date == other.acquisition_date, (acquisition_date, other.acquisition_date)
            assert sat == other.sat, (sat, other.sat)

        for band in bands:
            srcs = []

            srcs.append(self.export_band(band, as_float=as_float, out_dir=out_dir, proj4=proj4))

            for other in others:
                srcs.append(other.export_band(band, as_float=as_float, out_dir=out_dir, proj4=proj4))

            vrt_fn = self.identifier.split('.')
            vrt_fn[2] = 'XXXXXX'
            vrt_fn[-1] = 'vrt'

            vrt_fn.insert(-1, '_{}'.format(band))
            vrt_fn = '.'.join(vrt_fn)
            vrt_fn = _join(out_dir, vrt_fn)
            fname = vrt_fn[:-4] + '.tif'

            cmd = ['gdalbuildvrt', vrt_fn] + srcs

            _log = open(vrt_fn + '.err', 'w')
            p = Popen(cmd, stdout=_log, stderr=_log)
            p.wait()
            _log.close()

            if _exists(vrt_fn):
                os.remove(vrt_fn + '.err')

            cmd = ['gdal_translate', '-co', 'compress=DEFLATE', '-co', 'zlevel=9', vrt_fn, fname]

            _log = open(vrt_fn + '.err', 'w')
            p = Popen(cmd, stdout=_log, stderr=_log)
            p.wait()
            _log.close()

            if _exists(fname):
                os.remove(fname + '.err')
                for src in srcs:
                     os.remove(src)
                os.remove(vrt_fn)


if __name__ == "__main__":
    datadir = '/geodata/hls'

    bbox = [-114.05, 48.05, -113.95, 47.95]
    hls_manager = HLSManager(datadir=datadir)
    _mgrss = hls_manager.identify_mgrs_from_bbox(bbox=bbox)
    #_mgrss = [hls_manager.identify_mgrs_from_point(-114.05, 48.05)]
    print(_mgrss)

    # identifier = 'HLS.S30.T11TQN.2020001.v1.4.hdf'
    # identifier = 'HLS.S30.T11UQP.2020004.v1.4.hdf'
    # identifier = 'HLS.S30.T11UQQ.2020004.v1.4.hdf'
    # identifier = 'HLS.S30.T12TTK.2020001.v1.4.hdf'
    identifier = 'HLS.S30.T11UPP.2020004.v1.4.hdf'
    hls_manager.retrieve(identifier)
    _hls = hls_manager.get_hls(identifier)
    fn = _hls.export_band(band='ndvi', out_dir='/home/roger/zumwalt/hls')
    print(fn)

    import sys
    sys.exit()


    for _mgrs in _mgrss:
        listing = hls_manager.query(mgrs=_mgrs, sat='S', year=2020, startdate='5-1', enddate='7-31')

        for item in listing:
            print(item)
            hls_manager.retrieve(item, datadir='/geodata/hls/')


    hlss = []
    for fn in ['HLS.S30.T11TML.2020030.v1.4.hdf', 'HLS.S30.T11TNL.2020030.v1.4.hdf']:
        fn = _join('/geodata/hls/', fn)
        hlss.append(HLS(fn))

    hlss[0].merge_and_crop(hlss[1:], bands=['red', 'green', 'blue', 'ndvi'],
                           bbox=bbox,
                           out_dir='/home/roger/zumwalt/hls')

    import sys
    sys.exit()

    identifier = 'data/HLS.L30.T11TNN.2020007.v1.4.hdf'

    hls = HLS(identifier)
    print(hls.variables)

    hls2 = HLS('data/HLS.S30.T11TNN.2020280.v1.4.hdf')
    print(hls2.variables)

    swir1 = hls.export_band('ndvi', as_float=False)
    swir1 = hls2.export_band('ndvi', as_float=False)

    import sys
    sys.exit()

    hls_manager = HLSManager()
    _mgrs = hls_manager.identify_mgrs_from_point(lng=-116, lat=47)

    print(_mgrs)

    listing = hls_manager.query(mgrs=_mgrs, sat='S')
    print(listing)

    # hls_manager.retrieve('HLS.L30.T11TNN.2020007.v1.4.hdf')
    hls_manager.retrieve('HLS.S30.T11TNN.2020280.v1.4.hdf')
