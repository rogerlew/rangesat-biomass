# Copyright: University of Idaho (2019)
# Author:    Roger Lew (rogerlew@uidaho.edu)
# Date:      4/20/2019
# License:   BSD-3 Clause

import os
import io
import tarfile
import shutil
from datetime import date

from glob import glob
from os.path import join as _join
from os.path import exists as _exists
from os.path import split as _split
import numpy as np

import rasterio
from rasterio.io import MemoryFile
from rasterio.warp import transform_bounds

# Landsat 8 Tasseled Cap Coefficients
# https://community.hexagongeospatial.com/t5/Spatial-Modeler-Tutorials/Tasseled-Cap-Transformation-for-Landsat-8/ta-p/1609
#
# Landsat 7
# https://gis.stackexchange.com/a/156255


def get_gz_scene_bounds(fn):
    assert _exists(fn), fn
    assert fn.endswith('.tar.gz')

    tf = tarfile.open(fn, 'r|gz')

    member = tf.next()

    while not member.name.lower().endswith('.tif'):
        member = tf.next()

    data = io.BytesIO(tf.extractfile(member).read())
    ds = MemoryFile(data).open()
    return transform_bounds(ds.crs, 'EPSG:4326', *ds.bounds)


class LandSatScene(object):
    """
    Band designations are documented here
    https://landsat.usgs.gov/what-are-band-designations-landsat-satellites

    The qa_pixel band and vegatiation documented here
    https://pubs.usgs.gov/fs/2015/3034/pdf/fs2015-3034.pdf
    """
    def __init__(self, fn):
        if not _exists(fn):
            raise OSError

        self.tar = None

        if os.path.isdir(fn):
            self.__open_dir(fn)
            self.isdir = True
            self.basedir = fn
            self.tar_fn = None
        else:
            self.__open_targz(fn)
            self.isdir = False
            self.basedir = None
            self.tar_fn = fn

        self._build_bqa()

    def __open_dir(self, fn):
        fns = glob(_join(fn, '*.xml'))

        assert len(fns) == 1, fns
        product_id = _split(fns[0])[-1][:-4].replace('_MTL', '')

        if product_id.endswith('_'):
            product_id = product_id[:-1]

        d = {}
        for fn in glob(_join(fn, '*')):
            key = _split(fn)[-1].replace('%s_' % product_id, '') \
                                .lower() \
                                .replace('.tif', '') \
                                .replace('qa_pixel', 'pixel_qa') \
                                .replace('sr_b1', 'sr_band1') \
                                .replace('sr_b2', 'sr_band2') \
                                .replace('sr_b3', 'sr_band3') \
                                .replace('sr_b4', 'sr_band4') \
                                .replace('sr_b5', 'sr_band5') \
                                .replace('sr_b6', 'sr_band6') \
                                .replace('sr_b7', 'sr_band7') \
                               
            if fn.lower().endswith('.xml'):
                with open(fn, 'r') as fp:
                    d['.xml'] = fp.read()

            elif fn.lower().endswith('.tif'):
                d[key] = rasterio.open(fn)

        self.product_id = product_id
        self.fn = fn
        self._d = d

    @property
    def bands(self):
        return [k for k in self._d.keys() if k != '.xml']

    def __del__(self):
        for key, ds in self._d.items():
            try:
                ds.close()
            except:
                pass

    def __open_targz(self, fn):
        assert fn.endswith('.tar.gz')

        # Open tarfile
        tar = tarfile.open(fn)

        product_id = None
        for member in tar.getnames():
            if member.lower().endswith('.tif'):
                product_id = '_'.join(_split(member)[-1].split('_')[:7])
                break

        d = {}
        for member in tar.getnames():
            key = _split(member)[-1].replace('%s_' % product_id, '')\
                                    .replace('.tif', '').replace('.TIF', '')

            if member.lower().endswith('.xml'):
                d['.xml'] = tar.extractfile(member).read()
            if member.lower().endswith('.txt') and 'MTL' in member:
                d['.mtl'] = tar.extractfile(member).read()
            elif member.lower().endswith('.tif'):
                d[key] = member

        self.tar = tar
        self.product_id = product_id
        self.fn = fn
        self._d = d

#    def __del__(self):
#        if self.tar is not None:
#            self.tar.close()

    def get_dataset(self, name):
        if name not in self._d:
            raise KeyError

        if self.tar is not None:
            fn = '{}_{}.tif'.format(self.product_id, name)
            data = io.BytesIO(self.tar.extractfile(fn).read())
            return MemoryFile(data).open()

        return self._d[name]

    def _build_bqa(self):
        # the np.unpackbits only works with uint8 data types. The pixel_qa band is uint16
        # the 9th and 10th bits are used to encode the cirrus confidence. This ends up
        # dropping those bits for the sake of performance

        # ds = self.get_dataset('pixel_qa')
        pixel_qa = np.array(self._d['pixel_qa'].read(1), dtype=np.uint16)
        m, n = pixel_qa.shape
        bqa = np.unpackbits(pixel_qa.view(np.uint8)).reshape((m, n, 16))
        self.bqa = np.concatenate((bqa[:, :, 8:], bqa[:, :, :8]), axis=2)

    @property
    def cellsize(self):
        gt = self._d['pixel_qa'].transform
        px_x = gt[0]
        px_y = -gt[4]

        assert px_x == px_y
        return px_x

    def summary_dict(self):
        return dict(product_id=self.product_id, satellite=self.satellite,
                    acquisition_date=self.acquisition_date, wrs=self.wrs,
                    bounds=self.bounds, wgs_bounds=self.wgs_bounds)

    @property
    def landsat(self):
        return self.product_id.split('_')[0][:2]

    @property
    def satellite(self) -> int:
        return int(self.product_id.split('_')[0][2:])

    @property
    def processing_correction_level(self):
        return self.product_id.split('_')[1]

    @property
    def wrs(self):
        wrs = self.product_id.split('_')[2]
        return int(wrs[:3]), int(wrs[3:])

    @property
    def bounds(self):
        src = self._d['pixel_qa']
        bounds = src.bounds
        return [bounds.left, bounds.bottom, bounds.right, bounds.top]

    @property
    def proj4(self):
        src = self._d['pixel_qa']
        return src.crs.to_proj4()

    @property
    def wgs_bounds(self):
        src = self._d['pixel_qa']
        return transform_bounds(src.crs, 'EPSG:4326', *src.bounds)

    @property
    def acquisition_date(self):
        _date = self.product_id.split('_')[3]
        return date(int(_date[:4]), int(_date[4:6]), int(_date[6:]))

    @property
    def processing_date(self):
        _date = self.product_id.split('_')[4]
        return date(int(_date[:4]), int(_date[4:6]), int(_date[6:]))

    @property
    def collection_number(self):
        return int(self.product_id.split('_')[5])

    @property
    def collection_category(self):
        return self.product_id.split('_')[6]

    def __getitem__(self, key):
        return self._d[key]

    def get_index(self, indexname):
        if indexname.lower() == 'ndvi':
            return self.ndvi
        elif indexname.lower() == 'nbr':
            return self.nbr
        elif indexname.lower() in ['nbr2', 'ndti']:
            return self.nbr2
        elif indexname.lower() == 'evi':
            return self.evi
        elif indexname.lower() == 'tcg':
            return self.tasseled_cap_greenness
        elif indexname.lower() == 'tcb':
            return self.tasseled_cap_brightness
        elif indexname.lower() == 'tcw':
            return self.tasseled_cap_wetness
        elif indexname.lower() == 'savi':
            return self.savi
        elif indexname.lower() == 'msavi':
            return self.msavi
        elif indexname.lower() == 'ndmi':
            return self.ndmi
        elif indexname.lower() == 'sr':
            return self.sr
        elif indexname.lower() == 'rdvi':
            return self.rdvi
        elif indexname.lower() == 'mtvii':
            return self.mtvii
        elif indexname.lower() == 'psri':
            return self.psri
        elif indexname.lower() == 'ci':
            return self.ci
        elif indexname.lower() == 'nci':
            return self.nci
        elif indexname.lower() == 'ndci':
            return self.ndci
        elif indexname.lower() == 'satvi':
            return self.satvi
        elif indexname.lower() == 'sf':
            return self.sf
        elif indexname.lower() == 'ndii7':
            return self.ndii7
        elif indexname.lower() == 'ndwi':
            return self.ndwi
        elif indexname.lower() == 'sti':
            return self.sti
        elif indexname.lower() == 'swir1':
            return self.swir1
        elif indexname.lower() == 'swir2':
            return self.swir2
        elif indexname.lower() == 'swir_ratio':
            return self.swir_ratio
        elif indexname.lower() == 'aerosol':
            return self.aerosol

        raise KeyError(indexname)

    def extract(self, dst):
        tar = tarfile.open(self.tar_fn)
        tar.extractall(path=dst)
        tar.close()

    @property
    def qa_fill(self):
        return self.bqa[:, :, 15-0]

    @property
    def qa_notclear(self):
        clear = np.array(self.qa_clear, np.bool)
        return np.array(np.logical_not(clear), dtype=np.uint8)

    @property
    def qa_clear(self):
        return self.bqa[:, :, 15-1]

    @property
    def qa_water(self):
        return self.bqa[:, :, 15-2]

    @property
    def qa_cloud_shadow(self):
        return self.bqa[:, :, 15-3]

    @property
    def qa_snow(self):
        return self.bqa[:, :, 15-4]

    @property
    def qa_cloud(self):
        return self.bqa[:, :, 15-5]

    @property
    def qa_cloud_confidence(self):
        return self.bqa[:, :, 15-6] + 2.0 * self.bqa[:, :, 15-7]

    @property
    def qa_cirrus_confidence(self):
        return self.bqa[:, :, 15-8] + 2.0 * self.bqa[:, :, 15-9]

    @property
    def aerosol(self):
        if self.satellite == 8:
            return self._d['sr_aerosol'].read(1, masked=True)
        else:
            return self._d['sr_atmos_opacity'].read(1, masked=True)

    def threshold_aerosol(self, threshold=101, mask=None):
        aero = self.aerosol

        if mask is not None:
            aero = np.ma.array(aero, mask=mask)

        mask = np.zeros(aero.shape, dtype=np.uint8)
        mask[np.where(aero > threshold)] = 1
        return mask

    def _band_proc(self, measure):
        return np.abs(self._d[measure].read(1, masked=True))

    def _tasseled_cap_greenness__5(self):
        return -0.1603 * self._band_proc('sr_band1') + \
               -0.2819 * self._band_proc('sr_band2') + \
               -0.4934 * self._band_proc('sr_band3') + \
                0.7940 * self._band_proc('sr_band4') + \
               -0.0002 * self._band_proc('sr_band5') + \
               -0.1446 * self._band_proc('sr_band7')

    def _tasseled_cap_greenness__7(self):
        return -0.3344 * self._band_proc('sr_band1') + \
               -0.3544 * self._band_proc('sr_band2') + \
               -0.4556 * self._band_proc('sr_band3') + \
                0.6966 * self._band_proc('sr_band4') + \
               -0.0242 * self._band_proc('sr_band5') + \
               -0.2630 * self._band_proc('sr_band7')

    def _tasseled_cap_greenness__8(self):
        return -0.2941 * self._band_proc('sr_band2') + \
               -0.2430 * self._band_proc('sr_band3') + \
               -0.5424 * self._band_proc('sr_band4') + \
                0.7276 * self._band_proc('sr_band5') + \
                0.0713 * self._band_proc('sr_band6') + \
               -0.1608 * self._band_proc('sr_band7')

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
    def ultra_blue(self):
        if self.satellite == 8:
            return self._band_proc('sr_band1')

    @property
    def blue(self):
        if self.satellite == 8:
            return self._band_proc('sr_band2')
        elif self.satellite == 7:
            return 0.0003 + 0.8474 * self._band_proc('sr_band1')
        else:
            return -0.0095 + 0.9785 * self._band_proc('sr_band1')

    @property
    def green(self):
        if self.satellite == 8:
            return self._band_proc('sr_band3')
        elif self.satellite == 7:
            return 0.0088 + 0.8483 * self._band_proc('sr_band2')
        else:
            return -0.0016 + 0.9542 * self._band_proc('sr_band2')

    @property
    def red(self):
        if self.satellite == 8:
            return self._band_proc('sr_band4')
        elif self.satellite == 7:
            return 0.0061 + 0.9047 * self._band_proc('sr_band3')
        else:
            return -0.0022 + 0.9825 * self._band_proc('sr_band3')


    @property
    def nir(self):
        if self.satellite == 8:
            return self._band_proc('sr_band5')
        elif self.satellite == 7:
            return 0.0412 + 0.8462 * self._band_proc('sr_band4')
        else:
            return -0.0021 + 1.0073 * self._band_proc('sr_band4')

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
    def swir1(self):
        if self.satellite == 8:
            return self._band_proc('sr_band6')
        elif self.satellite == 7:
            return 0.0254 + 0.8937 * self._band_proc('sr_band5')
        else:
            return -0.0030 + 1.0171 * self._band_proc('sr_band5')

    @property
    def swir2(self):
        if self.satellite == 8:
            return self._band_proc('sr_band7')
        elif self.satellite == 7:
            return 0.0172 + 0.9071 * self._band_proc('sr_band7')
        else:
            return 0.0029 + 0.9949 * self._band_proc('sr_band7')

    @property
    def sti(self):
        return self.swir1 / self.swir2

    @property
    def swir_ratio(self):
        return self.swir2 / self.swir1

    @property
    def rgb(self, red_gamma=1.03, blue_gamma=0.925):
        red = np.array(self.red, dtype=np.float64) / 10000.0
        green = np.array(self.green, dtype=np.float64) / 10000.0
        blue = np.array(self.blue, dtype=np.float64) / 10000.0

        red = np.power(red, 1.0/red_gamma)
        blue = np.power(blue, 1.0/blue_gamma)

        red = np.clip(red, a_min=0.0, a_max=1.0)
        green = np.clip(green, a_min=0.0, a_max=1.0)
        blue = np.clip(blue, a_min=0.0, a_max=1.0)

        return np.stack((red, green, blue), axis=0)

    @property
    def rgba(self, red_gamma=1.03, blue_gamma=0.925):

        red = np.array(self.red, dtype=np.float64) / 10000.0
        green = np.array(self.green, dtype=np.float64) / 10000.0
        blue = np.array(self.blue, dtype=np.float64) / 10000.0
        alpha = self.qa_clear

        red = np.power(red, 1.0/red_gamma)
        blue = np.power(blue, 1.0/blue_gamma)

        red = np.clip(red, a_min=0.0, a_max=1.0)
        green = np.clip(green, a_min=0.0, a_max=1.0)
        blue = np.clip(blue, a_min=0.0, a_max=1.0)

        return np.stack((red, green, blue, alpha), axis=0)

    def dump_rgb(self, dst_fn, gamma=None):
        rgb = self.rgb
        if gamma is not None:
            rgb = np.power(rgb, 1.0/gamma)

        rgb = np.array(rgb * 255, dtype=np.uint8)

        with rasterio.Env():
            profile = self._d['pixel_qa'].profile
            profile.update(
                dtype=rasterio.ubyte,
                count=3,
                compress='lzw')

            with rasterio.open(dst_fn, 'w', **profile) as dst:
                dst.write(rgb.astype(rasterio.ubyte))

    def _veg_proc(self, measure):
        res = self._d[measure].read(1, masked=True)
        res = np.ma.masked_values(res, -9999.0)
        res = np.ma.array(res, dtype=np.float64)
        res *= 0.0001
        res = np.clip(res, -1.0, 1.0)
        return res

    @property
    def ndvi(self):
        """https://www.usgs.gov/land-resources/nli/landsat/landsat-normalized-difference-vegetation-index?qt-science_support_page_related_con=0#qt-science_support_page_related_con"""
        nir = self.nir
        red = self.red

        return (nir - red) / (nir + red)


    def threshold(self, indexname, threshold=0.38, mask=None):
        """
        returns mask where 1 if index is greater than threshold
        and 0 if less or equal to threshold
        """
        if indexname is  None:
            _mask = np.zeros(self.aerosol.shape, dtype=np.uint8)
            _mask[mask == 0] = 1
            return _mask

        assert threshold >= -1.0
        assert threshold <= 1.0

        data = self.get_index(indexname)

        if mask is not None:
            data = np.ma.array(data, mask=mask)

        _mask = np.zeros(data.shape, dtype=np.uint8)
        _mask[np.where(data > threshold)] = 1
        return _mask

    @property
    def evi(self):
        """
        https://www.usgs.gov/land-resources/nli/landsat/landsat-enhanced-vegetation-index?qt-science_support_page_related_con=0#qt-science_support_page_related_con
        https://en.wikipedia.org/wiki/Enhanced_vegetation_index
        """

        nir = self.nir
        red = self.red
        blue = self.blue
        g = 2.5
        c1 = 6.0
        c2 = 7.5
        L = 1

        return g * ((nir - red) / (nir + c1 * red - c2 * blue + L))

    #    return self._veg_proc('sr_evi')

    @property
    def savi(self):
        nir = self.nir
        red = self.red
        L = 0.5

        return ((nir - red) / (nir + red + L)) * (1 + L)

    #    return self._veg_proc('sr_savi')

    @property
    def msavi(self):
        nir = self.nir
        red = self.red

        return (2.0 * nir + 1.0 - np.sqrt( (2.0 * nir + 1.0)**2.0 - 8.0 * (nir - red))) / 2.0

    #    return self._veg_proc('sr_msavi')

    @property
    def ndmi(self):
        """https://www.usgs.gov/land-resources/nli/landsat/normalized-difference-moisture-index"""
        nir = self.nir
        swir = self.swir1

        return (nir - swir) / (nir + swir)

    #    return self._veg_proc('sr_ndmi')

    @property
    def nbr(self):
        """https://www.usgs.gov/land-resources/nli/landsat/landsat-normalized-burn-ratio"""
        nir = self.nir
        swir = self.swir2

        return (nir - swir) / (nir + swir)

    #    return self._veg_proc('sr_nbr')

    @property
    def nbr2(self):
        """https://www.usgs.gov/land-resources/nli/landsat/landsat-normalized-burn-ratio-2"""
        swir1 = self.swir1
        swir2 = self.swir2

        return (swir1 - swir2) / (swir1 + swir2)

    #    return self._veg_proc('sr_nbr2')

    @property
    def template_ds(self):
        """
        template rasterio.Dataset
        """
        return self._d['pixel_qa']

    def dump(self, data, dst_fn, nodata=-9999, dtype=rasterio.float32):
        """
        utility method to export arrays
        with the same projection and size as the
        landsat scenes as geotiffs
        """
        assert _exists(_split(dst_fn)[0])

        if isinstance(data, np.ma.core.MaskedArray):
            data.fill_value = nodata
            _data = data.filled()
        else:
            _data = data

        with rasterio.Env():
            profile = self._d['pixel_qa'].profile
            profile.update(
                dtype=rasterio.float32,
                count=1,
                nodata=nodata,
                compress='lzw')

            with rasterio.open(dst_fn, 'w', **profile) as dst:
                dst.write(_data.astype(rasterio.float32), 1)

    def clip(self, bounds, outdir, bands=None):
        """
        crops the scene
        """
        assert outdir is not None
        assert self.product_id is not None
        outdir = _join(outdir, self.product_id)

        if bands is None:
            bands = self.bands
            bands = [k for k in bands if 'toa' not in k]
            bands = [k for k in bands if 'sensor' not in k]
            bands = [k for k in bands if 'solar' not in k]
            bands = [k for k in bands if 'b1' not in k]
            bands = [k for k in bands if 'b2' not in k]
            bands = [k for k in bands if 'b3' not in k]
            bands = [k for k in bands if 'b4' not in k]
            bands = [k for k in bands if 'b5' not in k]
            bands = [k for k in bands if 'b6' not in k]
            bands = [k for k in bands if 'b61' not in k]
            bands = [k for k in bands if 'b62' not in k]
            bands = [k for k in bands if 'b7' not in k]
            bands = [k for k in bands if 'b8' not in k]
            bands = [k for k in bands if 'bt_band6' not in k]

        from rasterio.windows import Window

        if _exists(outdir):
            shutil.rmtree(outdir)
        os.makedirs(outdir)

        for measure in self._d:
            try:
                src = self.get_dataset(measure)
            except KeyError:
                continue

            if '.xml' in measure:
                dst_fn = _join(outdir, '%s.xml' % self.product_id)
                with open(dst_fn, 'w') as fp:
                    fp.write(src)
                continue

            if measure is not None and measure not in bands:
                continue

            _bounds = transform_bounds('epsg:4326', src.crs, *bounds)
            bounds_window = src.window(*_bounds)
            bounds_window = bounds_window.intersection(
                Window(0, 0, src.width, src.height))

            # Get the window with integer height
            # and width that contains the bounds window.
            out_window = bounds_window.round_lengths(op='ceil')

            height = int(out_window.height)
            width = int(out_window.width)

            profile = src.profile
            profile.update(
                driver='GTiff',
                height=height,
                width=width,
                transform=src.window_transform(out_window),
                compress='lzw')

            dst_fn = _join(outdir, '%s_%s.tif' % (self.product_id, measure))
            with rasterio.open(dst_fn, 'w', **profile) as out:
                out.write(src.read(window=out_window,
                                   out_shape=(src.count, height, width)))

        return LandSatScene(outdir)
