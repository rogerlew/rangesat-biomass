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


def get_gz_scene_bounds(fn):
    assert _exists(fn)
    assert fn.endswith('.tar.gz')

    tf = tarfile.open(fn, 'r|gz')

    member = tf.next()

    while not member.name.endswith('.tif'):
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
        fns = glob(_join(fn, '*'))

        target_fn = [fn for fn in fns if fn.endswith('.tif')][0]
        product_id = '_'.join(_split(target_fn)[-1].split('_')[:7])

        d = {}
        for fn in fns:
            key = _split(fn)[-1].replace('%s_' % product_id, '')\
                                .replace('.tif', '')

            if fn.endswith('.xml'):
                with open(fn, 'r') as fp:
                    d['.xml'] = fp.read()

            elif fn.endswith('.tif'):
                d[key] = rasterio.open(fn)

        self.product_id = product_id
        self.fn = fn
        self._d = d

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
            if member.endswith('.tif'):
                product_id = '_'.join(_split(member)[-1].split('_')[:7])
                break

        d = {}
        for member in tar.getnames():
            key = _split(member)[-1].replace('%s_' % product_id, '')\
                                    .replace('.tif', '')

            if member.endswith('.xml'):
                d['.xml'] = tar.extractfile(member).read()
            elif member.endswith('.tif'):
                d[key] = member

        self.tar = tar
        self.product_id = product_id
        self.fn = fn
        self._d = d

    def __del__(self):
        if self.tar is not None:
            self.tar.close()

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

    @property
    def ultra_blue(self):
        if self.satellite == 8:
            return self._band_proc('sr_band1')

    @property
    def blue(self):
        if self.satellite == 8:
            return self._band_proc('sr_band2')
        else:
            return self._band_proc('sr_band1')

    @property
    def green(self):
        if self.satellite == 8:
            return self._band_proc('sr_band3')
        else:
            return self._band_proc('sr_band2')

    @property
    def red(self):
        if self.satellite == 8:
            return self._band_proc('sr_band4')
        else:
            return self._band_proc('sr_band3')

    @property
    def nir(self):
        if self.satellite == 8:
            return self._band_proc('sr_band5')
        elif self.satellite == 7:
            return self._band_proc('sr_band4')

    @property
    def swir1(self):
        if self.satellite == 8:
            return self._band_proc('sr_band6')
        elif self.satellite == 7:
            return self._band_proc('sr_band5')

    @property
    def swir2(self):
        if self.satellite == 8:
            return self._band_proc('sr_band7')
        elif self.satellite == 7:
            return self._band_proc('sr_band7')

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
        res = np.array(res, dtype=np.float64)
        res *= 0.0001
        return res

    @property
    def ndvi(self):
        return self._veg_proc('sr_ndvi')

    def threshold_ndvi(self, threshold=0.38, mask=None):
        """
        returns mask where 1 if ndvi is greater than threshold
        and 0 if less or equal to threshold
        """
        assert threshold >= -1.0
        assert threshold <= 1.0

        ndvi = self.ndvi

        if mask is not None:
            ndvi = np.ma.array(ndvi, mask=mask)

        mask = np.zeros(ndvi.shape, dtype=np.uint8)
        mask[np.where(ndvi > threshold)] = 1
        return mask

    @property
    def evi(self):
        return self._veg_proc('sr_evi')

    @property
    def savi(self):
        return self._veg_proc('sr_savi')

    @property
    def msavi(self):
        return self._veg_proc('sr_msavi')

    @property
    def ndmi(self):
        return self._veg_proc('sr_ndmi')

    @property
    def nbr(self):
        return self._veg_proc('sr_nbr')

    @property
    def nbr2(self):
        return self._veg_proc('sr_nbr2')

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
            bands = ['pixel_qa', 'sr_ndvi', 'sr_nbr', 'sr_nbr2',
                     'sr_aerosol', 'sr_nbr', 'sr_atmos_opacity']

        from rasterio.windows import Window

        if _exists(outdir):
            shutil.rmtree(outdir)
        os.makedirs(outdir)

        for measure in self._d:
            src = self.get_dataset(measure)
            if '.xml' in measure:
                dst_fn = _join(outdir, '%s_%s' % (self.product_id, measure))
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
