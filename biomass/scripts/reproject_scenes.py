from glob import glob
from subprocess import Popen
import os
from os.path import join as _join
from os.path import exists

if __name__ == "__main__":
    root = '/Users/roger/geodata/rangesat/Zumwalt/analyzed_rasters'
    fns = glob(_join(root, '*/*.tif'))
    fns.extend(glob(_join(root, '*/*/*.tif')))

    fns = [fn for fn in fns if not fn.endswith('.wgs.tif')]# and not exists(fn[:-4] + '.wgs.tif')]

    for i, src in enumerate(fns):

        dst = src[:-4] + '.wgs.vrt'
        dst2 = src[:-4] + '.wgs.tif'

        if exists(dst):
            os.remove(dst)
        if exists(dst2):
            os.remove(dst2)

        cmd = ['gdalwarp', '-t_srs', 'EPSG:4326', '-of', 'vrt', src, dst]

        p = Popen(cmd)
        print('%i of %i...' % (i, len(fns)), end='')
        p.wait()

        cmd = ['gdal_translate', '-co', 'COMPRESS=LZW', '-of', 'GTiff', dst, dst2]

        p = Popen(cmd)
        print('%i of %i...' % (i, len(fns)), end='')
        p.wait()
        print('done.')

        assert exists(dst)
