import os
from os.path import exists as _exists
from os.path import join as _join
from os.path import split as _split

import requests
from urllib.request import urlopen

if __name__ == '__main__':
    raster_save_dir = 'biomass_rasters'
    start_year = 2015
    end_year = 2017

    start_month = 5
    end_month = 7

    if not _exists(raster_save_dir):
        os.mkdir(raster_save_dir)

    r = requests.get('https://rangesat.nkn.uidaho.edu/api/scenemeta/Zumwalt/')
    scenes = r.json()

    for year in range(start_year, end_year+1):
        year = str(year)
        print(year)

        if not _exists(_join(raster_save_dir, year)):
            os.mkdir(_join(raster_save_dir, year))

        for scn in (scn for scn in scenes):
            acquisition_date = scn.split('_')[3]

            month = int(acquisition_date[4:6])
            if not (acquisition_date.startswith(year) and start_month <= month <= end_month):
                continue

            url = 'https://rangesat.nkn.uidaho.edu/api/raster/Zumwalt/{scn}/biomass/?utm=True'\
                  .format(scn=scn)
            fname = _join(raster_save_dir, year, scn + '.tif')

            print('acquiring {scn}...'.format(scn=scn), end='')
            output = urlopen(url)
            with open(fname, 'wb') as fp:
                fp.write(output.read())

            print('done')
