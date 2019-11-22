import fiona
import sys
import os
import csv

from os.path import join as _join
from os.path import exists, isdir

from glob import glob

from pprint import pprint

sys.path.insert(0, '/var/www/rangesat-biomass')

from database import Location

from biomass.landsat import LandSatScene
from all_your_base.locationinfo import RasterDatasetInterpolator

from all_your_base import SCRATCH, RANGESAT_DIRS

if __name__ == "__main__":

    #sf_fn = '/Volumes/Space/geodata/rangesat/CIG_MacroplotsSampled_2019/CIG_MacroPlotsSampled_2019.shp'
    sf_fn = '/space/rangesat/CIG_MacroPlotsSampled_2019/CIG_MacroPlotsSampled_2019.shp'
    sf_fn = os.path.abspath(sf_fn)
    sf = fiona.open(sf_fn, 'r')

    out_fn = '/space/rangesat/CIG_MacroPlotsSampled_2019/compiled.csv'
    fp = open(out_fn, 'w')
    csv_wtr = csv.DictWriter(fp, ('location', 'site_id', 'lng', 'lat', 'product_id',
                                  'acquisition_date', 'ndvi', 'nbr', 'nbr2', 'tcg'))
    header_written = False

    sites = {}
    for feature in sf:
        properties = feature['properties']
        site_id = properties['SiteID']
        site = site_id[:4]
        lng = float(properties['Long'])
        lat = float(properties['Lat'])

        if site not in sites:
            sites[site] = []

        sites[site].append(dict(site_id=site_id, lng=lng, lat=lat, landsat=[]))

    for location in sites:
        print(location)

        ls_fns = []
        for rangesat_dir in RANGESAT_DIRS:
            _ls_fns = glob(_join(rangesat_dir, location, 'analyzed_rasters', '*'))
            _ls_fns = [fn for fn in _ls_fns if isdir(fn)]
            _ls_fns = [fn for fn in _ls_fns if len(glob(_join(fn, '*ndvi.tif'))) > 0]
            ls_fns.extend(_ls_fns)

        for fn in ls_fns:
            print(fn)
            rdi = RasterDatasetInterpolator(glob(_join(fn, '*ndvi.tif'))[0])
            ls = LandSatScene(fn)

            for site in sites[location]:
                site_id = site['site_id']
                lng = site['lng']
                lat = site['lat']

                py, px = rdi.get_px_coord_from_geo(lng, lat)

                _ndvi = ls.ndvi[px, py]
                _nbr = ls.nbr[px, py]
                _nbr2 = ls.nbr2[px, py]
                _tcg = ls.tasseled_cap_greenness[px, py]

                d = {'location': location, 'site_id': site_id,
                     'lng': lat, 'lat': lat, 'product_id': ls.product_id,
                     'acquisition_date': ls.acquisition_date,
                     'ndvi': _ndvi,
                     'nbr': _nbr,
                     'nbr2': _nbr2,
                     'tcg': _tcg}

                if not header_written:
                    csv_wtr.writeheader()
                    header_written = True

                csv_wtr.writerow(d)

                pprint(d)

    fp.close()
