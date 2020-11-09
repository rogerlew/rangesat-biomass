import sqlite3
from glob import glob
from datetime import date
import os
import csv
from os.path import join as _join
from os.path import exists
import shutil
import sys

sys.path.insert(0, '/var/www/rangesat-biomass')
from api.app import RANGESAT_DIRS, Location
from all_your_base import isfloat

locations = ['Zumwalt2']
db_fn = '/space/rangesat/db/Zumwalt2/_sqlite3.db'

for location in locations:
    _location = None
    for rangesat_dir in RANGESAT_DIRS:
        loc_path = _join(rangesat_dir, location)
        if exists(loc_path):
            _location = Location(loc_path)
            break

    assert _location is not None

    out_dir = _location.out_dir
    key_delimiter = "+"#_location.

    #db_fn = _join(out_dir, '_sqlite3.db')

    if exists(db_fn):
        os.remove(db_fn)

    conn = sqlite3.connect(db_fn)
    c = conn.cursor()

    # Create table
    c.execute("""CREATE TABLE pasture_stats
                 (product_id TEXT, key TEXT, pasture TEXT, ranch TEXT, total_px INTEGER, snow_px INTEGER, 
                 water_px INTEGER, aerosol_px INTEGER,
                 valid_px INTEGER, coverage REAL, model	TEXT, biomass_mean_gpm REAL, biomass_ci90_gpm REAL,	
                 biomass_10pct_gpm REAL, biomass_75pct_gpm REAL, biomass_90pct_gpm REAL, biomass_total_kg REAL,
                 biomass_sd_gpm REAL, summer_vi_mean_gpm REAL, fall_vi_mean_gpm REAL, fraction_summer REAL, 
                 satellite TEXT, acquisition_date TEXT, wrs TEXT, bounds TEXT, wgs_bounds TEXT, valid_pastures_cnt INTEGER,
                 ndvi_mean REAL, ndvi_sd REAL, ndvi_10pct REAL, ndvi_75pct REAL, ndvi_90pct REAL, ndvi_ci90 REAL,
                 nbr_mean REAL, nbr_sd REAL, nbr_10pct REAL, nbr_75pct REAL, nbr_90pct REAL, nbr_ci90 REAL,
                 nbr2_mean REAL, nbr2_sd REAL, nbr2_10pct REAL, nbr2_75pct REAL, nbr2_90pct REAL, nbr2_ci90 REAL)""")

    fns = glob(_join(out_dir, '*.csv'))

    for fn in fns:
        print(fn)
        with open(fn) as fp:
            reader = csv.reader(fp)

            for i, row in enumerate(reader):
                if i == 0:
                    continue

                product_id, \
                key, \
                total_px, \
                snow_px, \
                water_px, \
                aerosol_px, \
                valid_px, \
                coverage, \
                area_ha, \
                model, \
                biomass_mean_gpm, \
                biomass_ci90_gpm, \
                biomass_10pct_gpm, \
                biomass_75pct_gpm, \
                biomass_90pct_gpm, \
                biomass_total_kg, \
                biomass_sd_gpm, \
                summer_vi_mean_gpm, \
                fall_vi_mean_gpm, \
                fraction_summer, \
                _product_id, \
                satellite, \
                _acquisition_date, \
                wrs, \
                bounds, \
                wgs_bounds, \
                valid_pastures_cnt,\
                ndvi_mean,\
                ndvi_sd,\
                ndvi_10pct,\
                ndvi_75pct,\
                ndvi_90pct,\
                ndvi_ci90,\
                nbr_mean,\
                nbr_sd,\
                nbr_10pct,\
                nbr_75pct,\
                nbr_90pct,\
                nbr_ci90,\
                nbr2_mean,\
                nbr2_sd,\
                nbr2_10pct,\
                nbr2_75pct,\
                nbr2_90pct,\
                nbr2_ci90 = row

                if snow_px.replace('-', '') == '':
                    snow_px = 'null'

                if water_px.replace('-', '') == '':
                    water_px = 'null'

                if aerosol_px.replace('-', '') == '':
                    aerosol_px = 'null'

                if coverage.replace('-', '') == '':
                    coverage = '0'

                if area_ha.replace('-', '') == '':
                    area_ha = 'null'

                if valid_px.replace('-', '') == '':
                    valid_px = '0'

                if biomass_mean_gpm.replace('-', '') == '':
                    biomass_mean_gpm = 'null'

                if biomass_ci90_gpm.replace('-', '') == '':
                    biomass_ci90_gpm = 'null'

                if biomass_10pct_gpm.replace('-', '') == '':
                    biomass_10pct_gpm = 'null'

                if biomass_75pct_gpm.replace('-', '') == '':
                    biomass_75pct_gpm = 'null'

                if biomass_90pct_gpm.replace('-', '') == '':
                    biomass_90pct_gpm = 'null'

                if biomass_total_kg.replace('-', '') == '':
                    biomass_total_kg = 'null'

                if biomass_sd_gpm.replace('-', '') == '':
                    biomass_sd_gpm = 'null'

                if summer_vi_mean_gpm.replace('-', '') == '':
                    summer_vi_mean_gpm = 'null'

                if fall_vi_mean_gpm.replace('-', '') == '':
                    fall_vi_mean_gpm = 'null'

                if fraction_summer.replace('-', '') == '':
                    fraction_summer = 'null'

                if ndvi_mean.replace('-', '') == '':
                    ndvi_mean = 'null'

                if ndvi_sd.replace('-', '') == '':
                    ndvi_sd = 'null'

                if ndvi_10pct.replace('-', '') == '':
                    ndvi_10pct = 'null'

                if ndvi_75pct.replace('-', '') == '':
                    ndvi_75pct = 'null'

                if ndvi_90pct.replace('-', '') == '':
                    ndvi_90pct = 'null'

                if ndvi_ci90.replace('-', '') == '':
                    ndvi_ci90 = 'null'

                if nbr_mean.replace('-', '') == '':
                    nbr_mean = 'null'

                if nbr_sd.replace('-', '') == '':
                    nbr_sd = 'null'

                if nbr_10pct.replace('-', '') == '':
                    nbr_10pct = 'null'

                if nbr_75pct.replace('-', '') == '':
                    nbr_75pct = 'null'

                if nbr_90pct.replace('-', '') == '':
                    nbr_90pct = 'null'

                if nbr_ci90.replace('-', '') == '':
                    nbr_ci90 = 'null'

                if nbr2_mean.replace('-', '') == '':
                    nbr2_mean = 'null'

                if nbr2_sd.replace('-', '') == '':
                    nbr2_sd = 'null'

                if nbr2_10pct.replace('-', '') == '':
                    nbr2_10pct = 'null'

                if nbr2_75pct.replace('-', '') == '':
                    nbr2_75pct = 'null'

                if nbr2_90pct.replace('-', '') == '':
                    nbr2_90pct = 'null'

                if nbr2_ci90.replace('-', '') == '':
                    nbr2_ci90 = 'null'

                if isfloat(nbr_mean):
                    if float(nbr_mean) == -1.0:
                        coverage = '0'
                        valid_px = '0'
                        biomass_mean_gpm = 'null'
                        biomass_ci90_gpm = 'null'
                        biomass_10pct_gpm = 'null'
                        biomass_75pct_gpm = 'null'
                        biomass_90pct_gpm = 'null'
                        biomass_total_kg = 'null'
                        biomass_sd_gpm = 'null'
                        summer_vi_mean_gpm = 'null'
                        fall_vi_mean_gpm = 'null'
                        fraction_summer = 'null'

                pasture, ranch = key.replace('-RCR', '+RCR').replace('Tripple', 'Triple').split(key_delimiter)
                _date = product_id.split('_')[3]
                acquisition_date = date(int(_date[:4]), int(_date[4:6]), int(_date[6:]))

                # Insert a row of data
                query = """INSERT INTO pasture_stats VALUES ("{product_id}","{key}","{pasture}","{ranch}",\
{total_px},{snow_px},{water_px},{aerosol_px},{valid_px},{coverage},"{model}",{biomass_mean_gpm},{biomass_ci90_gpm},\
{biomass_10pct_gpm},{biomass_75pct_gpm},{biomass_90pct_gpm},{biomass_total_kg},{biomass_sd_gpm},{summer_vi_mean_gpm},\
{fall_vi_mean_gpm},{fraction_summer},"{satellite}","{acquisition_date}","{wrs}","{bounds}","{wgs_bounds}",{valid_pastures_cnt},\
{ndvi_mean},{ndvi_sd},{ndvi_10pct},{ndvi_75pct},{ndvi_90pct},{ndvi_ci90},\
{nbr_mean},{nbr_sd},{nbr_10pct},{nbr_75pct},{nbr_90pct},{nbr_ci90},\
{nbr2_mean},{nbr2_sd},{nbr2_10pct},{nbr2_75pct},{nbr2_90pct},{nbr2_ci90})""".\
                          format(product_id=product_id, key=key, pasture=pasture, ranch=ranch, total_px=total_px,
                                 snow_px=snow_px, water_px=water_px, aerosol_px=aerosol_px, valid_px=valid_px,
                                 coverage=coverage, model=model, biomass_mean_gpm=biomass_mean_gpm,
                                 biomass_ci90_gpm=biomass_ci90_gpm, biomass_10pct_gpm=biomass_10pct_gpm,
                                 biomass_75pct_gpm=biomass_75pct_gpm, biomass_90pct_gpm=biomass_90pct_gpm,
                                 biomass_total_kg=biomass_total_kg, biomass_sd_gpm=biomass_sd_gpm,
                                 summer_vi_mean_gpm=summer_vi_mean_gpm, fall_vi_mean_gpm=fall_vi_mean_gpm,
                                 fraction_summer=fraction_summer, satellite=satellite,
                                 acquisition_date=acquisition_date, wrs=wrs, bounds=bounds, wgs_bounds=wgs_bounds,
                                 valid_pastures_cnt=valid_pastures_cnt,
                                 ndvi_mean=ndvi_mean, ndvi_sd=ndvi_sd, ndvi_10pct=ndvi_10pct, ndvi_75pct=ndvi_75pct,
                                 ndvi_90pct=ndvi_90pct, ndvi_ci90=ndvi_ci90,
                                 nbr_mean=nbr_mean, nbr_sd=nbr_sd, nbr_10pct=nbr_10pct, nbr_75pct=nbr_75pct,
                                 nbr_90pct=nbr_90pct, nbr_ci90=nbr_ci90,
                                 nbr2_mean=nbr2_mean, nbr2_sd=nbr2_sd, nbr2_10pct=nbr2_10pct, nbr2_75pct=nbr2_75pct,
                                 nbr2_90pct=nbr2_90pct, nbr2_ci90=nbr2_ci90)

                query = query.replace('inf', 'null')

                try:
                    c.execute(query)
                except:
                    print(query, coverage, area_ha, valid_px)
                    raise

    # Save (commit) the changes
    conn.commit()

#    if location =="Zumwalt":
#        c.execute("""DELETE FROM pasture_stats WHERE pasture == "6 Ranch All";""")
#        c.execute("""UPDATE pasture_stats SET key = "P 3A+Probert", ranch = "Probert" WHERE pasture == "P 3A";""")
#        c.execute("""UPDATE pasture_stats SET key = "P 2+Probert", ranch = "Probert" WHERE pasture == "P 2";""")
#        c.execute("""UPDATE pasture_stats SET key = "P 1+Probert", ranch = "Probert" WHERE pasture == "P 1";""")
#        c.execute("""UPDATE pasture_stats SET key = "P 8+Probert", ranch = "Probert" WHERE pasture == "P 8";""")

    # Save (commit) the changes
    conn.commit()

    # We can also close the connection if we are done with it.
    # Just be sure any changes have been committed or they will be lost.
    conn.close()
