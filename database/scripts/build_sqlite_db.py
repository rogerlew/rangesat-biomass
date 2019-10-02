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
from api.app import RANGESAT_DIR, Location

locations = ['Zumwalt', 'RCR']

for location in locations:
    loc_path = _join(RANGESAT_DIR, location)
    if exists(loc_path):
        _location = Location(loc_path)

    out_dir = _location.out_dir
    key_delimiter = "+"#_location.

    db_fn = _join(out_dir, 'sqlite3.db')

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
                 satellite TEXT, acquisition_date TEXT, wrs TEXT, bounds TEXT, wgs_bounds TEXT, valid_pastures_cnt INTEGER )""")

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
                valid_pastures_cnt = row

                if biomass_mean_gpm == '':
                    biomass_mean_gpm = 'null'

                if biomass_ci90_gpm == '':
                    biomass_ci90_gpm = 'null'

                if biomass_10pct_gpm == '':
                    biomass_10pct_gpm = 'null'

                if biomass_75pct_gpm == '':
                    biomass_75pct_gpm = 'null'

                if biomass_90pct_gpm == '':
                    biomass_90pct_gpm = 'null'

                if biomass_total_kg == '':
                    biomass_total_kg = 'null'

                if biomass_sd_gpm == '':
                    biomass_sd_gpm = 'null'

                if summer_vi_mean_gpm == '':
                    summer_vi_mean_gpm = 'null'

                if fall_vi_mean_gpm == '':
                    fall_vi_mean_gpm = 'null'

                if fraction_summer == '':
                    fraction_summer = 'null'

                pasture, ranch = key.split(key_delimiter)
                _date = product_id.split('_')[3]
                acquisition_date = date(int(_date[:4]), int(_date[4:6]), int(_date[6:]))

                # Insert a row of data
                query = """INSERT INTO pasture_stats VALUES ("{product_id}","{key}","{pasture}","{ranch}",\
{total_px},{snow_px},{water_px},{aerosol_px},{valid_px},{coverage},"{model}",{biomass_mean_gpm},{biomass_ci90_gpm},\
{biomass_10pct_gpm},{biomass_75pct_gpm},{biomass_90pct_gpm},{biomass_total_kg},{biomass_sd_gpm},{summer_vi_mean_gpm},
{fall_vi_mean_gpm},{fraction_summer},"{satellite}","{acquisition_date}","{wrs}","{bounds}","{wgs_bounds}",{valid_pastures_cnt})""".\
                          format(product_id=product_id, key=key, pasture=pasture, ranch=ranch, total_px=total_px,
                                 snow_px=snow_px, water_px=water_px, aerosol_px=aerosol_px, valid_px=valid_px,
                                 coverage=coverage, model=model, biomass_mean_gpm=biomass_mean_gpm,
                                 biomass_ci90_gpm=biomass_ci90_gpm, biomass_10pct_gpm=biomass_10pct_gpm,
                                 biomass_75pct_gpm=biomass_75pct_gpm, biomass_90pct_gpm=biomass_90pct_gpm,
                                 biomass_total_kg=biomass_total_kg, biomass_sd_gpm=biomass_sd_gpm,
                                 summer_vi_mean_gpm=summer_vi_mean_gpm, fall_vi_mean_gpm=fall_vi_mean_gpm,
                                 fraction_summer=fraction_summer, satellite=satellite,
                                 acquisition_date=acquisition_date, wrs=wrs, bounds=bounds, wgs_bounds=wgs_bounds,
                                 valid_pastures_cnt=valid_pastures_cnt)

                try:
                    c.execute(query)
                except:
                    print(query)
    # Save (commit) the changes
    conn.commit()

    # We can also close the connection if we are done with it.
    # Just be sure any changes have been committed or they will be lost.
    conn.close()
