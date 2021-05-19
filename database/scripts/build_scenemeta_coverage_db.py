import sqlite3
from glob import glob
from datetime import date
import os

from os.path import exists as _exists
import csv
from os.path import join as _join
from os.path import exists
import shutil
import sys
import multiprocessing

sys.path.insert(0, '/var/www/rangesat-biomass')
from database.pasturestats import query_scene_product_ids
from api.app import RANGESAT_DIRS, Location
from all_your_base import isfloat

location = sys.argv[-1]

_location = None
for rangesat_dir in RANGESAT_DIRS:
    loc_path = _join(rangesat_dir, location)
    if exists(loc_path):
        _location = Location(loc_path)
        break

assert _location is not None

out_dir = _location.out_dir
key_delimiter = _location.key_delimiter

db_fn = _join(out_dir, 'sqlite3.db')
cov_db_fn = _join(out_dir, 'scenemeta_coverage.db')

NCPU = multiprocessing.cpu_count() - 1

conn = sqlite3.connect(db_fn)
c = conn.cursor()

query = 'SELECT product_id, coverage FROM pasture_stats'

c.execute(query)
rows = c.fetchall()


def calc_scene_coverage(product_id):
    """
    determines the fraction of pastures with coverage over the coverage_threshold_fraction
    """
    x = 0.0
    n = 0
    for row in rows:
        if product_id == row[0]:
            v = row[1]
            if v is not None:
                x += v
            n += 1

    return product_id, x / float(n)


if __name__ == "__main__":

    pool = multiprocessing.Pool(NCPU)

    if _exists(cov_db_fn):
        os.remove(cov_db_fn)

    product_ids = query_scene_product_ids(db_fn)

    conn = sqlite3.connect(cov_db_fn)
    c = conn.cursor()

    # Create table
    c.execute("""CREATE TABLE scenemeta_coverage
                 (product_id TEXT, coverage REAL)""")

    for product_id, coverage in pool.imap_unordered(calc_scene_coverage, product_ids):
        print(product_id, coverage)

        query = 'INSERT INTO scenemeta_coverage VALUES ("{product_id}",{coverage})'\
                .format(product_id=product_id, coverage=coverage)

        c.execute(query)

    # Save (commit) the changes
    conn.commit()

    # We can also close the connection if we are done with it.
    # Just be sure any changes have been committed or they will be lost.
    conn.close()
