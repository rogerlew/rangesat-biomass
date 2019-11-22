import sqlite3
from glob import glob
from datetime import date
import os
import csv
from os.path import join as _join
from os.path import exists
import shutil
import sys

from database.pasturestats import query_scene_product_ids


def query_scene_coverage(db_fn, product_id):
    """
    determines the fraction of pastures with coverage over the coverage_threshold_fraction
    """
    conn = sqlite3.connect(db_fn)
    c = conn.cursor()

    query = 'SELECT biomass_mean_gpm FROM pasture_stats WHERE product_id = "{product_id}"'\
            .format(product_id=product_id)

    c.execute(query)
    rows = c.fetchall()
    res = [row[0] is not None for row in rows]
    return sum(res) / len(res)


def query_scenes_coverage(scn_cov_db_fn, product_ids):
    conn = sqlite3.connect(scn_cov_db_fn)
    c = conn.cursor()

    query = 'SELECT * FROM scenemeta_coverage'

    c.execute(query)
    rows = c.fetchall()
    d = {product_id: coverage for product_id, coverage in rows}

    res = []
    for product_id in product_ids:
        res.append(d.get(product_id, 0.0))

    return res


if __name__ == "__main__":
    db_fn = '/Users/roger/Downloads/sqlite3.db'
    cov_db_fn = '/Users/roger/Downloads/scenemeta_coverage.db'

    product_ids = query_scene_product_ids(db_fn)

    #print(query_scenes_coverage(cov_db_fn, product_ids))
    #sys.exit()

    conn = sqlite3.connect(cov_db_fn)
    c = conn.cursor()

    # Create table
    c.execute("""CREATE TABLE scenemeta_coverage
                 (product_id TEXT, coverage REAL)""")

    for product_id in product_ids:

        coverage = query_scene_coverage(db_fn, product_id)
        print(product_id, coverage)

        query = 'INSERT INTO scenemeta_coverage VALUES ("{product_id}",{coverage})'\
                .format(product_id=product_id, coverage=coverage)

        c.execute(query)

    # Save (commit) the changes
    conn.commit()

    # We can also close the connection if we are done with it.
    # Just be sure any changes have been committed or they will be lost.
    conn.close()
