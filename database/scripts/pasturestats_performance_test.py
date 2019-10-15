import sys
from os.path import exists
from os.path import join as _join
from os.path import split as _split
import numpy as np

sys.path.insert(0, '/var/www/rangesat-biomass')
sys.path.insert(0, '/Users/roger/rangesat-biomass')

from database import pasturestats

db_fn = '/geodata/rangesat/Zumwalt/analyzed_rasters/sqlite3.db'

if not exists(db_fn):
    db_fn = '/Users/roger' + db_fn

assert exists(db_fn), db_fn

res = pasturestats.query_interyear_pasture_stats(db_fn, ranch=None, start_year=1980, end_year=2019,
                                                 start_date='4-1', end_date='7-31',
                                                 agg_func=np.mean, key_delimiter='+')

#print(len(res))
