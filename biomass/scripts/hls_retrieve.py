from ..hls import HLSManager

bbox=[-117.8270, 46.0027, -116.5416, 45.3048]
manager = HLSManager()
mgrss = manager.identify_mgrs_from_bbox(bbox=bbox)

for _mgrs in mgrss:
    for sat in 'LS':
        for year in [2020]:
            listing = manager.query(mgrs=_mgrs, sat=sat, year=year)
            for identifier in listing:
                manager.retrieve(identifier, datadir='/geodata/hls')
