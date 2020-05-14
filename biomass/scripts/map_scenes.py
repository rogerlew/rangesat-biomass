
product_ids = """\
LC08_L1TP_042028_20200116_20200128_01_T1
LC08_L1TP_042028_20200201_20200211_01_T1
LC08_L1TP_042028_20200217_20200225_01_T1
LC08_L1TP_042028_20200304_20200314_01_T1
LC08_L1TP_042028_20200320_20200326_01_T1
LC08_L1TP_042028_20200405_20200410_01_T1
LC08_L1TP_042029_20200217_20200225_01_T1
LC08_L1TP_042029_20200304_20200314_01_T1
LC08_L1TP_042029_20200320_20200326_01_T1
LC08_L1TP_042029_20200405_20200410_01_T1
LC08_L1TP_043028_20200208_20200211_01_T1
LC08_L1TP_043028_20200224_20200313_01_T1
LC08_L1TP_043028_20200311_20200325_01_T1
LC08_L1TP_043028_20200412_20200422_01_T1
LE07_L1TP_042028_20200108_20200205_01_T1
LE07_L1TP_042028_20200209_20200307_01_T1
LE07_L1TP_042028_20200225_20200322_01_T1
LE07_L1TP_042028_20200312_20200407_01_T1
LE07_L1TP_042028_20200328_20200423_01_T1
LE07_L1TP_042029_20200209_20200307_01_T1
LE07_L1TP_042029_20200225_20200322_01_T1
LE07_L1TP_042029_20200312_20200407_01_T1
LE07_L1TP_042029_20200328_20200423_01_T1
LE07_L1TP_043028_20200115_20200210_01_T1
LE07_L1TP_043028_20200303_20200329_01_T1
LE07_L1TP_043028_20200319_20200414_01_T1
LE07_L1TP_043028_20200404_20200430_01_T1""".split()

url = 'https://www.rangesat.org/api/raster/Zumwalt/{product_id}/rgb'

for product_id in product_ids:
    print(url.format(product_id=product_id))
