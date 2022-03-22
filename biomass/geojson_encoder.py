import geojson


if __name__ == "__main__":
    import sys
    infile = sys.argv[-2]
    outfile = sys.argv[-1]

    geojson.geometry.DEFAULT_PRECISION = 5

    assert infile.endswith('json')
    assert outfile.endswith('json')

    with open(infile) as fp:
        js = geojson.load(fp)

    with open(outfile, 'w') as fp:
        geojson.dump(js, fp)

