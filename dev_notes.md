# Developer Notes


## Data, file, server locations

`rangesat.org` is hosted by RCDS

has 2.4 TB NAS share provided by RCDS

### API Repository

API is in `/var/www/rangesat-biomass`

configured as WSGI service in /etc/apache2/sites-available/default-ssl.conf

### Front end

Hosted by apache2

site is in `/var/www/html/rangesat/web`

### Old QNAS for Landsat Collection 1 (deprecated)

Store of raw Landsat Collection 1 scenes from earth explorer

- Physically in IRIC 121 closet
- Configured in `/etc/`fstab as CIFS share
- hostname is `torch.aa.uidaho.edu`
- share name is `bunchgrass`
- mounted to `/geodata/torch-landsat/`
- Not currently connected or available (offline as of 12/14/2023).
- Earth Explorer retired Collection 1 end of 2022


## RCDS NAS for Landsat Collection 2 scenes 

in `/geodata/nas/landsat/zumwalt/<year>`


## Workflow for processing new scenes for Zumwalt (Adding a new year)

#### 1. Acquire clean Landsat 7 / 8 / 9 Collection 2 scenes from Earth Explorer

- Done manually to visual inspect cloud cover on the Zumwalt
- Just acquire 043208 scenes. These fully cover the Zumwalt area. There are row/paths that intersect the Zumwalt but if you get both for the same date the api can get confused.
- There is no true merging of same-date row/paths. The API only lets you filter by multiple rowpaths (e.g., `rowpath=042028 043028`) but will still pick a single product_id when it needs one.

##### Row/path mixing gotchas
- If you process multiple WRS paths/rows for the same date, the API will still return a single product_id for “latest/closest-date” lookups; whichever CSV happens to be found first wins.
- Default Zumwalt API rowpath filters are `042028 043028` for listing and `042028` for most raster/processing routes, so you must pass `rowpath=` explicitly when you want other paths.
- The current `database/scripts/build_sqlite_db.py` skips Zumwalt CSVs containing `042029`; remove that guard before rebuilding if you need R029 scenes in the DB.
- There is no on-the-fly mosaic/merge of overlapping paths; if you need mosaicked same-date coverage, build that before ingest.

#### 2. Download scenes to server

- place in `/geodata/nas/landsat/zumwalt/<year>`

(I think I used wget to download scenes)

#### 3. Process scenes

##### Background

`/var/www/rangesat-biomass/biomass/scripts` contains .yaml site configuation files and scripts to process the scenes

"processing" a scene essentially extracts and crops a landsat scene to a directory and creates a raster stack of biomass estimates

The current Zumwalt config is `zumwalt4_config.yaml`

The processed scenes are in `/geodata/nas/rangesat/Zumwalt4/analyzed_rasters`

After the scenes are processed pasture level statistics are calculated an stored in a .csv file. Each scene has its own .csv

These are also in the `analyzed_rasters` directory stored by `<scene_id>_pasture_stats.csv`

(The .csv files are aggregated into an sqlite3 database that is used by the API)

**Recommeded**: spot check .csv files to make sure they contain data

##### Scripts

**Highly recommended**

Run scripts from `tmux` or `byobu`. These are terminal multiplexers that will maintain a persistent shell session in the event your ssh connection is dropped.

`/var/www/rangesat-biomass/biomass/scripts/process_scene.py` processes a single scene it expects two command line arguments

- path to .yaml config file
- path to archive of scene to process (.tar .tar.gz)

Example usage:
```bash
> cd /var/www/rangesat-biomass/biomass/scripts/
> python3 process_scene.py zumwalt4_config.yaml /geodata/nas/landsat/zumwalt/2021/LC08_L2SP_042028_20211017_20211026_02_T1.tar
```

`/var/www/rangesat-biomass/biomass/scripts/process_scenes.py` processes a collection of scenes for a single site

it expects the site config as a command line arguement **and (as of 12/20/2024) the landsat_scene_directory** (e.g. /geodata/nas/landsat/zumwalt/)

It will apply recursive glob to find scenes relative to the `landsat_scene_directory`

Example usage:

```bash
> cd /var/www/rangesat-biomass/biomass/scripts/
> python3 process_scenes.py zumwalt4_config.yaml
```

It is currently setup to process a single year of scenes. The directory of the scenes in hardcoded on line 85

```python
 landsat_scene_directory = '/geodata/nas/landsat/zumwalt/2020'
```

line 132 has it skip over scenes that have already been processed

```python
fns = [fn for fn in fns if not is_processed(fn)]
```

This script uses subprocess to call the process_scene.py script.

##### Scene Processing Details

`biomass.landsat` has a Landsat class that can handle Collection 1 (5/7/8) and Collection 2 (7/8/9) datasets.

It provides functionality to:
- crop scenes
- unzip .tar and .tar.gz archives
- identify if scene bounds overlap extent bounds
- qa masking
- implements property based band access (e.g. normalized access for `blue` across datasets)
- computes vegetation metrics
- landsat 7/8,9 correction factors

`biomass.rangesat_biomass` processes biomass models defined in yaml site configuration files. These have parameters for models by satellite and season (fall, spring)

##### Example model from yaml

```yaml
models:
    - name: Herbaceous
      satellite_pars:
          - satellite: 9
            discriminate_threshold: 0.38
            discriminate_index: ndvi
            summer_int: 101.09
            summer_slp: 330.25
            summer_index: nbr
            fall_int: -58.04
            fall_slp: 1070.64
            fall_index: nbr2
            required_coverage: 0.5
            minimum_area_ha: 1.8
            log_transformed_estimate: False
          - satellite: 8
            ...
```

The .yaml configuation also defines the shapefile to extract pasture statistics.

```yaml
sf_fn: /geodata/nas/rangesat/Zumwalt4/vector_data/Zumwalt2023.shp
```

##### Spatial Reference

The landsat scenes are cropped in their original spatial reference. The Zumwalt scenes are in UTM 11N. Likely the sf_fn needs to be in the same projection as the landsat scenes.


##### Updating Models/Pastures and Reprocessing

If the model parameters or pastures shapefile need updating it is possible to reprocess just the biomass statistics without having to crop/extract each scene using the `recalc_pasture_stats.py` script. it will iterate over existing scenes and rebuild just the biomass grids and extract pasture statistics

```bash
> cd /var/www/rangesat-biomass/biomass/scripts/
> python3 recalc_pasture_stats.py zumwalt4_config.yaml
```


#### 4. Build Database

The API uses sqlite3 as a database. Sqlite3 is a light weight file based relational database. The database is only for the pasture statistics and is readonly.

The database has two files:

`/geodata/nas/rangesat/Zumwalt4/analyzed_rasters/sqlite3.db` aggregated table of all of the pasturestats.csv files

`/geodata/nas/rangesat/Zumwalt4/analyzed_rasters/scenemeta_coverage.db` has coverage (valid cloud free areas) by scene and pasture. this is used to support API filtering.

##### 4.1. (Recommended) backup the .db files

##### 4.2. Build the sqlite3.db

```bash
> cd /var/www/rangesat-biomass/database/scripts/
> python3 build_sqlite_db.py Zumwalt4
```

##### 4.3. Build the scenemeta_coverage.db

Then build the scenemeta_coverage.db. This script reads the sqlite3.db so it needs to be done sond

```bash
> cd /var/www/rangesat-biomass/database/scripts/
> python3 build_scenemeta_coverage_db.py Zumwalt4
```

##### Note

[DB Browser for SQLite](https://sqlitebrowser.org/) is a handy tool for viewing the contents of the *.db files. It can also export tables to .csv

#### 5. Restart API

The Flask API caches responses for many routes. The cache can be reset with

```bash
> sudo apachectl graceful
```

#### 6. Test api routes (See API Orientation)

Make sure the new scenes are processed


#### 7. Configure frontend

Ask Jen, Just adding years might require adding a new select option.

If ranches are added to the shape file or site or if new user accounts are needed, Jen can set them up.

Or if the name of the biomass models are changed, frontend might need updated.

Or if the site name is revised e.g. Zumwalt4 -> Zumwalt5 frontend might need updated.

## API orientation

The rangesat API is implemented as a python3 Flask app. The api is in `rangesat_biomass/api`

**The API cache's routes so you may need to restart apache to get the routes to actual reprocess requests**

```bash
sudo apachectl stop
sudo apachectl start
```

The api endpoints that are used by rangesat.org are:

https://rangesat.org/api/geojson/Zumwalt4/The_Nature_Conservancy/

https://rangesat.org/api/pasturestats/intra-year/Zumwalt4/?year=2022&start_date=6-1&end_date=6-30&ranch=The_Nature_Conservancy

https://rangesat.org/api/pasturestats/intra-year/Zumwalt4/?year=2022&start_date=5-1&end_date=6-30&ranch=The_Nature_Conservancy

https://www.rangesat.org/api/scenemeta/Zumwalt4/?pasture_coverage_threshold=0.5&filter=latest

https://www.rangesat.org/api/scenemeta/Zumwalt4/?pasture_coverage_threshold=0.5

https://rangesat.org/api/raster/Zumwalt4/LC08_L1TP_043028_20210906_20210916_01_T1/biomass/?ranches=[%27The_Nature_Conservancy%27]

https://www.rangesat.org/api/histogram/single-scene/Zumwalt4/The_Nature_Conservancy/?bins=[0,112.08511557166881,168.12767335750323,500]&product=biomass&product_id=LC08_L1TP_043028_20210906_20210916_01_T1

https://www.rangesat.org/api/histogram/single-scene-bypasture/Zumwalt4/The_Nature_Conservancy/?bins=[0,112.08511557166881,168.12767335750323,500]&product=biomass&product_id=LC08_L1TP_043028_20210906_20210916_01_T1

https://rangesat.org/api/raster-processing/difference/Zumwalt4/biomass/?product_id=LC08_L1TP_043028_20210906_20210916_01_T1&product_id2=LC08_L1TP_043028_20210602_20210608_01_T1&ranches=[%27The_Nature_Conservancy%27]

https://rangesat.org/api/gridmet/single-year-monthly/Zumwalt4/The_Nature_Conservancy/A1/?year=2022&units=English

https://rangesat.org/api/gridmet/annual-progression-monthly/Zumwalt4/The_Nature_Conservancy/A1/?year=2022&units=English

https://rangesat.org/api/pasturestats/seasonal-progression/Zumwalt4/?ranch=The_Nature_Conservancy&pasture=A1

https://rangesat.org/api/pasturestats/single-year-monthly/Zumwalt4/?ranch=The_Nature_Conservancy&pasture=A1&year=2022

https://rangesat.org/api/pasturestats/multi-year/Zumwalt4/?ranch=The_Nature_Conservancy&pasture=A1&start_year=1984&end_year=2022&start_date=05-15&end_date=07-15&agg_func=mean&units=English


### Site Configuration

The configuration of the sites is directory based.

`all_your_base.RANGESAT_DIRS` defines a short list of directories that can contain sites. Sites are defined by the name of the directory.

Would recommend putting sites in `/geodata/nas/rangesat`

Use `du -h` to see available disk usage on the NAS. Ask Luke Sheneman for more space if needed.

The api provides geojson resources to clients. To accomplish this it needs a WGS `pastures.geojson` in the project root. This json should have the same shapes as the `sf_fn` file used to process the scenes.

There is also a `config.yaml` in the site directory.

The `config.yaml` also specifies parameters that are used by `database.location.Location` 

`sf_fn` is a shapefile path that is used by to load the pasture metadata

`sf_feature_properties_key` specifies a column name from the attribute table containing ranch and pasture IDs.

`sf_feature_properties_delimiter` specifies a delimiter character that splits ranch and pasture names in the sf_feature_properties_key column. It defaults to `+`

`models` I believe is unused by the api

`out_dir` specifies the analyzed_rasters directory that the scenes are processed into. it is used to find the .db files for the location

`reverse_key` specifies whether to unpack sf_feature_properties_key as pasture, ranch or ranch, pasture.

(Zumwalt4 is with reverse_key = true, this was done as a hotfix that I regret. would be better to just change the shapefiles...)


#### raster masks

The scene processing does not crop individual rasters. To serve rasters for single ranch locations raster masks are needed for fast processing. In otherwords each ranch has it's own set of raster masks: a utm raster mask and a wgs raster maske. It should work without them, but is faster with them. The raster masks are in raster_masks. Each ranch has a utm and a wgs raster mask.

The script to build the masks is 'database/scripts/make_raster_masks.py'


### Gridmet data

Gridmet data is acquired through a gridmet web client. The client is `climate/gridmet/client.py` The bottom contains an if `__name__ == "__main__":` section to acquire gridmet data for new sites.

The `climate/gridmet/scripts/sync_current_year.py` script is using a daily crontab to update location climate data.

edit with `sudo crontab -e`

The climate data is saved as .npy binary files as `<location>/gridmet/<ranch>/<pasture>/<year>/<measure>.npy`

## Deploying New Pasture Map / Deploying Revised Models

The strategy is build the new database along side the existing databases and then swap the frontend to the new database.

**Note: since the "torch" Q-NAS is down, we only have 2016-2024 scenes available**

The configurations for the databases are in `/var/www/rangesat-biomass/biomass/scripts/`

1. Create a new config by copying existing config.

e.g.
```bash
cd /var/www/rangesat-biomass/biomass/scripts/
cp zumwalt4_config.yaml zumwalt5_config.yaml
```

2. Then create a directory for the new database
```bash
/geodata/nas/rangesat/Zumwalt5/
```

3. Then create the shapefile needed for the biomass database

```yaml
# shapefile containing pastures to analyze
sf_fn: /geodata/nas/rangesat/Zumwalt5/vector_data/Zumwalt2025_w_PastID_Key.shp
```

Notes on this file:
also in `/geodata/nas/rangesat/Zumwalt5/vector_data/from_mike/README.md` (non-version controlled)

`Pastures_2025.lpkx` is a 7z archive

To extract archive

```bash
7z x Pastures_2025.lpkx
```

Convert to shapefile

```bash
ogr2ogr -f "ESRI Shapefile" Pastures_2025 commondata/infrastructure.gdb/
```

The Pastures_2025.lpkx is missing the PastID_Key attribute needed by rangesat.

The `PastID_Key_generation.py` was developed to add this attribute to a new shapefile. The `Zumwalt_2023.shp` is used as a lookup table with one additional manual entry. The locations of the datasets are hardcoded in the script.


```bash
python3 PastID_Key_generation.py
```


A python3 script was developed to add the PastID_Key column to the dataset:
`/geodata/nas/rangesat/Zumwalt5/vector_data/from_mike/PastID_Key_generation.py` (non-version controlled)
```python
from osgeo import ogr, osr
import sys

# Input shapefiles
shapefile2023 = "/geodata/nas/rangesat/Zumwalt4/vector_data/Zumwalt2023.shp"
shapefile2025 = "/geodata/nas/rangesat/Zumwalt5/vector_data/from_mike/Pastures_2025/Pastures_2025.shp"
output_shapefile = "/geodata/nas/rangesat/Zumwalt5/vector_data/Zumwalt2025_w_PastID_Key.shp"

# Open input shapefiles
driver = ogr.GetDriverByName("ESRI Shapefile")

# Open 2023 shapefile
source2023 = driver.Open(shapefile2023, 0)  # Read-only
if source2023 is None:
    raise Exception(f"Failed to open {shapefile2023}")
layer2023 = source2023.GetLayer()

# Open 2025 shapefile
source2025 = driver.Open(shapefile2025, 0)  # Read-only
if source2025 is None:
    raise Exception(f"Failed to open {shapefile2025}")
layer2025 = source2025.GetLayer()

# Create a lookup dictionary for Property -> MgmtUnit and PASTURE -> PastID_Key
prop_mgmt = {}
lookup = {}

for feature in layer2023:
    prop_mgmt[feature["Property"]] = feature["MgmtUnit"]
    lookup[feature["PASTURE"]] = feature["PastID_Key"]

# Add custom lookup entry
lookup['LCR 14'] = 'Midway+LCR 14'

# Create the output shapefile
if driver.DeleteDataSource(output_shapefile):
    print(f"Deleted existing {output_shapefile}")

target_ds = driver.CreateDataSource(output_shapefile)
if target_ds is None:
    raise Exception(f"Failed to create {output_shapefile}")

# Create the target layer with the same spatial reference and fields as the source layer
spatial_ref = layer2025.GetSpatialRef()
target_layer = target_ds.CreateLayer("Zumwalt2025_revised", spatial_ref, ogr.wkbPolygon)

# Copy fields from the source layer
layer_defn = layer2025.GetLayerDefn()
for i in range(layer_defn.GetFieldCount()):
    field_defn = layer_defn.GetFieldDefn(i)
    target_layer.CreateField(field_defn)

# Add the PastID_Key field explicitly
pastid_key_field = ogr.FieldDefn("PastID_Key", ogr.OFTString)
pastid_key_field.SetWidth(50)  # Set a reasonable width
if target_layer.CreateField(pastid_key_field) != 0:
    raise Exception("Failed to create PastID_Key field in target shapefile")

target_defn = target_layer.GetLayerDefn()

# Process features and write to the new shapefile
for feature in layer2025:
    pasture = feature["PASTURE"]
    _property = feature["Property"]

    pastid_key_value = None
    if pasture in lookup:
        pastid_key_value = lookup[pasture]
    else:
        mgmt = prop_mgmt.get(_property, None)
        if mgmt is not None:
            pastid_key_value = f'{mgmt}+{pasture}'

    if pastid_key_value is None:
        raise Exception(f"Could not find pasture {pasture} in lookup")

    # Create and add the updated feature to the target layer
    new_feature = ogr.Feature(target_defn)
    new_feature.SetGeometry(feature.GetGeometryRef())

    # Copy existing fields
    for i in range(target_defn.GetFieldCount()):
        field_name = target_defn.GetFieldDefn(i).GetNameRef()
        if field_name != "PastID_Key":
            new_feature.SetField(field_name, feature.GetField(field_name))

    # Set the new PastID_Key field
    new_feature.SetField("PastID_Key", pastid_key_value)

    target_layer.CreateFeature(new_feature)
    new_feature = None  # Clear memory

# Cleanup
target_ds = None
source2023 = None
source2025 = None

print(f"Revised shapefile written to {output_shapefile}")
```

4. Create the WGS pastures.geojson

```bash
cd /geodata/nas/rangesat/Zumwalt5/
ogr2ogr -f "GeoJSON" -t_srs "EPSG:4326" pastures.geojson vector_data/Zumwalt2025_w_PastID_Key.shp
```

At onepoint we need the shapefiles `PastID_Key` to support `RANCH+PASTURE` and `PASTURE+RANCH`.
The `reverse_key=True` in the database tells it to use `RANCH+PASTURE`.

**However, the `pastures.geojson` always needs to be `PASTURE+RANCH`**

There is a `swap_pasture_ranch.py` script that swaps it inplace if the keys are RANCH+PASTURE

```python
import json

fn = 'pastures.geojson'
with open(fn) as fp:
    js = json.load(fp)

key_delimiter = '+'

for i, feature in enumerate(js['features']):
    k = feature['properties']['PastID_Key']
    k1, k2 = k.split(key_delimiter)
    js['features'][i]['properties']['PastID_Key'] = f'{k2}{key_delimiter}{k1}'
    print(k2,k1)

with open(fn, 'w') as fp:
    json.dump(js, fp)
```

The front-end uses the MgmtUnit feature attribute for labeling.

If it isn't present it can be added as such
```python
import json

fn = 'pastures.geojson'
with open(fn) as fp:
    js = json.load(fp)

key_delimiter = '+'

for i, feature in enumerate(js['features']):
    k = feature['properties']['PASTURE']
    js['features'][i]['properties']['MgmtUnit'] = k
    print(k)

with open(fn, 'w') as fp:
    json.dump(js, fp)

```

5. Create config.yaml in database directory
```bash
cp ../config.yaml .
```

update the `config.yaml` as necessary

5. Run `/var/www/rangesat-biomass/biomass/scripts/process_scenes.py` to build the rasters and pasture stats csv files

```bash
python3 /var/www/rangesat-biomass/biomass/scripts/process_scenes.py zumwalt5_config.yaml /geodata/nas/landsat/zumwalt/
```

Or you can multiprocess by running process_scenes.py in parallel for each year within different byobu panes.

6. Build the .db database files

```bash
cd /var/www/rangesat-biomass/database/scripts
python3 build_sqlite_db.py Zumwalt5
python3 build_scenemeta_coverage_db.py Zumwalt5
```

verify they exist

```bash
ls -ltra /geodata/nas/rangesat/Zumwalt5/analyzed_rasters/*.db
```

6. Make raster masks

```bash
cd /var/www/rangesat-biomass/database/scripts
python3 make_raster_masks.py Zumwalt5 
```

Then check for files with

```bash
ls /geodata/nas/rangesat/Zumwalt5/raster_masks/
```

7. Build GRIDMET Climate database

```bash
cd /var/www/rangesat-biomass/climates/gridmet
python3 client.py Zumwalt5 
```

8. Make `www-data` user and `webgroup` group owner of database
```bash
cd /geodata/nas/rangesat
sudo chown -R www-data:webgroup Zumwalt5  
sudo chmod -R 775 Zumwalt5
```

9. Check endpoints

(they look okay to me)

Note: Zumwalt5 has only 6 Ranches:

- 6_Ranch
- Krebs
- McClaran
- Midway
- The_Nature_Conservancy
- Triple_Creek 
