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

### Old QNAS for Landsat Collection 1

Store of raw Landsat Collection 1 scenes from earth explorer

- Physically in IRIC 121 closet
- Configured in `/etc/`fstab as CIFS share
- hostname is `torch.aa.uidaho.edu`
- share name is `bunchgrass`
- mounted to `/geodata/torch-landsat/`
- Not currently connected or available. not sure why? 12/14/2023
- Earth Explorer retired Collection 1 end of 2022


## RCDS NAS for Landsat Collection 2 scenes 

in `/geodata/nas/landsat/zumwalt/<year>`


## Workflow for processing Zumwalt scenes

### Acquire clean Landsat 7 / 8 / 9 Collection 2 scenes from Earth Explorer

- Done manually to visual inspect cloud cover on the Zumwalt

### Download scenes to server

- place in `/geodata/nas/landsat/zumwalt/<year>`

(I think I used wget to download scenes)

### Process scenes

#### Background

`/var/www/rangesat-biomass/biomass/scripts` contains .yaml site configuation files and scripts to process the scenes

"processing" a scene essentially extracts and crops a landsat scene to a directory and creates a raster stack of biomass estimates

The current Zumwalt config is `zumwalt4_config.yaml`

The processed scenes are in `/geodata/nas/rangesat/Zumwalt4/analyzed_rasters`

After the scenes are processed pasture level statistics are calculated an stored in a .csv file. Each scene has its own .csv

These are also in the `analyzed_rasters` directory stored by `<scene_id>_pasture_stats.csv`

(The .csv files are aggregated into an sqlite3 database that is used by the API)

**Recommeded**: spot check .csv files to make sure they contain data

#### Scripts

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

it expects the site config as a command line arguement

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

#### Scene Processing Details

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

Example model configuration
```yaml
models:
    - name: Herbaceous
      satellite_pars:
          - satellite: 9
            discriminate_threshold: 0.38
            discriminate_index: ndvi
            summer_int: 101.09
            summer_slp: 330.25│/var/www/rangesat-biomass/database/scripts/
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

#### Updating Models/Pastures and Reprocessing

If the model parameters or pastures shapefile need updating it is possible to reprocess just the biomass statistics without having to crop/extract each scene using the `recalc_pasture_stats.py` script. it will iterate over existing scenes and rebuild just the biomass grids and extract pasture statistics

```bash
> cd /var/www/rangesat-biomass/biomass/scripts/
> python3 recalc_pasture_stats.py zumwalt4_config.yaml
```


### Build Database

The API uses sqlite3 as a database. Sqlite3 is a light weight file based relational database. The database is only for the pasture statistics and is readonly.

The database has two files:

`/geodata/nas/rangesat/Zumwalt4/analyzed_rasters/sqlite3.db` aggregated table of all of the pasturestats.csv files

`/geodata/nas/rangesat/Zumwalt4/analyzed_rasters/scenemeta_coverage.db` has coverage (valid cloud free areas) by scene and pasture. this is used to support API filtering.

#### 1. (Recommended) backup the .db files

#### 2. Build the sqlite3.db

```bash
> cd /var/www/rangesat-biomass/database/scripts/
> python3 build_sqlite_db.py Zumwalt4
```

#### 3. Build the scenemeta_coverage.db

Then build the scenemeta_coverage.db. This script reads the sqlite3.db so it needs to be done sond

```bash
> cd /var/www/rangesat-biomass/database/scripts/
> python3 build_scenemeta_coverage_db.py Zumwalt4
```


### test api routes (See API Orientation)

Make sure the new scenes are processed

## API orientation

The rangesat API is implemented as a Python Flask app. The api is in 
rangesat-biomasss/api

