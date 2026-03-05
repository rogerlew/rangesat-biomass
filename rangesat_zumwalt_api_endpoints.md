# RangeSAT Zumwalt Prairie — API Endpoint Reference

**Purpose:** Troubleshooting Zumwalt5 compatibility by comparing against known-working Zumwalt4 endpoints.
**Base URL:** `https://rangesat.org/api/`
**Site identifier (Zumwalt4):** `Zumwalt4`
**Ranch identifier (TNC):** `The_Nature_Conservancy`
**Generated:** 2026-03-04

---

## Hidden Page Variables

Each tool page embeds hidden inputs that drive API calls:

| Variable    | HTML ID      | Zumwalt4 Value            | Notes                                       |
|-------------|-------------|---------------------------|----------------------------------------------|
| `site`      | `#site`     | `Zumwalt4`                | Previously `Zumwalt3`; comment says "Zumwalt3 or SageSteppe" |
| `usrRanch`  | `#siteRanch`| `The_Nature_Conservancy`  | Other option: `Rinker_Rock_Creek_Ranch`       |

---

## 1. GeoJSON — Pasture Boundaries

**Used by:** All map tools (Pasture Averages, Pixel View, Threshold, Difference)

### Endpoint
```
GET /api/geojson/{site}/{ranch}
```

### Example Query
```
GET https://rangesat.org/api/geojson/Zumwalt4/The_Nature_Conservancy
```

### Expected Response
- **Type:** GeoJSON `FeatureCollection`
- **CRS:** `urn:ogc:def:crs:EPSG::4236`
- **Features count:** 86 pasture polygons
- **Feature properties:**

| Property      | Type    | Example Value                                  |
|---------------|---------|------------------------------------------------|
| `Acres`       | float   | `232.06`                                       |
| `Hectares`    | float   | `93.91`                                        |
| `Manager`     | string  | `"The Nature Conservancy"`                     |
| `MgmtUnit`    | string  | `"The Nature Conservancy"`                     |
| `Owner`       | string  | `"The Nature Conservancy"`                     |
| `PASTURE`     | string  | `"Main Gate South"`                            |
| `PastID_Key`  | string  | `"Main Gate South+The Nature Conservancy"`     |
| `Property`    | string  | `"zpp"`                                        |
| `Shape_Area`  | float   | `939117.19`                                    |
| `Shape_Leng`  | float   | `4768.21`                                      |

- **Geometry:** MultiPolygon / Polygon with 3D coordinates `[lon, lat, 0.0]`

---

## 2. Scene Metadata

**Used by:** Pixel View Map, Threshold Map, Difference Map
**JS files:** `scene_select_auto.js`, `map_scene_select_submit_btn.js`, `scene_select_diff_anon.js`

### 2a. List All Scenes
```
GET /api/scenemeta/{site}?pasture_coverage_threshold=0.5
```

### Example Query
```
GET https://rangesat.org/api/scenemeta/Zumwalt4?pasture_coverage_threshold=0.5
```

### Expected Response
- **Type:** JSON array of Landsat scene ID strings
- **Length:** ~1500 scenes (spanning 1984–2025)
- **Example entries:**
  - `"LT05_L1TP_043028_19840425_20161004_01_T1"` (earliest)
  - `"LC09_L2SP_043028_20250925_20250926_02_T1"` (latest)
- **Scene ID format:** `{sensor}_L{level}_{path}{row}_{acqdate}_{procdate}_{collection}_{tier}`

### 2b. Latest Scene Only
```
GET /api/scenemeta/{site}?pasture_coverage_threshold=0.5&filter=latest
```

### Example Query
```
GET https://rangesat.org/api/scenemeta/Zumwalt4?pasture_coverage_threshold=0.5&filter=latest
```

### Expected Response
- **Type:** JSON string (single scene ID, quoted)
- **Example:** `"LC09_L2SP_043028_20250925_20250926_02_T1"`

---

## 3. Raster Tiles (GeoTIFF)

**Used by:** Pixel View Map, Threshold Map, Difference Map
**Consumed by:** Leaflet `leafletGeotiff` plugin

### 3a. Single-Scene Raster
```
GET /api/raster/{site}/{scene_id}/{product}/
GET /api/raster/{site}/{scene_id}/{product}/?ranches=['{ranch}']
```

### Parameters
| Parameter   | Values                                                  |
|-------------|---------------------------------------------------------|
| `product`   | `biomass`, `ndvi`, `nbr`, `nbr2`, `fall_vi`, `summer_vi` |
| `ranches`   | URL-encoded array, e.g. `['The_Nature_Conservancy']`    |

### Example Queries
```
GET https://rangesat.org/api/raster/Zumwalt4/LC09_L2SP_043028_20250925_20250926_02_T1/biomass/
GET https://rangesat.org/api/raster/Zumwalt4/LC09_L2SP_043028_20250925_20250926_02_T1/ndvi/?ranches=['The_Nature_Conservancy']
```

### Expected Response
- **Type:** GeoTIFF binary raster
- **Band:** 0
- **Display ranges (from JS):**
  - NDVI/NBR/NBR2: `displayMin: -9998, displayMax: 10000`
  - Biomass: `displayMin: 2, displayMax: 280`

### 3b. Difference Raster (two scenes)
```
GET /api/raster-processing/difference/{site}/biomass/?product_id={scene1}&product_id2={scene2}&ranches=['{ranch}']
```

### Example Query
```
GET https://rangesat.org/api/raster-processing/difference/Zumwalt4/biomass/?product_id=LC08_L2SP_043028_20250917_20250929_02_T1&product_id2=LC09_L2SP_043028_20250925_20250926_02_T1&ranches=['The_Nature_Conservancy']
```

### Expected Response
- **Type:** GeoTIFF binary raster (relative difference values)
- **Display range:** `displayMin: -0.99, displayMax: 0.99`

---

## 4. Pasture Statistics — Intra-Year (Monthly Averages)

**Used by:** Pasture Averages Map
**JS file:** `map_pastureAvg_anon.js`

### Endpoint
```
GET /api/pasturestats/intra-year/{site}/?year={year}&start_date={mm}-1&end_date={mm}-{dayEnd}&ranch={ranch}
```

### Example Query
```
GET https://rangesat.org/api/pasturestats/intra-year/Zumwalt4/?year=2025&start_date=6-1&end_date=6-30&ranch=The_Nature_Conservancy
```

### Expected Response
- **Type:** JSON array of objects (one per pasture)
- **Length:** 85 records (one per pasture within the ranch)
- **Record fields:**

| Field                 | Type   | Description                              |
|-----------------------|--------|------------------------------------------|
| `pasture`             | string | Pasture name                             |
| `ranch`               | string | Ranch/management unit name               |
| `date_period`         | string | e.g. `"Jun 1 - Jun 30"`                 |
| `biomass_mean_gpm`    | float  | Mean biomass (g/m²)                      |
| `biomass_sd_gpm`      | float  | Std deviation of biomass                 |
| `biomass_10pct_gpm`   | float  | 10th percentile biomass                  |
| `biomass_75pct_gpm`   | float  | 75th percentile biomass                  |
| `biomass_90pct_gpm`   | float  | 90th percentile biomass                  |
| `biomass_ci90_gpm`    | float  | 90% confidence interval                  |
| `biomass_total_kg`    | float  | Total biomass in kg                      |
| `ndvi_mean`           | float  | Mean NDVI                                |
| `ndvi_sd`             | float  | NDVI std deviation                       |
| `ndvi_10pct`          | float  | NDVI 10th percentile                     |
| `ndvi_75pct`          | float  | NDVI 75th percentile                     |
| `ndvi_90pct`          | float  | NDVI 90th percentile                     |
| `ndvi_ci90`           | float  | NDVI 90% CI                              |
| `nbr_mean`            | float  | Mean NBR                                 |
| `nbr_sd`, `nbr_10pct`, `nbr_75pct`, `nbr_90pct`, `nbr_ci90` | float | NBR stats |
| `nbr2_mean`           | float  | Mean NBR2                                |
| `nbr2_sd`, `nbr2_10pct`, `nbr2_75pct`, `nbr2_90pct`, `nbr2_ci90` | float | NBR2 stats |
| `fall_vi_mean_gpm`    | float  | Fall vegetation index mean               |
| `summer_vi_mean_gpm`  | float  | Summer vegetation index mean             |

---

## 5. Pasture Statistics — Difference

**Used by:** Difference Map (CSV download)
**JS file:** `scene_select_diff_anon.js`

### Endpoint
```
GET /api/pasturestats/difference/{site}/biomass/?product_id={scene1}&product_id2={scene2}&ranches=['{ranch}']&csv=pasture_stats_difference
```

### Expected Response
- **Type:** CSV download (when `csv` param present) or JSON
- Contains per-pasture biomass difference statistics between two scenes

---

## 6. Histogram — Single Scene

**Used by:** Threshold Map
**JS file:** `map_threshold_anon.js`

### 6a. Ranch-Level Histogram
```
GET /api/histogram/single-scene/{site}/{ranch}/?bins={bin_values}&product={product}&product_id={scene_id}
```

### Example Query
```
GET https://rangesat.org/api/histogram/single-scene/Zumwalt4/The_Nature_Conservancy/?bins=500,750,1000,1250,1500,1750,2000&product=biomass&product_id=LC09_L2SP_043028_20250925_20250926_02_T1
```

### Expected Response
```json
{
  "bins": [500.0, 750.0, 1000.0, 1250.0, 1500.0, 1750.0, 2000.0],
  "counts": [0, 0, 0, 0, 0, 0],
  "masked": 92900,
  "product": "biomass",
  "product_id": "LC09_L2SP_043028_20250925_20250926_02_T1",
  "ranch": "The_Nature_Conservancy",
  "total_px": 92900
}
```

### 6b. Pasture-Level Histogram
```
GET /api/histogram/single-scene-bypasture/{site}/{ranch}/?bins={bin_values}&product={product}&product_id={scene_id}
```

### Expected Response
- Same structure as ranch-level but broken out per pasture

---

## 7. Ranch Statistics — Single-Year Monthly

**Used by:** Single Year Analysis
**JS file:** `chart_singleYr_anon_btn.js`

### Endpoint
```
GET /api/ranchstats/single-year-monthly/{site}?ranch={ranch}&year={year}
```

### Example Query
```
GET https://rangesat.org/api/ranchstats/single-year-monthly/Zumwalt4?ranch=The_Nature_Conservancy&year=2025
```

### Expected Response
- **Type:** JSON object keyed by ranch display name
- **Structure:** `{ "The Nature Conservancy": [ {month record}, ... ] }`
- **Records:** 12 (one per month: January–December)
- **Record fields:** Same biomass/ndvi/nbr/nbr2 stats as intra-year, plus `month` (string)
- **Note:** Months without satellite data have `null` values

---

## 8. Pasture Statistics — Single-Year Monthly

**Used by:** Single Year Analysis (when a specific pasture is selected)

### Endpoint
```
GET /api/pasturestats/single-year-monthly/{site}?ranch={ranch}&pasture={pasture_name}&year={year}
```

### CSV Download Variant
Appends: `&units=en&drop={columns_to_drop}&csv=True`

---

## 9. Ranch Statistics — Seasonal Progression

**Used by:** Single Year Analysis
**JS file:** `chart_singleYr_anon_btn.js`

### Ranch-Level
```
GET /api/ranchstats/seasonal-progression/{site}?ranch={ranch}
```

### Pasture-Level
```
GET /api/pasturestats/seasonal-progression/{site}?ranch={ranch}&pasture={pasture_name}
```

### Example Query
```
GET https://rangesat.org/api/ranchstats/seasonal-progression/Zumwalt4?ranch=The_Nature_Conservancy
```

### Expected Response
- **Type:** JSON object keyed by ranch display name
- **Structure:** `{ "The Nature Conservancy": [ {record}, ... ] }`
- **Records:** 12 (one per month)
- **Record fields:** Same biomass/ndvi/nbr/nbr2 stats plus `month` (string), `ranch` (string), `year_period` (string)

---

## 10. GridMET Weather Data

**Used by:** Single Year Analysis, Multi-Year Analysis
**JS file:** `chart_singleYr_anon_btn.js`, `chart_multiYr_anon_btn.js`

### 10a. Single-Year Monthly (Ranch-Level)
```
GET /api/gridmet/single-year-monthly/{site}/{ranch}/?year={year}&units=English
```

### 10b. Single-Year Monthly (Pasture-Level)
```
GET /api/gridmet/single-year-monthly/{site}/{ranch}/{pasture}/?year={year}&units=English
```

### 10c. Annual Progression Monthly (Ranch-Level)
```
GET /api/gridmet/annual-progression-monthly/{site}/{ranch}/?year={year}&units=English
```

### 10d. Annual Progression Monthly (Pasture-Level)
```
GET /api/gridmet/annual-progression-monthly/{site}/{ranch}/{pasture}/?year={year}&units=English
```

### Example Query
```
GET https://rangesat.org/api/gridmet/single-year-monthly/Zumwalt4/The_Nature_Conservancy/?year=2025&units=English
```

### Expected Response
```json
{
  "bi":    [18.87, 9.57, ...],   // Burning index (12 monthly values)
  "months": ["January", "February", ...],
  "pdsi":  [null, null, ...],    // Palmer Drought Severity Index
  "pet":   [...],                // Potential evapotranspiration
  "pr":    [...],                // Precipitation
  "pwd":   [...],                // ?
  "srad":  [...],                // Solar radiation
  "tmmn":  [...],                // Min temperature
  "tmmx":  [...],                // Max temperature
  "year":  2025
}
```

---

## 11. Ranch Statistics — Multi-Year

**Used by:** Multi-Year Analysis
**JS file:** `chart_multiYr_anon_btn.js`

### Ranch-Level
```
GET /api/ranchstats/multi-year/{site}/?ranch={ranch}&start_year={yr1}&end_year={yr2}&start_date={mm1}-{dd1}&end_date={mm2}-{dd2}&agg_func={stat}&units=English
```

### Pasture-Level
```
GET /api/pasturestats/multi-year/{site}/?ranch={ranch}&pasture={pasture}&start_year={yr1}&end_year={yr2}&start_date={mm1}-{dd1}&end_date={mm2}-{dd2}&agg_func={stat}&units=English
```

### Parameters
| Parameter    | Description                              | Example Values           |
|-------------|------------------------------------------|--------------------------|
| `start_year` | Beginning of year range                  | `1984`                   |
| `end_year`   | End of year range                        | `2025`                   |
| `start_date` | Season start (MM-DD)                     | `05-15`                  |
| `end_date`   | Season end (MM-DD)                       | `07-15`                  |
| `agg_func`   | Aggregation function                     | `mean`, `median`, `max`  |
| `units`      | Unit system                              | `English`                |

### Expected Response
- Per-year biomass/NDVI/NBR statistics for the specified date window
- CSV download variant appends `&csv=True`

---

## 12. Ranch Statistics — Inter-Year

**Referenced in:** Multi-Year Analysis JS constants (may not be actively used in current UI)

### Endpoint
```
GET /api/ranchstats/inter-year/{site}/?ranch={ranch}&...
```

---

## Summary: API Path Patterns

| # | Endpoint Pattern | Tool(s) |
|---|-----------------|---------|
| 1 | `/api/geojson/{site}/{ranch}` | All maps |
| 2 | `/api/scenemeta/{site}?pasture_coverage_threshold=...&filter=...` | Pixel View, Threshold, Difference |
| 3 | `/api/raster/{site}/{scene}/{product}/` | Pixel View, Threshold |
| 4 | `/api/raster-processing/difference/{site}/{product}/` | Difference |
| 5 | `/api/pasturestats/intra-year/{site}/` | Pasture Averages |
| 6 | `/api/pasturestats/difference/{site}/{product}/` | Difference |
| 7 | `/api/pasturestats/single-year-monthly/{site}` | Single Year |
| 8 | `/api/pasturestats/seasonal-progression/{site}` | Single Year |
| 9 | `/api/pasturestats/multi-year/{site}/` | Multi-Year |
| 10 | `/api/ranchstats/single-year-monthly/{site}` | Single Year |
| 11 | `/api/ranchstats/seasonal-progression/{site}` | Single Year |
| 12 | `/api/ranchstats/multi-year/{site}/` | Multi-Year |
| 13 | `/api/ranchstats/inter-year/{site}/` | Multi-Year (ref'd) |
| 14 | `/api/histogram/single-scene/{site}/{ranch}/` | Threshold |
| 15 | `/api/histogram/single-scene-bypasture/{site}/{ranch}/` | Threshold |
| 16 | `/api/gridmet/single-year-monthly/{site}/{ranch}/` | Single Year |
| 17 | `/api/gridmet/annual-progression-monthly/{site}/{ranch}/` | Single/Multi-Year |

---

## Troubleshooting Notes for Zumwalt5

When comparing Zumwalt5 against Zumwalt4, check:

1. **Site identifier:** Ensure `Zumwalt5` is valid — JS comments reference `Zumwalt3` as a prior name, suggesting the site string has changed before.
2. **Hidden input values:** Verify `#site` resolves to the correct value on Zumwalt5 pages.
3. **Ranch names:** The `usrRanch` value (`The_Nature_Conservancy`) must match exactly. Some endpoints use display name `"The Nature Conservancy"` in responses but underscore-delimited in request paths.
4. **GeoJSON availability:** Test `/api/geojson/Zumwalt5/The_Nature_Conservancy` first — if this fails, no map tools will work.
5. **Scene metadata:** Test `/api/scenemeta/Zumwalt5?pasture_coverage_threshold=0.5` — if empty, pixel-based tools will have no scenes to display.
6. **Raster availability:** Raster endpoints require both the site and valid scene IDs. If scenes exist for Zumwalt5 but rasters fail, the processing pipeline may not have run.
7. **Query parameter format:** Some JS constructs URLs without `?` using `/` separators for query params (e.g., `/_year=2025_start_date=...`). Verify Zumwalt5 backend accepts both URL formats.
8. **CORS/domain:** Some JS references use `www.rangesat.org` and others use `rangesat.org` — ensure Zumwalt5 handles both.
