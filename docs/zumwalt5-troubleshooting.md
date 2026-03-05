# Zumwalt5 Troubleshooting and Verification

## Context
This document captures the March 2026 debug/fix cycle for reports that Single-Year and Multi-Year charts were failing after migration from `Zumwalt4` to `Zumwalt5`.

Primary concern from stakeholders:
- Some tools worked, but Single-Year/Multi-Year chart workflows intermittently failed.
- Suspected cause: incomplete `Zumwalt5` processing (GridMET/stats), or frontend still using stale values.

## What Was Observed

### Valid Zumwalt5 inputs
For valid ranch/pasture values, endpoints returned healthy responses (`200` with expected JSON/GeoTIFF) across:
- `geojson`
- `scenemeta`
- `raster` and `raster-processing/difference`
- `histogram/single-scene` and `histogram/single-scene-bypasture`
- `pasturestats` (`intra-year`, `single-year-monthly`, `seasonal-progression`, `multi-year`, `difference`, `inter-year`)
- `ranchstats` (`single-year-monthly`, `seasonal-progression`, `multi-year`)
- `gridmet` (`single-year-monthly`, `annual-progression-monthly`, ranch and pasture routes)

### Legacy pasture names
When a legacy/nonexistent pasture name (for example `A1`) was supplied to `Zumwalt5`:
- `pasturestats/single-year-monthly` returned `{}`.
- `pasturestats/multi-year` previously returned traceback JSON (`KeyError: 'dates'`) before patch.
- `gridmet/single-year-monthly` previously returned traceback JSON before patch.

This pattern can break chart workflows if frontend state still submits stale pasture IDs from `Zumwalt4`.

## Fixes Applied

### 1) GridMET upstream TLS
File:
- `climate/gridmet/client.py`

Change:
- Switched Thredds endpoint from `http://thredds.northwestknowledge.net:8080/...` to `https://thredds.northwestknowledge.net/...`.

### 2) Include Zumwalt5 in current-year climate sync
File:
- `climate/gridmet/scripts/sync_current_year.py`

Change:
- Added `Zumwalt5` to the `locations` list for regular sync.

### 3) Harden missing GridMET data handling
File:
- `database/gridmet.py`

Changes:
- Initialize `dates`/`cum_pr` keys even when precipitation data is missing.
- Use safe dictionary access (`.get`) for water-year path logic.
- Make monthly aggregation robust when all series are missing, returning null monthly values instead of exceptions.

### 4) Guard empty pasture result in multi-year pasturestats
File:
- `api/app.py`

Change:
- In `multiyear_pasturestats`, return early with empty output when query returns no data, avoiding downstream GridMET calls for invalid pasture names.

## Post-Fix Verification Result
- Apache restarted.
- Previously failing legacy-pasture calls now return clean empty/null payloads (no traceback JSON).
- Valid endpoint matrix still passes.

## Verification Commands

### Legacy-pasture regression checks
```bash
TS=$(date +%s)

curl -sS "https://rangesat.org/api/pasturestats/multi-year/Zumwalt5/?ranch=The_Nature_Conservancy&pasture=A1&start_year=2016&end_year=2024&start_date=05-01&end_date=07-31&agg_func=mean&units=English&nocache=$TS"
# expected: []

curl -sS "https://rangesat.org/api/gridmet/single-year-monthly/Zumwalt5/The_Nature_Conservancy/A1/?year=2024&units=English&nocache=$TS"
# expected: JSON with monthly null arrays, no traceback
```

### Valid-path smoke checks
```bash
curl -sS "https://rangesat.org/api/geojson/Zumwalt5/The_Nature_Conservancy/" | head -c 200
curl -sS "https://rangesat.org/api/scenemeta/Zumwalt5/?pasture_coverage_threshold=0.5&filter=latest"
curl -sS "https://rangesat.org/api/ranchstats/single-year-monthly/Zumwalt5/?ranch=The_Nature_Conservancy&year=2024" | head -c 200
curl -sS "https://rangesat.org/api/pasturestats/multi-year/Zumwalt5/?ranch=The_Nature_Conservancy&pasture=A%20Control&start_year=2016&end_year=2024&start_date=05-01&end_date=07-31&agg_func=mean&units=English" | head -c 200
```

## Known Caveats
- Some endpoints are cached (`@cache.cached(..., query_string=True)`); append a cache-busting query parameter during verification.
- No-trailing-slash variants may return `308` redirects.
- `ranchstats/inter-year` is not available as a route; do not rely on it.

## Follow-Up Items
1. Frontend should ensure current `Zumwalt5` pasture IDs are sent (avoid legacy/stale values).
2. Keep `Zumwalt5` in all climate sync/maintenance scripts.
3. If chart behavior regresses, rerun the legacy + valid verification commands above first.

