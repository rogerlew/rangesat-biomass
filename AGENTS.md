# AGENTS.md

## Purpose
Operational guide for engineers and coding agents working in `rangesat-biomass`.

## Scope
- Backend API in `api/`
- Biomass/statistics processing in `database/` and `biomass/`
- Climate/GridMET ingestion in `climate/gridmet/`

## Current Zumwalt5 Status (March 5, 2026)
- API endpoint verification for `Zumwalt5` completed.
- Valid ranch/pasture requests return expected results across map/chart endpoint families.
- Legacy pasture names are now handled gracefully (empty/null responses instead of traceback payloads).

## Critical Files for Zumwalt5 Stability
- `api/app.py`
- `database/gridmet.py`
- `climate/gridmet/client.py`
- `climate/gridmet/scripts/sync_current_year.py`
- `docs/zumwalt5-troubleshooting.md`

## Operational Rules
- After editing API code, restart Apache/mod_wsgi before re-testing production routes.
- Use cache-busting query params (for example `&nocache=<timestamp>`) when testing cached endpoints.
- Treat `The_Nature_Conservancy` (request value) and `The Nature Conservancy` (display value) carefully; do not normalize these interchangeably in URLs.
- Preserve trailing slash behavior on API routes where applicable; no-slash forms may 308 redirect.

## Fast Verification (Minimum)
1. `geojson`:
   - `/api/geojson/Zumwalt5/The_Nature_Conservancy/`
2. `scenemeta`:
   - `/api/scenemeta/Zumwalt5/?pasture_coverage_threshold=0.5&filter=latest`
3. Single-year chart endpoints:
   - `/api/ranchstats/single-year-monthly/Zumwalt5/?ranch=The_Nature_Conservancy&year=2024`
   - `/api/gridmet/single-year-monthly/Zumwalt5/The_Nature_Conservancy/A%20Control/?year=2024&units=English`
4. Multi-year chart endpoints:
   - `/api/pasturestats/multi-year/Zumwalt5/?ranch=The_Nature_Conservancy&pasture=A%20Control&start_year=2016&end_year=2024&start_date=05-01&end_date=07-31&agg_func=mean&units=English`
5. Legacy pasture guardrail:
   - `/api/pasturestats/multi-year/Zumwalt5/?ranch=The_Nature_Conservancy&pasture=A1&start_year=2016&end_year=2024&start_date=05-01&end_date=07-31&agg_func=mean&units=English`
   - Expected: `[]` (not traceback JSON).

## If Charts Fail Again
1. Confirm frontend is not sending legacy pasture names from Zumwalt4.
2. Re-run endpoint checks in `docs/zumwalt5-troubleshooting.md`.
3. Re-sync current-year GridMET:
   - `python climate/gridmet/scripts/sync_current_year.py`
4. Restart Apache and retest with cache-busting params.

