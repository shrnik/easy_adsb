# easy_adsb

Stream and query ADS-B flight data from [adsb.lol](https://adsb.lol) GitHub releases — no intermediate files, no database required.

## Workflow

**Two-step (download then query):**

```bash
# 1. Download a day's data
python download.py --date 2026-02-05

# 2. Query it
python find_pings.py --lat 43.0755143 --lon -89.4154526 --date 2026-02-05 --out pings.csv
```

**One-step (download + query a date range):**

```bash
python pipeline.py \
    --start-date 2026-02-01 --end-date 2026-02-07 \
    --lat 43.0755143 --lon -89.4154526 \
    --out pings.csv
```

## download.py

Downloads split-tar ADS-B releases from the [adsblol/globe_history_*](https://github.com/adsblol) GitHub repos. Uses the GitHub API to discover assets for the given date, then downloads each part with a progress bar. Skips parts that are already fully downloaded.

Release tags follow the pattern `v{YYYY.MM.DD}-planes-readsb-{variant}`. The repo is auto-detected from the year (e.g. `adsblol/globe_history_2026`).

### Usage

```bash
# Download prod data for a date (saved to data/)
python download.py --date 2026-02-05

# Save to a custom directory
python download.py --date 2026-02-05 --out-dir data/2026-02-05

# Use a different variant
python download.py --date 2026-02-05 --variant staging-0

# Provide a GitHub token to avoid the 60 req/hr rate limit
python download.py --date 2026-02-05 --token ghp_xxxx
```

### Arguments

| Argument | Default | Description |
|---|---|---|
| `--date` | *(required)* | Date to download (`YYYY-MM-DD`) |
| `--variant` | `prod-0` | Release variant: `prod-0`, `staging-0`, `mlatonly-0` |
| `--repo` | auto | GitHub repo (auto-detected from year) |
| `--out-dir` | `data/` | Directory to save downloaded files |
| `--token` | none | GitHub personal access token |

---

## find_pings.py

Streams through split-tar releases and prints all ADS-B pings near a given lat/lon.

**Filtering:**
- Bounding box pre-filter (`--radius`, in degrees) for fast rejection
- Haversine distance filter (`--max-dist`, in km) for precise results
- Optional date filter to target a specific day's data

### Usage

```bash
# Basic search — pings within 100 km of a point
python find_pings.py --lat 43.0755143 --lon -89.4154526

# Filter to a specific date
python find_pings.py --lat 43.0755143 --lon -89.4154526 --date 2026-02-05

# Custom distance radius
python find_pings.py --lat 43.0755143 --lon -89.4154526 --max-dist 50

# Save results to CSV
python find_pings.py --lat 43.0755143 --lon -89.4154526 --out results.csv

# Limit output rows (useful for quick tests)
python find_pings.py --lat 43.0755143 --lon -89.4154526 --limit 100

# Include helicopters (excluded by default)
python find_pings.py --lat 43.0755143 --lon -89.4154526 --include-helicopters
```

### Arguments

| Argument | Default | Description |
|---|---|---|
| `--lat` | *(required)* | Center latitude |
| `--lon` | *(required)* | Center longitude |
| `--radius` | `0.5` | Bounding box half-width in degrees (pre-filter) |
| `--max-dist` | `100` | Max haversine distance in km |
| `--date` | all dates | Filter to a specific date (`YYYY-MM-DD`) |
| `--out` | stdout | Write results to a CSV file |
| `--limit` | unlimited | Stop after N matching rows |
| `--include-helicopters` | `False` | Include rotorcraft (category A7) |
| `--data-dir` | `data/` | Directory containing `.tar.??` part files |

### Output columns

`timestamp`, `icao`, `registration`, `flight`, `lat`, `lon`, `altitude_baro`, `alt_geom`, `ground_speed`, `track_degrees`, `vertical_rate`, `aircraft_type`, `description`, `operator`, `squawk`, `category`, `source_type`

---

## pipeline.py

Combines downloading and ping search into a single command. For each date in the range, it downloads any missing tar parts, then streams through all of them to find pings near the given location.

Files already present with the correct size are skipped automatically. If a release is not found on GitHub for a date, any locally present files for that date are used instead.

### Usage

```bash
# Download + search a date range
python pipeline.py \
    --start-date 2026-02-01 --end-date 2026-02-07 \
    --lat 43.0755143 --lon -89.4154526 \
    --out pings.csv

# Narrower radius, altitude filter, GitHub token
python pipeline.py \
    --start-date 2026-02-05 --end-date 2026-02-05 \
    --lat 33.4484 --lon -112.0740 --radius 0.5 --max-dist 100 \
    --min-alt 1000 --token ghp_xxxx --out arizona.csv

# Download only — skip the ping search
python pipeline.py \
    --start-date 2026-02-01 --end-date 2026-02-03 \
    --lat 0 --lon 0 --download-only
```

### Arguments

| Argument | Default | Description |
|---|---|---|
| `--start-date` | *(required)* | First date in range (`YYYY-MM-DD`) |
| `--end-date` | *(required)* | Last date in range, inclusive (`YYYY-MM-DD`) |
| `--lat` | *(required)* | Center latitude |
| `--lon` | *(required)* | Center longitude |
| `--radius` | `1` | Bounding box half-width in degrees (pre-filter) |
| `--max-dist` | `161` | Max haversine distance in km |
| `--min-alt` | none | Minimum `altitude_baro` in feet (excludes ground traffic) |
| `--out` | stdout | Write results to a CSV file |
| `--limit` | unlimited | Stop after N matching pings |
| `--data-dir` | `data/` | Directory for downloaded tar files |
| `--variant` | `prod-0` | Release variant: `prod-0`, `staging-0`, `mlatonly-0` |
| `--repo` | auto | GitHub repo override (auto-detected from year) |
| `--token` | none | GitHub personal access token (avoids 60 req/hr rate limit) |
| `--download-only` | `False` | Download files only, skip ping search |
