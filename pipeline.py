"""
pipeline.py — Download missing ADS-B data for a date range, then find pings
              near a given lat/lon.

Usage:
    python pipeline.py \\
        --start-date 2026-02-01 --end-date 2026-02-07 \\
        --lat 43.0755143 --lon -89.4154526 \\
        --out pings.csv

    # With optional filters:
    python pipeline.py \\
        --start-date 2026-02-05 --end-date 2026-02-05 \\
        --lat 33.4484 --lon -112.0740 --radius 0.5 --max-dist 100 \\
        --min-alt 1000 --out arizona.csv

    # Provide a GitHub token to avoid the 60 req/hr rate limit:
    python pipeline.py --start-date 2026-02-01 --end-date 2026-02-03 \\
        --lat 43.0755 --lon -89.415 --token ghp_xxxx --out out.csv

    # Only download, skip ping search:
    python pipeline.py --start-date 2026-02-01 --end-date 2026-02-03 \\
        --lat 0 --lon 0 --download-only

Download behaviour:
    - Files are saved to --data-dir (default: data/).
    - A file is skipped if it already exists with the correct size.
    - Dates whose release is not found on GitHub are skipped with a warning.
"""

import argparse
import csv
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

from download import download_file, find_release_assets, repo_for_date
from find_pings import COLUMNS, stream_pings

DATA_DIR = Path("data")


def date_range(start: str, end: str):
    """Yield YYYY-MM-DD strings from start to end inclusive."""
    cur = datetime.strptime(start, "%Y-%m-%d").date()
    stop = datetime.strptime(end, "%Y-%m-%d").date()
    while cur <= stop:
        yield cur.strftime("%Y-%m-%d")
        cur += timedelta(days=1)


def download_date(date_str: str, out_dir: Path, variant: str,
                  repo: str | None, token: str | None) -> list[Path]:
    """Download all tar parts for one date. Returns local paths of all parts."""
    dot_date = date_str.replace("-", ".")
    used_repo = repo or repo_for_date(date_str)
    assets = find_release_assets(used_repo, dot_date, variant, token)
    if not assets:
        # Fall back to whatever is already on disk for this date
        existing = sorted(out_dir.glob(f"v{dot_date}-*.tar.??"))
        if existing:
            print(f"  Using {len(existing)} local file(s) (release unavailable).")
        return existing

    parts = []
    for asset in assets:
        dest = out_dir / asset["name"]
        if dest.exists() and dest.stat().st_size == asset["size"]:
            print(f"  {asset['name']}  already present, skipping.")
        else:
            print(f"  Downloading {asset['name']} ({asset['size'] / 1e6:.0f} MB) ...")
            t0 = time.perf_counter()
            download_file(asset["browser_download_url"], dest, token)
            elapsed = time.perf_counter() - t0
            mb = asset["size"] / 1_000_000
            print(f"  -> saved {dest.name}  ({mb:.0f} MB in {elapsed:.0f}s, {mb/elapsed:.1f} MB/s)")
        parts.append(dest)
    return parts


def main():
    parser = argparse.ArgumentParser(
        description="Download ADS-B data for a date range and find pings near a location."
    )

    # Date range
    parser.add_argument("--start-date", required=True, help="First date (YYYY-MM-DD)")
    parser.add_argument("--end-date",   required=True, help="Last date inclusive (YYYY-MM-DD)")

    # Location
    parser.add_argument("--lat",      type=float, required=True, help="Center latitude")
    parser.add_argument("--lon",      type=float, required=True, help="Center longitude")
    parser.add_argument("--radius",   type=float, default=1,
                        help="Bounding-box half-size in degrees (default 0.5)")
    parser.add_argument("--max-dist", type=float, default=161.0,
                        help="Max haversine distance in km (default 161)")
    parser.add_argument("--min-alt",  type=float, default=None,
                        help="Minimum altitude_baro in feet")

    # Output
    parser.add_argument("--out",   type=str, default=None, help="Save pings to CSV")
    parser.add_argument("--limit", type=int, default=0,    help="Stop after N pings (0=all)")

    # Download config
    parser.add_argument("--data-dir", type=Path, default=DATA_DIR,
                        help="Directory for tar files (default: data/)")
    parser.add_argument("--variant",  default="prod-0",
                        help="Release variant: prod-0 (default), staging-0, mlatonly-0")
    parser.add_argument("--repo",     default=None,
                        help="GitHub repo override (auto-detected from year if omitted)")
    parser.add_argument("--token",    default=None,
                        help="GitHub personal access token (avoids 60 req/hr rate limit)")
    parser.add_argument("--download-only", action="store_true",
                        help="Only download files, skip ping search")

    args = parser.parse_args()

    # Validate dates
    for label, val in [("--start-date", args.start_date), ("--end-date", args.end_date)]:
        try:
            datetime.strptime(val, "%Y-%m-%d")
        except ValueError:
            sys.exit(f"Invalid {label} '{val}': expected YYYY-MM-DD")
    if args.start_date > args.end_date:
        sys.exit("--start-date must be <= --end-date")

    dates = list(date_range(args.start_date, args.end_date))
    args.data_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Step 1: Download missing files
    # ------------------------------------------------------------------
    print(f"=== Download: {args.start_date} -> {args.end_date} ({len(dates)} day(s)) ===\n")
    all_parts: list[Path] = []

    for d in dates:
        print(f"[{d}]")
        parts = download_date(d, args.data_dir, args.variant, args.repo, args.token)
        all_parts.extend(parts)
        print()

    all_parts = sorted(set(all_parts))
    print(f"Total tar parts: {len(all_parts)}")
    for p in all_parts:
        print(f"  {p.name}")

    if args.download_only:
        print("\n--download-only set; skipping ping search.")
        return

    if not all_parts:
        sys.exit("\nNo tar parts found — nothing to search.")

    # ------------------------------------------------------------------
    # Step 2: Find pings
    # ------------------------------------------------------------------
    print(f"\n=== Ping search: lat={args.lat}, lon={args.lon}, "
          f"radius={args.radius}°, max-dist={args.max_dist} km ===")

    lat_min = args.lat - args.radius
    lat_max = args.lat + args.radius
    lon_min = args.lon - args.radius
    lon_max = args.lon + args.radius
    alt_str = f"  min alt {args.min_alt:.0f} ft" if args.min_alt is not None else ""
    print(f"  lat [{lat_min:.4f}, {lat_max:.4f}]  lon [{lon_min:.4f}, {lon_max:.4f}]{alt_str}")
    if args.limit:
        print(f"  (stopping after {args.limit} matches)")
    print()

    out_file = None
    writer   = None
    if args.out:
        out_file = open(args.out, "w", newline="")
        writer   = csv.DictWriter(out_file, fieldnames=COLUMNS)
        writer.writeheader()

    count = 0
    try:
        for row in stream_pings(
            all_parts, lat_min, lat_max, lon_min, lon_max,
            args.lat, args.lon, args.max_dist, args.min_alt,
        ):
            if writer:
                writer.writerow({k: row.get(k) for k in COLUMNS})
            count += 1
            if count % 1_000 == 0:
                print(f"  {count:,} pings found so far...", end="\r", flush=True)
            if args.limit and count >= args.limit:
                break
    except KeyboardInterrupt:
        print("\n[interrupted]")

    if out_file:
        out_file.close()

    print(f"\nTotal pings found: {count:,}")
    if args.out:
        print(f"Saved -> {args.out}")


if __name__ == "__main__":
    main()
