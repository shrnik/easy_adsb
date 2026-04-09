"""
find_pings.py — Stream adsb.lol split-tar releases and print ADS-B pings
                near a given lat/lon without any intermediate files.

Usage:
    python find_pings.py --lat 43.0755143 --lon -89.4154526 --radius 0.5

    # Specific date (files named v2026.02.05-planes-readsb-prod-0.tar.aa …):
    python find_pings.py --lat 43.0755143 --lon -89.4154526 --radius 0.5 --date 2026-02-05

    # Wider radius, save to CSV:
    python find_pings.py --lat 43.0755143 --lon -89.4154526 --radius 1.0 --out out.csv

    # Limit to first N matching rows (handy for quick tests):
    python find_pings.py --lat 43.0755143 --lon -89.4154526 --radius 0.5 --limit 100
"""

import argparse
import csv
import gzip
import json
import math
import re
import subprocess
import sys
import tarfile
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

DATA_DIR = Path("data")

# ADS-B category A7 = rotorcraft; also catch common helicopter ICAO type codes
# that may not broadcast the correct category.
HELI_CATEGORY = {"A7"}
HELI_TYPES = {
    "EC45", "H145",                    # Airbus H145
    "EC35", "H135",                    # Airbus H135
    "EC30", "EC35", "EC45", "EC55",    # Airbus light/medium family
    "B06",  "B407", "B429",            # Bell
    "R22",  "R44",  "R66",             # Robinson
    "S76",  "S92",                     # Sikorsky
    "AS50", "AS55", "AS65",            # Airbus light
    "MD52", "MD60",                    # MD Helicopters
    "AW09", "AW19", "AW13", "AW16",   # Leonardo AW
    "H160", "H175",                    # Airbus heavy
}

COLUMNS = [
    "timestamp", "icao", "registration", "flight",
    "lat", "lon", "altitude_baro", "alt_geom", "ground_speed", "track_degrees",
    "vertical_rate", "aircraft_type", "description", "operator",
    "squawk", "category", "source_type",
]


def haversine_km(lat1, lon1, lat2, lon2):
    """Great-circle distance in km between two lat/lon points."""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    return R * 2 * math.asin(math.sqrt(a))


def _f(val):
    try:
        return float(val) if val is not None else None
    except (TypeError, ValueError):
        return None


def _s(val):
    return str(val).strip() if val is not None else None


def stream_pings(parts, lat_min, lat_max, lon_min, lon_max, center_lat, center_lon, max_dist_km, min_alt_ft=None, utc_start=None, utc_end=None, display_tz=None):
    """Yield one dict per matching trace point, streaming through the tar.

    utc_start/utc_end: if set, only yield pings whose UTC timestamp falls in [start, end).
    display_tz: if set, output timestamps converted to this timezone.
    """
    cat = subprocess.Popen(
        ["cat"] + [str(p) for p in parts],
        stdout=subprocess.PIPE,
    )
    tf = tarfile.open(fileobj=cat.stdout, mode="r|")

    try:
        for member in tf:
            if not member.name.startswith("./traces/") or not member.name.endswith(".json"):
                continue

            f = tf.extractfile(member)
            if f is None:
                continue

            raw = f.read()
            try:
                data = json.loads(gzip.decompress(raw))
            except Exception:
                try:
                    data = json.loads(raw)
                except Exception:
                    continue

            base_ts  = data.get("timestamp", 0)
            icao     = _s(data.get("icao", "").lower())
            reg      = _s(data.get("r"))
            atype    = _s(data.get("t"))
            desc     = _s(data.get("desc"))
            operator = _s(data.get("ownOp"))

            for pt in data.get("trace", []):
                if not isinstance(pt, list) or len(pt) < 3:
                    continue

                lat = _f(pt[1])
                lon = _f(pt[2])
                if lat is None or lon is None:
                    continue
                if not (lat_min <= lat <= lat_max and lon_min <= lon <= lon_max):
                    continue
                if haversine_km(center_lat, center_lon, lat, lon) > max_dist_km:
                    continue

                alt_baro = _f(pt[3]) if len(pt) > 3 else None
                if min_alt_ft is not None and (alt_baro is None or alt_baro < min_alt_ft):
                    continue

                offset = _f(pt[0])
                if offset is None:
                    continue
                ts = datetime.fromtimestamp(int(base_ts + offset), tz=timezone.utc)

                if utc_start is not None and ts < utc_start:
                    continue
                if utc_end is not None and ts >= utc_end:
                    continue

                display_ts = ts.astimezone(display_tz) if display_tz else ts
                ac = pt[8] if len(pt) > 8 and isinstance(pt[8], dict) else {}

                yield {
                    "timestamp":     display_ts.isoformat(),
                    "icao":          icao,
                    "registration":  reg,
                    "flight":        _s(ac.get("flight")) or None,
                    "lat":           lat,
                    "lon":           lon,
                    "altitude_baro": alt_baro,
                    "alt_geom":      _f(pt[10]) if len(pt) > 10 else None,
                    "ground_speed":  _f(pt[4]) if len(pt) > 4 else None,
                    "track_degrees": _f(pt[5]) if len(pt) > 5 else None,
                    "vertical_rate": _f(pt[7]) if len(pt) > 7 else None,
                    "aircraft_type": atype,
                    "description":   desc,
                    "operator":      operator,
                    "squawk":        _s(ac.get("squawk")),
                    "category":      _s(ac.get("category")),
                    "source_type":   _s(pt[9]) if len(pt) > 9 else None,
                }
    finally:
        tf.close()
        cat.stdout.close()
        cat.wait()


def group_parts_by_archive(parts):
    """Group tar split parts by their archive prefix.

    Files like v2026.02.05-planes-readsb-prod-0.tar.aa and .tar.ab share the
    prefix "v2026.02.05-planes-readsb-prod-0.tar" and belong to one archive.
    Each group must be cat-ed together independently.
    """
    groups = defaultdict(list)
    for p in parts:
        # Strip the two-letter split suffix (.aa, .ab, …) to get the archive key
        key = re.sub(r'\.[a-z]{2}$', '', p.name)
        groups[key].append(p)
    # Return groups sorted by key so dates are processed in order
    return [sorted(groups[k]) for k in sorted(groups)]


def main():
    parser = argparse.ArgumentParser(description="Find ADS-B pings near a lat/lon")
    parser.add_argument("--lat",    type=float, required=True,  help="Center latitude")
    parser.add_argument("--lon",    type=float, required=True,  help="Center longitude")
    parser.add_argument("--radius", type=float, default=1,    help="±degrees (default 0.5)")
    parser.add_argument("--limit",  type=int,   default=0,      help="Stop after N rows (0=all)")
    parser.add_argument("--out",    type=str,   default=None,   help="Write CSV to file")
    parser.add_argument("--max-dist", type=float, default=161.0, help="Max haversine distance in km (default 100)")
    parser.add_argument("--min-alt",  type=float, default=None,  help="Minimum altitude_baro in feet (exclude lower/ground)")
    parser.add_argument("--date",     type=str,   default=None,   help="Filter to a specific date YYYY-MM-DD (e.g. 2026-02-05)")
    parser.add_argument("--tz",       type=str,   default=None,   help="Timezone for --date and output (e.g. America/Chicago, US/Eastern)")
    parser.add_argument("--start-time", type=str, default=None,   help="Start time HH:MM within --date (requires --tz and --date)")
    parser.add_argument("--end-time",   type=str, default=None,   help="End time HH:MM within --date (requires --tz and --date)")
    parser.add_argument("--data-dir", type=Path, default=DATA_DIR)
    args = parser.parse_args()

    # Resolve timezone
    display_tz = None
    if args.tz:
        try:
            display_tz = ZoneInfo(args.tz)
        except KeyError:
            sys.exit(f"Unknown timezone '{args.tz}'. Use IANA names like America/Chicago, US/Eastern, Europe/London.")

    if (args.start_time or args.end_time) and not (args.date and args.tz):
        sys.exit("--start-time / --end-time require both --date and --tz")

    # Compute UTC time window and determine which date(s) of tar files to load
    utc_start = None
    utc_end = None

    if args.date:
        try:
            date_obj = datetime.strptime(args.date, "%Y-%m-%d")
        except ValueError:
            sys.exit(f"Invalid --date '{args.date}': expected YYYY-MM-DD")

        if display_tz:
            # Build local start/end in the given timezone
            local_date = date_obj.date()
            if args.start_time:
                try:
                    st = datetime.strptime(args.start_time, "%H:%M").time()
                except ValueError:
                    sys.exit(f"Invalid --start-time '{args.start_time}': expected HH:MM")
            else:
                st = datetime.min.time()  # 00:00

            if args.end_time:
                try:
                    et = datetime.strptime(args.end_time, "%H:%M").time()
                except ValueError:
                    sys.exit(f"Invalid --end-time '{args.end_time}': expected HH:MM")
            else:
                et = datetime.min.time()  # next day 00:00

            utc_start = datetime.combine(local_date, st, tzinfo=display_tz).astimezone(timezone.utc)
            if args.end_time:
                utc_end = datetime.combine(local_date, et, tzinfo=display_tz).astimezone(timezone.utc)
            else:
                utc_end = datetime.combine(local_date + timedelta(days=1), et, tzinfo=display_tz).astimezone(timezone.utc)

            # Collect tar files for all UTC dates that overlap the window
            parts = []
            d = utc_start.date()
            while d <= utc_end.date():
                dg = f"v{d.strftime('%Y.%m.%d')}-*.tar.??"
                parts.extend(args.data_dir.glob(dg))
                d += timedelta(days=1)
            parts = sorted(set(parts))
            if not parts:
                sys.exit(f"No tar files found for UTC dates {utc_start.date()} – {utc_end.date()} in {args.data_dir}")
        else:
            # No timezone — just load the literal date
            date_glob = f"v{date_obj.strftime('%Y.%m.%d')}-*.tar.??"
            parts = sorted(args.data_dir.glob(date_glob))
            if not parts:
                sys.exit(f"No parts found for date {args.date} (pattern: {date_glob}) in {args.data_dir}")
    else:
        parts = sorted(args.data_dir.glob("*.tar.??"))
        if not parts:
            sys.exit(f"No *.tar.?? files found in {args.data_dir}")

    archive_groups = group_parts_by_archive(parts)
    total_parts = sum(len(g) for g in archive_groups)

    lat_min = args.lat - args.radius
    lat_max = args.lat + args.radius
    lon_min = args.lon - args.radius
    lon_max = args.lon + args.radius

    date_label = f" [{args.date}]" if args.date else ""
    tz_label = f" tz={args.tz}" if args.tz else ""
    print(f"Searching {total_parts} tar part(s) across {len(archive_groups)} archive(s){date_label}{tz_label} for pings in:")
    if utc_start and utc_end:
        print(f"  UTC window: {utc_start.isoformat()} to {utc_end.isoformat()}")
    alt_str = f"  min alt {args.min_alt:.0f} ft" if args.min_alt is not None else ""
    print(f"  lat [{lat_min:.4f}, {lat_max:.4f}]  lon [{lon_min:.4f}, {lon_max:.4f}]  max dist {args.max_dist:.0f} km{alt_str}")
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
    done = False
    try:
        for i, group in enumerate(archive_groups, 1):
            print(f"Processing archive {i}/{len(archive_groups)}: {group[0].name} … ({len(group)} part(s))")
            for row in stream_pings(group, lat_min, lat_max, lon_min, lon_max, args.lat, args.lon, args.max_dist, args.min_alt, utc_start, utc_end, display_tz):
                if writer:
                    writer.writerow({k: row.get(k) for k in COLUMNS})
                count += 1
                if args.limit and count >= args.limit:
                    done = True
                    break
            if done:
                break
    except KeyboardInterrupt:
        print("\n[interrupted]")

    if out_file:
        out_file.close()

    print(f"Total pings found: {count:,}")
    if args.out:
        print(f"Saved → {args.out}")


if __name__ == "__main__":
    main()
