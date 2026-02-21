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
import subprocess
import sys
import tarfile
from datetime import datetime, timezone
from pathlib import Path

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


def stream_pings(parts, lat_min, lat_max, lon_min, lon_max, center_lat, center_lon, max_dist_km, min_alt_ft=None):
    """Yield one dict per matching trace point, streaming through the tar."""
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

                ac = pt[8] if len(pt) > 8 and isinstance(pt[8], dict) else {}

                yield {
                    "timestamp":     ts.isoformat(),
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
    parser.add_argument("--data-dir", type=Path, default=DATA_DIR)
    args = parser.parse_args()
    if args.date:
        try:
            date_obj = datetime.strptime(args.date, "%Y-%m-%d")
        except ValueError:
            sys.exit(f"Invalid --date '{args.date}': expected YYYY-MM-DD")
        date_glob = f"v{date_obj.strftime('%Y.%m.%d')}-*.tar.??"
        parts = sorted(args.data_dir.glob(date_glob))
        if not parts:
            sys.exit(f"No parts found for date {args.date} (pattern: {date_glob}) in {args.data_dir}")
    else:
        parts = sorted(args.data_dir.glob("*.tar.??"))
        # print file names
        for part in parts:
            print(f"Found part: {part.name}")
        if not parts:
            sys.exit(f"No *.tar.?? files found in {args.data_dir}")

    lat_min = args.lat - args.radius
    lat_max = args.lat + args.radius
    lon_min = args.lon - args.radius
    lon_max = args.lon + args.radius

    date_label = f" [{args.date}]" if args.date else ""
    print(f"Searching {len(parts)} tar part(s){date_label} for pings in:")
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
    try:
        for row in stream_pings(parts, lat_min, lat_max, lon_min, lon_max, args.lat, args.lon, args.max_dist, args.min_alt):
            if writer:
                writer.writerow({k: row.get(k) for k in COLUMNS})
            count += 1
            if args.limit and count >= args.limit:
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
