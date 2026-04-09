#!/usr/bin/env python3
"""
Verify a pings CSV against a ground-truth CSV.

Usage:
  python verify_pings.py pings_file gt_file day
  python verify_pings.py   (defaults to Madison Oct 1 2025)

For each matched aircraft, finds the nearest ping by timestamp for each
ground-truth observation and reports differences in lat, lon, alt_baro,
and alt_geom.
"""

import csv
import sys
from datetime import datetime
from collections import defaultdict

if len(sys.argv) == 4:
    PINGS_FILE = sys.argv[1]
    GT_FILE    = sys.argv[2]
    _DAY_ARG   = sys.argv[3]
elif len(sys.argv) == 1:
    PINGS_FILE = "data/madison_pings_2025_10_01.csv"
    GT_FILE    = "data/madison_ground_truth_2025-10-01.csv"
    _DAY_ARG   = "2025-10-01"
else:
    sys.exit("Usage: python verify_pings.py [pings_file gt_file day]")

# Nearest-ping window: skip comparisons where closest ping is farther than this
MAX_TIME_GAP_S = 30   # seconds

# Thresholds — a row is flagged (*) if any |delta| exceeds these
THRESH_LAT_DEG  = 0.02   # ~2 km
THRESH_LON_DEG  = 0.02   # ~2 km
THRESH_ALT_FT   = 500    # feet baro
THRESH_GEOM_FT  = 500    # feet geometric


def parse_dt(s: str) -> datetime:
    s = s.strip().replace(" ", "T")
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return datetime.fromisoformat(s)


def load_ground_truth(path: str, day: str):
    records: dict[str, list[dict]] = defaultdict(list)
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            dt = parse_dt(row["time"])
            if dt.strftime("%Y-%m-%d") != day:
                continue
            row["_dt"] = dt
            records[row["transponder_id"].upper()].append(row)
    if not records:
        sys.exit(f"No ground-truth rows found for {day}")
    all_times = [r["_dt"] for rows in records.values() for r in rows]
    return min(all_times), max(all_times), records


def load_pings(path: str, t_min: datetime, t_max: datetime):
    records: dict[str, list[dict]] = defaultdict(list)
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            dt = parse_dt(row["timestamp"])
            if t_min <= dt <= t_max:
                row["_dt"] = dt
                records[row["icao"].upper()].append(row)
    return records


def nearest_ping(ping_rows: list[dict], gt_dt: datetime) -> dict | None:
    """Return the ping closest in time to gt_dt, or None if it exceeds MAX_TIME_GAP_S."""
    best = min(ping_rows, key=lambda r: abs((r["_dt"] - gt_dt).total_seconds()))
    if abs((best["_dt"] - gt_dt).total_seconds()) > MAX_TIME_GAP_S:
        return None
    return best


def _fval(row: dict, *keys) -> float | None:
    for k in keys:
        v = row.get(k, "").strip()
        if v:
            try:
                return float(v)
            except ValueError:
                pass
    return None


def compare_aircraft(gt_rows: list[dict], ping_rows: list[dict]) -> list[dict]:
    """
    For each GT observation find the nearest ping within MAX_TIME_GAP_S and compute deltas.
    Returns one dict per GT observation (dlat/dlon/dalt/dalt_geo are None if no close ping).
    """
    ping_sorted = sorted(ping_rows, key=lambda r: r["_dt"])
    results = []
    for gt in sorted(gt_rows, key=lambda r: r["_dt"]):
        near = nearest_ping(ping_sorted, gt["_dt"])

        if near is None:
            results.append({
                "gt_time": gt["_dt"].isoformat(), "ping_time": None,
                "gap_s": None, "dlat": None, "dlon": None,
                "dalt": None, "dalt_geo": None,
            })
            continue

        gap = (near["_dt"] - gt["_dt"]).total_seconds()

        gt_lat  = _fval(gt,   "lat");        p_lat  = _fval(near, "lat")
        gt_lon  = _fval(gt,   "lon");        p_lon  = _fval(near, "lon")
        gt_alt  = _fval(gt,   "alt");        p_alt  = _fval(near, "altitude_baro")
        gt_geom = _fval(gt,   "alt_gnss");   p_geom = _fval(near, "alt_geom")

        results.append({
            "gt_time":   gt["_dt"].isoformat(),
            "ping_time": near["_dt"].isoformat(),
            "gap_s":     gap,
            "dlat":     (p_lat  - gt_lat)  if (gt_lat  is not None and p_lat  is not None) else None,
            "dlon":     (p_lon  - gt_lon)  if (gt_lon  is not None and p_lon  is not None) else None,
            "dalt":     (p_alt  - gt_alt)  if (gt_alt  is not None and p_alt  is not None) else None,
            "dalt_geo": (p_geom - gt_geom) if (gt_geom is not None and p_geom is not None) else None,
        })
    return results


def _stats(vals):
    vals = [v for v in vals if v is not None]
    if not vals:
        return None, None, None
    return sum(vals) / len(vals), min(vals), max(vals)


def main():
    day = _DAY_ARG
    print(f"Loading ground truth  : {GT_FILE}")
    t_min, t_max, gt = load_ground_truth(GT_FILE, day)
    print(f"  window   : {t_min.isoformat()} → {t_max.isoformat()}")
    print(f"  aircraft : {len(gt)}")

    print(f"\nLoading pings         : {PINGS_FILE}")
    pings = load_pings(PINGS_FILE, t_min, t_max)
    print(f"  aircraft in window : {len(pings)}")

    gt_icaos   = set(gt)
    ping_icaos = set(pings)
    missing    = gt_icaos - ping_icaos
    extra      = ping_icaos - gt_icaos
    matched    = gt_icaos & ping_icaos

    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"  GT aircraft      : {len(gt_icaos)}")
    print(f"  Pings aircraft   : {len(ping_icaos)}")
    print(f"  Matched          : {len(matched)}")
    print(f"  Missing in pings : {len(missing)}")
    print(f"  Extra in pings   : {len(extra)}")

    if missing:
        print(f"\n{'─'*80}")
        print(f"MISSING ({len(missing)}) — in ground truth, not in pings:")
        for icao in sorted(missing):
            rows   = gt[icao]
            idents = sorted({r["ident"] for r in rows if r.get("ident")})
            t0 = min(r["_dt"] for r in rows).isoformat()
            t1 = max(r["_dt"] for r in rows).isoformat()
            print(f"  {icao:8s}  ident={idents}  obs={len(rows)}  {t0} → {t1}")

    if extra:
        print(f"\n{'─'*80}")
        print(f"EXTRA ({len(extra)}) — in pings, not in ground truth:")
        for icao in sorted(extra):
            rows    = pings[icao]
            flights = sorted({r["flight"] for r in rows if r.get("flight")})
            print(f"  {icao:8s}  flight={flights}  pings={len(rows)}")

    # -------------------------------------------------------------------------
    print(f"\n{'─'*80}")
    print(f"PER-AIRCRAFT DELTAS  (ping − ground_truth, nearest ping within {MAX_TIME_GAP_S}s)")
    print(f"Thresholds:  lat ±{THRESH_LAT_DEG}°  lon ±{THRESH_LON_DEG}°  "
          f"alt ±{THRESH_ALT_FT}ft  alt_geom ±{THRESH_GEOM_FT}ft   (* = threshold exceeded)")
    print(f"{'─'*80}")
    hdr = (f"  {'ICAO':<8}  {'ident':<10}  {'obs':>4}  {'cmp':>4}  "
           f"{'gap_mean':>9}  {'gap_max':>8}  "
           f"{'dlat_mean':>10}  {'dlat_max':>9}  "
           f"{'dlon_mean':>10}  {'dlon_max':>9}  "
           f"{'dalt_mean':>10}  {'dalt_max':>9}  "
           f"{'dgeo_mean':>10}  {'dgeo_max':>9}  {'flags'}")
    print(hdr)
    print(f"  {'':─<8}  {'':─<10}  {'':─>4}  {'':─>4}  "
          f"{'(s)':>9}  {'(s)':>8}  "
          f"{'(deg)':>10}  {'(deg)':>9}  "
          f"{'(deg)':>10}  {'(deg)':>9}  "
          f"{'(ft)':>10}  {'(ft)':>9}  "
          f"{'(ft)':>10}  {'(ft)':>9}")

    def fmt_f(v, w, d): return f"{v:+{w}.{d}f}" if v is not None else f"{'—':>{w}}"

    # CSV output path derived from pings filename
    csv_out_path = PINGS_FILE.replace(".csv", "_verify.csv")
    CSV_FIELDS = ["icao", "ident", "gt_time", "ping_time", "gap_s",
                  "dlat", "dlon", "dalt", "dalt_geo", "flags"]
    csv_out = open(csv_out_path, "w", newline="")
    csv_writer = csv.DictWriter(csv_out, fieldnames=CSV_FIELDS)
    csv_writer.writeheader()

    flagged = 0
    for icao in sorted(matched):
        rows   = compare_aircraft(gt[icao], pings[icao])
        ident  = next((r["ident"] for r in gt[icao] if r.get("ident")), "")
        n_obs  = len(rows)
        n_comp = sum(1 for r in rows if r["dlat"] is not None)

        for r in rows:
            row_flags = []
            if r["dlat"]     is not None and abs(r["dlat"])     > THRESH_LAT_DEG:  row_flags.append("LAT")
            if r["dlon"]     is not None and abs(r["dlon"])     > THRESH_LON_DEG:  row_flags.append("LON")
            if r["dalt"]     is not None and abs(r["dalt"])     > THRESH_ALT_FT:   row_flags.append("ALT")
            if r["dalt_geo"] is not None and abs(r["dalt_geo"]) > THRESH_GEOM_FT:  row_flags.append("GEO")
            csv_writer.writerow({
                "icao":      icao,
                "ident":     ident,
                "gt_time":   r["gt_time"],
                "ping_time": r["ping_time"] or "",
                "gap_s":     "" if r["gap_s"] is None else f"{r['gap_s']:.1f}",
                "dlat":      "" if r["dlat"]     is None else f"{r['dlat']:.6f}",
                "dlon":      "" if r["dlon"]     is None else f"{r['dlon']:.6f}",
                "dalt":      "" if r["dalt"]     is None else f"{r['dalt']:.1f}",
                "dalt_geo":  "" if r["dalt_geo"] is None else f"{r['dalt_geo']:.1f}",
                "flags":     ",".join(row_flags),
            })

        gap_m, _, gap_hi = _stats([r["gap_s"]    for r in rows])
        lat_m, _, lat_hi = _stats([r["dlat"]     for r in rows])
        lon_m, _, lon_hi = _stats([r["dlon"]     for r in rows])
        alt_m, _, alt_hi = _stats([r["dalt"]     for r in rows])
        geo_m, _, geo_hi = _stats([r["dalt_geo"] for r in rows])

        flags = []
        if lat_hi is not None and abs(lat_hi) > THRESH_LAT_DEG:  flags.append("LAT")
        if lon_hi is not None and abs(lon_hi) > THRESH_LON_DEG:  flags.append("LON")
        if alt_hi is not None and abs(alt_hi) > THRESH_ALT_FT:   flags.append("ALT")
        if geo_hi is not None and abs(geo_hi) > THRESH_GEOM_FT:  flags.append("GEO")
        flag_str = "*" + ",".join(flags) if flags else ""
        if flags:
            flagged += 1

        print(f"  {icao:<8}  {ident:<10}  {n_obs:>4}  {n_comp:>4}  "
              f"{fmt_f(gap_m,9,1)}  {fmt_f(gap_hi,8,1)}  "
              f"{fmt_f(lat_m,10,5)}  {fmt_f(lat_hi,9,5)}  "
              f"{fmt_f(lon_m,10,5)}  {fmt_f(lon_hi,9,5)}  "
              f"{fmt_f(alt_m,10,0)}  {fmt_f(alt_hi,9,0)}  "
              f"{fmt_f(geo_m,10,0)}  {fmt_f(geo_hi,9,0)}  {flag_str}")

    csv_out.close()
    print(f"\n  {flagged}/{len(matched)} aircraft exceeded at least one threshold.")
    print(f"\n  Per-observation deltas written to: {csv_out_path}")

    print("\n" + "=" * 80)
    print("DONE")


if __name__ == "__main__":
    main()
