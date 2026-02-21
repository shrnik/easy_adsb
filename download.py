"""
download.py — Download adsb.lol globe_history release for a given date.

Usage:
    python download.py --date 2024-12-30
    python download.py --date 2024-12-30 --out-dir data/2024-12-30
    python download.py --date 2024-12-30 --variant staging-0
    python download.py --date 2024-12-30 --token ghp_xxxx   # avoid rate limits

The script uses the GitHub API to discover available assets for the date,
then downloads each split-tar part with a progress bar.

Repos by year:
    2024 → adsblol/globe_history_2024
    2025 → adsblol/globe_history_2025
    (auto-detected from date; override with --repo)
"""

import argparse
import sys
import urllib.request
import urllib.error
import json
import time
from pathlib import Path
from datetime import datetime


# ---------------------------------------------------------------------------
# GitHub API helpers
# ---------------------------------------------------------------------------

def gh_get(url: str, token: str | None) -> dict | list:
    req = urllib.request.Request(url)
    req.add_header("Accept", "application/vnd.github+json")
    req.add_header("X-GitHub-Api-Version", "2022-11-28")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    with urllib.request.urlopen(req) as resp:
        return json.load(resp)


def find_release_assets(repo: str, date: str, variant: str, token: str | None) -> list[dict]:
    """
    Return the list of tar asset dicts for the release matching
    v{date}-planes-readsb-{variant} in the given repo.
    Returns an empty list if the release is not found or has no assets.
    """
    tag = f"v{date}-planes-readsb-{variant}"
    url = f"https://api.github.com/repos/{repo}/releases/tags/{tag}"
    print(f"Looking up release: {tag} in {repo} ...")
    try:
        release = gh_get(url, token)
    except urllib.error.HTTPError as e:
        if e.code == 404:
            print(f"  Release not found: {tag}")
            return []
        if e.code == 403:
            print(f"  Rate-limited (403) for {tag}. Use --token to increase limits.")
            return []
        raise
    return [a for a in release.get("assets", []) if ".tar" in a["name"]]


# ---------------------------------------------------------------------------
# Download with progress
# ---------------------------------------------------------------------------

def download_file(url: str, dest: Path, token: str | None) -> None:
    req = urllib.request.Request(url)
    # GitHub release downloads redirect to S3; don't send auth header there
    if token and "api.github.com" in url:
        req.add_header("Authorization", f"Bearer {token}")

    tmp = dest.with_suffix(dest.suffix + ".part")
    try:
        with urllib.request.urlopen(req) as resp:
            total = int(resp.headers.get("Content-Length", 0))
            downloaded = 0
            chunk = 1024 * 1024  # 1 MB

            with open(tmp, "wb") as f:
                while True:
                    buf = resp.read(chunk)
                    if not buf:
                        break
                    f.write(buf)
                    downloaded += len(buf)
                    if total:
                        pct = downloaded / total * 100
                        mb_done = downloaded / 1_000_000
                        mb_total = total / 1_000_000
                        print(
                            f"\r  {dest.name}  {mb_done:.0f}/{mb_total:.0f} MB  ({pct:.1f}%)",
                            end="", flush=True,
                        )
                    else:
                        mb_done = downloaded / 1_000_000
                        print(f"\r  {dest.name}  {mb_done:.0f} MB", end="", flush=True)
        print()  # newline after progress
        tmp.rename(dest)
    except Exception:
        tmp.unlink(missing_ok=True)
        raise


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def repo_for_date(date_str: str) -> str:
    year = date_str.split("-")[0]
    return f"adsblol/globe_history_{year}"


def main():
    parser = argparse.ArgumentParser(description="Download adsb.lol globe_history release")
    parser.add_argument("--date",    required=True, help="Date in YYYY-MM-DD format")
    parser.add_argument("--variant", default="prod-0",
                        help="Release variant: prod-0 (default), staging-0, mlatonly-0")
    parser.add_argument("--repo",    default=None,
                        help="GitHub repo (auto-detected from year if omitted)")
    parser.add_argument("--out-dir", type=Path, default=None,
                        help="Directory to save files (default: data/)")
    parser.add_argument("--token",   default=None,
                        help="GitHub personal access token (avoids 60 req/hr rate limit)")
    args = parser.parse_args()

    # Validate date
    try:
        datetime.strptime(args.date, "%Y-%m-%d")
    except ValueError:
        sys.exit("--date must be YYYY-MM-DD, e.g. 2024-12-30")

    # Reformat date: 2024-12-30 → 2024.12.30
    dot_date = args.date.replace("-", ".")
    repo     = args.repo or repo_for_date(args.date)
    out_dir  = args.out_dir or Path("data")
    out_dir.mkdir(parents=True, exist_ok=True)

    # Discover assets via GitHub API
    tar_assets = find_release_assets(repo, dot_date, args.variant, args.token)
    if not tar_assets:
        sys.exit(
            f"No .tar assets found. Check the date, variant ({args.variant}), and repo ({repo}).\n"
            f"  Available variants: prod-0, staging-0, mlatonly-0"
        )

    print(f"Found {len(tar_assets)} part(s):")
    for a in tar_assets:
        size_mb = a["size"] / 1_000_000
        print(f"  {a['name']}  ({size_mb:.0f} MB)")
    print()

    # Download each part
    for asset in tar_assets:
        dest = out_dir / asset["name"]
        if dest.exists() and dest.stat().st_size == asset["size"]:
            print(f"  {asset['name']}  already complete, skipping.")
            continue
        t0 = time.perf_counter()
        download_file(asset["browser_download_url"], dest, args.token)
        elapsed = time.perf_counter() - t0
        mb = asset["size"] / 1_000_000
        print(f"  → {dest}  ({mb:.0f} MB in {elapsed:.0f}s, {mb/elapsed:.1f} MB/s)")

    print(f"\nDone. Files in: {out_dir}/")
    print()
    print("Next steps:")
    print(f"  python find_pings.py --lat <lat> --lon <lon> --data-dir {out_dir} --out pings.csv")


if __name__ == "__main__":
    main()
