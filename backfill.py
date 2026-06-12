"""
backfill.py — IODA historical data loader
==========================================
Fetches country codes directly from the IODA API and runs the ingester
container sequentially for each one.

Usage:
    python backfill.py                        # all countries, 30 days
    python backfill.py --days 7               # all countries, 7 days
    python backfill.py --countries IT IQ      # specific countries, 30 days
    python backfill.py --countries IT --days 7
    python backfill.py --dry-run              # print what would run, don't execute

Must be run from the repo root with the Docker stack already up:
    docker compose up -d
    python backfill.py --days 7

Default country set (15 countries, ~20 min for 30-day backfill):
  IT MM IN PK UA RU PS SY IR TR BD NG US DE GB

Selected for: 
    IT (home country, AGCOM validation), 
    MM/IN/PK (top 3 shutdown frequency 2024), 
    UA/RU/PS/SY (active conflict zones), 
    IR/TR/BD (persistent censorship), 
    NG (Africa's largest market), 
    US/DE/GB (stable Western baselines).

WARNING: running without --countries fetches all 253 countries from the IODA API
and runs them sequentially. At ~75 seconds per country this takes approximately
5 hours. For a first run, use --countries IT IQ UA TR NG or similar to target
specific countries.
"""

import argparse
import subprocess
import sys
import time
import requests

IODA_ENTITIES_URL = "https://api.ioda.inetintel.cc.gatech.edu/v2/entities/query"


def fetch_all_country_codes() -> list[str]:
    """
    Fetch every country entity code available in IODA.
    Returns a sorted list of ISO 2-letter codes e.g. ['AE', 'AF', 'AG', ...]
    """
    print("Fetching available country codes from IODA API...")
    try:
        resp = requests.get(
            IODA_ENTITIES_URL,
            params={"entityType": "country", "limit": 500},
            timeout=30,
            headers={"User-Agent": "IODA-Backfill/1.0 (UniTrento)"},
        )
        resp.raise_for_status()
        data = resp.json().get("data", [])

        # Response is {"data": [{"code": "IT", "name": "Italy", ...}, ...]}
        entities = data if isinstance(data, list) else []

        codes = sorted(e["code"] for e in entities if "code" in e)
        if not codes:
            raise ValueError("No country codes found in API response")

        print(f"Found {len(codes)} countries: {', '.join(codes)}")
        return codes

    except Exception as exc:
        print(f"ERROR: Could not fetch country codes from IODA: {exc}")
        print("Check your internet connection or try --countries IT IQ to run manually.")
        sys.exit(1)


def run_backfill(country_code: str, days: int, dry_run: bool) -> bool:
    """
    Run the ingester container for one country.
    Returns True on success, False on failure.
    """
    cmd = [
        "docker", "compose", "run", "--rm",
        "-e", f"ENTITY_CODES={country_code}",
        "ingester",
        "python", "run_loop.py", "backfill", str(days),
    ]

    print(f"\n>>> [{country_code}] Starting {days}-day backfill...")
    print(f"    {' '.join(cmd)}")

    if dry_run:
        print("    (dry-run — skipping)")
        return True

    start = time.monotonic()
    result = subprocess.run(cmd)
    elapsed = time.monotonic() - start

    if result.returncode == 0:
        print(f"    [{country_code}] Done in {elapsed:.0f}s")
        return True
    else:
        print(f"    [{country_code}] FAILED (exit code {result.returncode})")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Backfill IODA bronze data for one or all countries."
    )
    parser.add_argument(
        "--countries",
        nargs="+",
        metavar="CODE",
        help="ISO country codes to backfill (e.g. IT IQ UA). Omit for all.",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Number of days to backfill (default: 30).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print commands without executing them.",
    )
    args = parser.parse_args()

    countries = args.countries if args.countries else fetch_all_country_codes()

    print(f"\nBackfill plan: {len(countries)} countries × {args.days} days")
    if args.dry_run:
        print("(dry-run mode — nothing will actually run)\n")

    failed = []
    for i, code in enumerate(countries, 1):
        print(f"\n[{i}/{len(countries)}]", end="")
        success = run_backfill(code, args.days, args.dry_run)
        if not success:
            failed.append(code)

    print("\n" + "=" * 50)
    print(f"Backfill complete: {len(countries) - len(failed)}/{len(countries)} succeeded")
    if failed:
        print(f"Failed countries: {', '.join(failed)}")
        print("Re-run just the failures with:")
        print(f"  python backfill.py --countries {' '.join(failed)} --days {args.days}")
        sys.exit(1)
    else:
        print("All countries ingested successfully.")


if __name__ == "__main__":
    main()