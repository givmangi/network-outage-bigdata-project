"""
backfill.py — Master Historical Data Loader (IODA & RIPE)
=========================================================
Orchestrates historical data backfills across different ingestion containers.

Usage:
    python backfill.py --source ioda --days 7          # IODA only
    python backfill.py --source ripe --days 7          # RIPE only
    python backfill.py --source all  --days 30         # Both sources

    # IODA specific filtering:
    python backfill.py --source ioda --countries IT IQ --days 7
"""
from dotenv import load_dotenv, find_dotenv
import os
import argparse
import subprocess
import sys
import time
import requests

IODA_ENTITIES_URL = "https://api.ioda.inetintel.cc.gatech.edu/v2/entities/query"

def fetch_all_country_codes() -> list[str]:
    """Fetch every country entity code available in IODA."""
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
        entities = data if isinstance(data, list) else []
        codes = sorted(e["code"] for e in entities if "code" in e)
        if not codes:
            raise ValueError("No country codes found in API response")
        print(f"Found {len(codes)} countries: {', '.join(codes)}")
        return codes
    except Exception as exc:
        print(f"ERROR: Could not fetch country codes from IODA: {exc}")
        sys.exit(1)

def run_ioda_backfill(country_code: str, days: int, dry_run: bool) -> bool:
    """Run the IODA ingester container for one country."""
    cmd = [
        "docker", "compose", "run", "--rm",
        "-e", f"ENTITY_CODES={country_code}",
        "ingester",
        "python", "run_loop.py", "backfill", str(days),
    ]

    print(f"\n>>> [IODA] [{country_code}] Starting {days}-day backfill...")
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

def run_ripe_backfill(days: int, dry_run: bool) -> bool:
    """Run the RIPE historical backfill container for all priority countries."""
    # Unlike IODA, RIPE has no per-country API parameter — root server ping
    # measurements are global by nature. Country scoping happens upstream,
    # via ripe_recon.py filtering probe IDs into ripe_probe_mapping.json
    # (built from TARGET_COUNTRIES). This function just runs the container
    # once; no per-country loop is needed or possible here.
    cmd = [
        "docker", "compose", "run", "--rm",
        "ripe-ingester",
        "python", "ripe_bronze_ingestion.py", "--days", str(days),
    ]

    print(f"\n>>> [RIPE Atlas] Starting {days}-day backfill for all target countries...")
    if dry_run:
        print("    (dry-run — skipping)")
        return True

    start = time.monotonic()
    result = subprocess.run(cmd)
    elapsed = time.monotonic() - start

    if result.returncode == 0:
        print(f"    [RIPE Atlas] Done in {elapsed:.0f}s")
        return True
    else:
        print(f"    [RIPE Atlas] FAILED (exit code {result.returncode})")
        return False

def main():
    load_dotenv() # Load variables from .env
    
    parser = argparse.ArgumentParser(description="Master Backfill Orchestrator (IODA & RIPE)")
    parser.add_argument("--source", choices=["ioda", "ripe", "all"], default="all", help="Which data source to backfill.")
    parser.add_argument("--countries", nargs="+", metavar="CODE", help="ISO country codes for IODA. Omit to use .env targets.")
    parser.add_argument("--days", type=int, default=30, help="Number of days to backfill (default: 30).")
    parser.add_argument("--dry-run", action="store_true", help="Print commands without executing them.")
    args = parser.parse_args()

    print(f"\n=== Master Backfill Plan: {args.days} Days | Source: {args.source.upper()} ===")
    
    # --- RIPE BACKFILL ---
    if args.source in ["ripe", "all"]:
        success = run_ripe_backfill(args.days, args.dry_run)
        if not success:
            print("\nWARNING: RIPE Backfill encountered an error.")

    # --- IODA BACKFILL ---
    if args.source in ["ioda", "all"]:
        # NEW LOGIC: Use CLI args first, then .env file, then fallback to all 253.
        if args.countries:
            countries = args.countries
        else:
            env_string = os.environ.get("TARGET_COUNTRIES")
            if env_string:
                countries = env_string.split()
                print(f"Loaded {len(countries)} target countries from .env file.")
            else:
                print("No targets in .env. Defaulting to ALL countries.")
                countries = fetch_all_country_codes()
                
        print(f"\nIODA Plan: {len(countries)} countries")
        
        failed = []
        for i, code in enumerate(countries, 1):
            print(f"\n[{i}/{len(countries)}]", end="")
            success = run_ioda_backfill(code, args.days, args.dry_run)
            if not success:
                failed.append(code)

        print("\n" + "=" * 50)
        print(f"IODA Backfill complete: {len(countries) - len(failed)}/{len(countries)} succeeded")
        if failed:
            print(f"Failed IODA countries: {', '.join(failed)}")

if __name__ == "__main__":
    main()