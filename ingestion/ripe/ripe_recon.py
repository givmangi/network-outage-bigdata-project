"""
ripe_recon.py — RIPE Atlas Probe Discovery
===========================================
Generates a mapping of RIPE Atlas Probe IDs to their physical countries.
This is required because RIPE Atlas built-in measurements are global; 
we must filter the incoming data stream by probe ID to isolate our 15 countries.
"""

import json
import requests

# The 15 priority countries chosen by your team
COUNTRIES = ["IT", "MM", "IN", "PK", "UA", "RU", "PS", "SY", "IR", "TR", "BD", "NG", "US", "DE", "GB"]

# These are the RIPE Atlas Built-In Measurement IDs for IPv4 Pings 
# to the 13 Global DNS Root Servers (A through M). 
# We will use these in the next step, but it's good to document them here.
ROOT_PING_MEASUREMENT_IDS = [
    2009, # A-root
    2010, # B-root
    2011, # C-root
    2012, # D-root
    2013, # E-root
    2004, # F-root
    2014, # G-root
    2015, # H-root
    2005, # I-root
    2016, # J-root
    2001, # K-root
    2008, # L-root
    2006, # M-root
]

def fetch_probe_mapping():
    probe_mapping = {}
    
    print("Starting RIPE Atlas probe reconnaissance...")
    
    for country in COUNTRIES:
        print(f"Fetching active probes for {country}...")
        url = "https://atlas.ripe.net/api/v2/probes/"
        
        # We only want probes that are currently online (status=1) and public
        params = {
            "country_code": country,
            "status": 1,
            "is_public": "true"
        }
        
        while url:
            resp = requests.get(url, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            
            # Map the probe ID to its country code
            for probe in data.get("results", []):
                probe_id = str(probe["id"])
                probe_mapping[probe_id] = country
                
            # Handle RIPE Atlas API pagination
            url = data.get("next")
            params = None  # The 'next' URL already includes the query parameters
            
    print(f"\nSuccess! Found {len(probe_mapping)} total active probes across the 15 countries.")
    
    # Save the mapping to a JSON file
    output_file = "ripe_probe_mapping.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(probe_mapping, f, indent=2)
        
    print(f"Saved mapping to {output_file}. We are ready for ingestion.")

if __name__ == "__main__":
    fetch_probe_mapping()