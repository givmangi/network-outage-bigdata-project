import json
import requests
import os
from dotenv import load_dotenv

# Load the target countries from your .env file
load_dotenv()
env_string = os.environ.get("TARGET_COUNTRIES", "IT MM IN PK UA RU PS SY IR TR BD NG US DE GB")
COUNTRIES = env_string.split()

PROBES_URL = "https://atlas.ripe.net/api/v2/probes/"

def build_probe_mapping():
    mapping = {}
    print(f"Fetching RIPE probes for {len(COUNTRIES)} countries...")
    
    for country in COUNTRIES:
        # We only want active (status=1), public probes
        params = {"country_code": country, "status": 1, "is_public": "true"}
        url = PROBES_URL
        
        try:
            # The RIPE API limits results per page, so we loop through the 'next' pages
            while url:
                resp = requests.get(url, params=params, timeout=10)
                resp.raise_for_status()
                data = resp.json()
                
                for probe in data.get("results", []):
                    probe_id = str(probe["id"])
                    asn = probe.get("asn_v4") # Grab the IPv4 ASN!
                    
                    # Only map probes that actually belong to a known ASN
                    if asn:
                        mapping[probe_id] = {
                            "country_code": country,
                            "asn": asn
                        }
                
                url = data.get("next")
                params = None # The 'next' URL already contains the parameters
                
        except Exception as e:
            print(f"  -> Failed to fetch probes for {country}: {e}")
            
    print(f"\nSuccessfully mapped {len(mapping)} physical probes with ASNs!")
    return mapping

if __name__ == "__main__":
    mapping = build_probe_mapping()
    
    # Save it with an indent so it's easy for humans to read
    with open("ripe_probe_mapping.json", "w", encoding="utf-8") as f:
        json.dump(mapping, f, indent=2)
        
    print("Saved mapping to ripe_probe_mapping.json.")