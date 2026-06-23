import socket
import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

conn = psycopg2.connect(
    host="timescaledb", port=5432,
    dbname="outage_intelligence",
    user=os.environ["TIMESCALEDB_USER"],
    password=os.environ["TIMESCALEDB_PASSWORD"],
)
cur = conn.cursor()

cur.execute("SELECT DISTINCT asn FROM asn_baselines WHERE asn IS NOT NULL ORDER BY asn")
asns = [row[0] for row in cur.fetchall()]
print(f"Looking up {len(asns)} ASNs via Cymru bulk whois...")

try:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(("whois.cymru.com", 43))
    query = "begin\nverbose\n" + "\n".join(f"AS{asn}" for asn in asns) + "\nend\n"
    s.sendall(query.encode())

    response = b""
    while True:
        chunk = s.recv(4096)
        if not chunk:
            break
        response += chunk
    s.close()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS asn_names (
            asn INTEGER PRIMARY KEY,
            name TEXT,
            country TEXT
        )
    """)

    lines = response.decode("utf-8", errors="replace").strip().split("\n")
    count = 0
    for line in lines:
        if "|" not in line:
            continue
        parts = [p.strip() for p in line.split("|")]
        # Cymru verbose format: ASN | CC | Registry | Allocated | Name
        if len(parts) < 5:
            continue
        try:
            asn_num = int(parts[0])
            country = parts[1]
            name = parts[4] if parts[4] else f"AS{asn_num}"
            cur.execute("""
                INSERT INTO asn_names (asn, name, country)
                VALUES (%s, %s, %s)
                ON CONFLICT (asn) DO UPDATE SET name=EXCLUDED.name, country=EXCLUDED.country
            """, (asn_num, name, country))
            count += 1
        except Exception:
            continue

    conn.commit()
    print(f"Done! Resolved {count} ASNs.")

except Exception as e:
    print(f"Failed: {e}")

cur.close()
conn.close()