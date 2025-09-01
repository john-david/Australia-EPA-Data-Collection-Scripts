import asyncio
import httpx
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv() # uses .env with with API_KEY

API_KEY = os.environ["API_KEY"]
BASE = "https://gateway.api.epa.vic.gov.au/environmentMonitoring/v1"

async def fetch_sites(environmental_segment: str = "air"):
    url = f"{BASE}/sites"
    params = {"environmentalSegment": environmental_segment}
    headers_primary = {"X-API-Key": API_KEY}
    headers_alt = {"Ocp-Apim-Subscription-Key": API_KEY}  # some APIMs expect this name

    async with httpx.AsyncClient(timeout=20) as client:
        # Try with X-API-Key first
        r = await client.get(url, params=params, headers=headers_primary)
        if r.status_code == 200:
            return r.json()

async def main():
    try:
        data = await fetch_sites("air")
        total = data.get("totalRecords")
        print(f"Total site records: {total}")
        for rec in data.get("records", []):
            name = rec.get("siteName")
            sid = rec.get("siteID")
            coords = rec.get("geometry", {}).get("coordinates")
            s_type = rec.get("siteType")
            print(f"- {name} ({s_type})  id={sid}  coords={coords}")
    except httpx.HTTPStatusError as e:
        print("HTTP error:", e.response.status_code, e.response.text)
    except Exception as e:
        print("Error:", e)

if __name__ == "__main__":
    asyncio.run(main())



