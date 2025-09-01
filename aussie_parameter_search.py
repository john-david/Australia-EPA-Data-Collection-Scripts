

import asyncio
from collections import deque
from datetime import datetime, timezone
from typing import Any, Dict, List

import httpx
import pandas as pd
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv() # uses .env with with API_KEY

API_KEY = os.environ["API_KEY"]
BASE = "https://gateway.api.epa.vic.gov.au/environmentMonitoring/v1"
SINCE = "2000-01-01T00:00:00Z"

# ---------------------------
# Global async rate limiter: ≤5 requests/second
# ---------------------------
class AsyncRateLimiter:
    def __init__(self, max_per_sec: int):
        self.max_per_sec = max_per_sec
        self._win = deque()
        self._lock = asyncio.Lock()

    async def acquire(self):
        async with self._lock:
            now = asyncio.get_event_loop().time()
            # drop timestamps older than 1s
            while self._win and (now - self._win[0]) > 1.0:
                self._win.popleft()
            if len(self._win) >= self.max_per_sec:
                sleep_for = 1.0 - (now - self._win[0])
                if sleep_for > 0:
                    await asyncio.sleep(sleep_for)
                # re-check window after sleeping
                now = asyncio.get_event_loop().time()
                while self._win and (now - self._win[0]) > 1.0:
                    self._win.popleft()
            self._win.append(asyncio.get_event_loop().time())

rate_limiter = AsyncRateLimiter(max_per_sec=5)

# ---------------------------
# 1) Fetch AIR sites
# ---------------------------
async def fetch_air_sites() -> List[Dict[str, Any]]:
    params = {"environmentalSegment": "air"}
    headers = {"X-API-Key": API_KEY}
    async with httpx.AsyncClient(timeout=30) as client:
        await rate_limiter.acquire()
        r = await client.get(f"{BASE}/sites", params=params, headers=headers)
        r.raise_for_status()
        return r.json().get("records", [])

def sites_to_df(records: List[Dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for rec in records:
        coords = rec.get("geometry", {}).get("coordinates") or [None, None]
        lon, lat = (coords + [None, None])[:2]
        rows.append({
            "siteID": rec.get("siteID"),
            "siteName": rec.get("siteName"),
            "siteType": rec.get("siteType"),
            "lon": lon, "lat": lat,
        })
    return pd.DataFrame(rows)

# ---------------------------
# 2) Fetch parameters per site with 5 rps limit + 429 backoff
# ---------------------------

CHECKPOINTS = [
    "2000-01-01T00:00:00Z",
    "2010-01-01T00:00:00Z",
    "2015-01-01T00:00:00Z",
    "2020-01-01T00:00:00Z",
    "2023-01-01T00:00:00Z",
]

async def fetch_site_parameters(client: httpx.AsyncClient, site_id: str) -> dict:
    url = f"{BASE}/sites/{site_id}/parameters"
    headers = {"X-API-Key": API_KEY}
    # this route doesn't accept 'since'; scope to air segment instead
    params = {"environmentalSegment": "air"}

    # keep your rate limit + retry logic
    for attempt in range(4):
        await rate_limiter.acquire()
        resp = await client.get(url, headers=headers, params=params)
        if resp.status_code == 429:
            ra = resp.headers.get("Retry-After")
            try:
                delay = float(ra)
            except (TypeError, ValueError):
                delay = min(2 ** attempt, 8)
            await asyncio.sleep(delay)
            continue

        if resp.status_code == 404:
            return {"siteID": site_id, "parameters": []}

        resp.raise_for_status()
        data = resp.json()
        data["siteID"] = site_id
        return data

    return {"siteID": site_id, "parameters": [], "_error": "exhausted retries"}


async def fetch_all_parameters(site_ids: List[str], concurrency: int = 8) -> List[Dict[str, Any]]:
    sem = asyncio.Semaphore(concurrency)
    async with httpx.AsyncClient(timeout=60) as client:
        async def worker(sid):
            async with sem:
                return await fetch_site_parameters(client, sid)
        # optional tiny jitter to avoid initial burst
        tasks = []
        for i, sid in enumerate(site_ids):
            tasks.append(asyncio.create_task(worker(sid)))
            await asyncio.sleep(0.01)  # spread starts slightly
        return await asyncio.gather(*tasks)

# ---------------------------
# 3) Normalize to tidy DataFrame
# ---------------------------
def parameters_to_df(payloads):
    rows = []
    for item in payloads:
        sid = item.get("siteID")
        for p in (item.get("parameters") or []):
            pname = p.get("name")
            unit = p.get("unit")
            for ts in (p.get("timeSeriesReadings") or []):
                ts_name = ts.get("timeSeriesName")  # e.g., "1HR_AV" or "24HR_AV"
                for r in (ts.get("readings") or []):
                    rows.append({
                        "siteID": sid,
                        "parameter": pname,
                        "unit": unit,
                        "series": ts_name,
                        "since": r.get("since"),
                        "until": r.get("until"),
                        "averageValue": r.get("averageValue"),
                        "healthAdvice": r.get("healthAdvice"),
                        "healthAdviceColor": r.get("healthAdviceColor"),
                        "healthCode": r.get("healthCode"),
                    })
    df = pd.DataFrame(rows)
    if not df.empty:
        df["since"] = pd.to_datetime(df["since"], utc=True, errors="coerce")
        df["until"] = pd.to_datetime(df["until"], utc=True, errors="coerce")
        df.sort_values(["siteID", "parameter", "series", "since"], inplace=True)
    return df

# ---------------------------
# 4) Glue it together
# ---------------------------
async def main():
    records = await fetch_air_sites()
    sites_df = sites_to_df(records)
    print(f"Sites: {len(sites_df)}")
    site_ids = sites_df["siteID"].dropna().unique().tolist()

    # Pull parameters for each site with limiter + retries
    payloads = await fetch_all_parameters(site_ids, concurrency=8)

    # after gathering payloads
    bad = [p for p in payloads if p.get("_error")]
    if bad:
        print("\nSites that rejected very old 'since':")
        for b in bad[:10]:
            print(f"- {b['siteID']} :: {b.get('_error')[:160]}...")

    params_df = parameters_to_df(payloads)
    print("Parameter rows:", len(params_df))
    # Save if desired
    # sites_df.to_csv("epa_sites_air.csv", index=False)
    # params_df.to_parquet("epa_parameters_since_2000.parquet", index=False)
    print("dataframe:", params_df.head())
    print("saving dataframe")
    params_df.to_csv("aussie_data.cvs")

if __name__ == "__main__":
    asyncio.run(main())



