# src/ingest/ingest_census.py
import os, requests

def _get(url, params, user_agent: str):
    headers = {"User-Agent": user_agent}
    r = requests.get(url, params=params, headers=headers, timeout=30)
    r.raise_for_status()
    return r.json()

def fetch_abscs_us_level():
    key = os.getenv("CENSUS_API_KEY")
    ua  = os.getenv("USER_AGENT", "InflectBot/1.0 (contact: dev@inflect.local)")
    if not key:
        raise RuntimeError("Missing CENSUS_API_KEY")

    url = "https://api.census.gov/data/2023/abscs"
    params = {
        "get": "GEO_ID,NAME,NAICS2022,NAICS2022_LABEL,YEAR,EMP,PAYANN",
        "for": "us:*",
        "key": key,
    }
    return {"endpoint": "abscs", "params": params, "data": _get(url, params, ua)}

def fetch_bds_timeseries_example():
    key = os.getenv("CENSUS_API_KEY")
    ua  = os.getenv("USER_AGENT", "InflectBot/1.0 (contact: dev@inflect.local)")
    if not key:
        raise RuntimeError("Missing CENSUS_API_KEY")

    url = "https://api.census.gov/data/timeseries/bds"
    params = {
        "get": "YEAR,FIRMDEATHS,FIRMDEATHS_F,JOB_CREATION,JOB_CREATION_F",
        "for": "us:*",
        "key": key,
    }
    return {"endpoint": "bds", "params": params, "data": _get(url, params, ua)}

