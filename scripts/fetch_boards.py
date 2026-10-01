"""Fetch postings from job boards beyond LinkedIn -- Dice and Indeed -- via Apify.

Produces the same raw item shape fetch_jobs.py does (title, companyName, location,
description, link, postedAt, employmentType, source), so score_jobs.py and
push_to_sheets.py need no changes to take these alongside LinkedIn.

Reads `target_roles` and an optional `boards` block from the search config:

    "boards": {
      "dice":   {"enabled": true, "per_role": 40, "employment_type": ["CONTRACTS", "THIRD_PARTY"]},
      "indeed": {"enabled": true, "per_role": 40, "query_suffix": "contract"}
    }

Dice's actor only returns a ~500-character summary, so each Dice posting's full
description is read from the schema.org JSON-LD on its public job page -- without
it, skill matching and the max_required_years cutoff would be working blind.
"""

from __future__ import annotations

import argparse
import html
import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import requests
from dotenv import load_dotenv

APIFY_SYNC = "https://api.apify.com/v2/acts/{}/run-sync-get-dataset-items"
DICE_ACTOR = "worldunboxer~dice-jobs-scraper"
INDEED_ACTOR = "curious_coder~indeed-scraper"
BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
)
JSON_LD_REGEX = re.compile(r'<script type="application/ld\+json"[^>]*>(.*?)</script>', re.S)
CONTRACT_TYPES = {"contract", "contracts", "third party", "third_party", "temporary"}


def is_contract_type(value: str) -> bool:
    """Dice combines types into one string ("Contract, Third Party")."""
    return any(part.strip().lower() in CONTRACT_TYPES for part in re.split(r"[,/]", value or ""))


def run_actor(token: str, actor: str, payload: dict) -> list[dict]:
    """Run an actor synchronously and return its dataset. Token goes in a
    header, never the URL -- see fetch_jobs._auth_headers for why."""
    resp = requests.post(
        APIFY_SYNC.format(actor),
        headers={"Authorization": f"Bearer {token}"},
        json=payload,
        params={"timeout": 600},
        timeout=660,
    )
    resp.raise_for_status()
    return resp.json()


def _html_to_text(raw: str) -> str:
    text = re.sub(r"<(br|/p|/li|/div)[^>]*>", "\n", html.unescape(raw or ""), flags=re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"[ \t]+", " ", text).strip()


def dice_full_description(session: requests.Session, url: str) -> str | None:
    try:
        resp = session.get(url, timeout=20)
    except requests.RequestException:
        return None
    if resp.status_code != 200:
        return None
    for block in JSON_LD_REGEX.findall(resp.text):
        try:
            data = json.loads(block)
        except json.JSONDecodeError:
            continue
        for entry in data if isinstance(data, list) else [data]:
            if isinstance(entry, dict) and entry.get("@type") == "JobPosting":
                return _html_to_text(entry.get("description", ""))
    return None


def fetch_dice(token: str, config: dict, opts: dict) -> list[dict]:
    loc = config.get("location") or {}
    # Dice only offers 1/3/7-day windows; anything longer fetches all and
    # leaves the cutoff to score_jobs.py's max_posting_age_days.
    days = int(config.get("posted_within_days", 7))
    posted = {1: "ONE", 3: "THREE", 7: "SEVEN"}.get(days, "ANY" if days > 7 else "SEVEN")
    where = ", ".join(p for p in [loc.get("city"), loc.get("region"), "USA"] if p)

    def one(role: str) -> list[dict]:
        return run_actor(token, DICE_ACTOR, {
            "keyword": role,
            "location": where,
            "radius": 3000,  # whole country around the candidate's city
            "unit": "mi",
            "job_entries": int(opts.get("per_role", 40)),
            "posted_date": posted,
            "employment_type": opts.get("employment_type", ["CONTRACTS", "THIRD_PARTY"]),
            "employer_type": ["Direct Hire", "Recruiter", "Other"],
            "work_settings": ["Remote", "On-site", "Hybrid"],
        })

    raw: dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=4) as pool:
        for role, items in zip(config["target_roles"], pool.map(one, config["target_roles"])):
            print(f"  Dice '{role}': {len(items)}", flush=True)
            for item in items:
                raw.setdefault(item.get("guid") or item.get("details_page_url"), item)

    session = requests.Session()
    session.headers.update({"User-Agent": BROWSER_UA})
    print(f"Reading {len(raw)} full Dice descriptions...", flush=True)
    out, missed = [], 0
    for i, item in enumerate(raw.values(), start=1):
        link = item.get("details_page_url") or ""
        full = dice_full_description(session, link) if link else None
        if full is None:
            missed += 1
        emp = str(item.get("employment_type") or "")
        remote = bool(item.get("is_remote"))
        location = item.get("location") or ""
        out.append({
            "title": item.get("title") or "",
            "companyName": item.get("company") or "",
            "location": f"{location} (Remote)" if remote and location else location or "Remote, USA",
            "description": full or item.get("summary") or "",
            "link": link,
            "postedAt": item.get("posted_date") or "",
            "employmentType": "Contract" if is_contract_type(emp) else emp,
            "source": "Dice",
        })
        time.sleep(0.5)
        if i % 25 == 0:
            print(f"  [{i}/{len(raw)}]", flush=True)
    print(f"Dice: {len(out)} postings ({missed} kept with summary only -- page unreadable)")
    return out


def fetch_indeed(token: str, config: dict, opts: dict) -> list[dict]:
    suffix = opts.get("query_suffix", "")
    # Indeed accepts 1/3/7/14 only; past 14 send no filter and let
    # max_posting_age_days make the cut.
    days = int(config.get("posted_within_days", 7))
    window = next((str(d) for d in (1, 3, 7, 14) if days <= d), None)

    def one(role: str) -> list[dict]:
        payload = {
            "country": "us",
            "query": f"{role} {suffix}".strip(),
            "location": "United States",
            "count": int(opts.get("per_role", 40)),
        }
        if window:
            payload["postedWithinDays"] = window
        return run_actor(token, INDEED_ACTOR, payload)

    out: dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=4) as pool:
        for role, items in zip(config["target_roles"], pool.map(one, config["target_roles"])):
            print(f"  Indeed '{role}': {len(items)}", flush=True)
            for item in items:
                key = item.get("id") or item.get("viewJobLink")
                if key in out:
                    continue
                types = [t for t in item.get("jobTypes") or [] if t]
                company = item.get("companyDetails") or {}
                location = item.get("formattedLocation") or ""
                view = item.get("viewJobLink") or ""
                description = item.get("jobDescription") or ""
                if any(is_contract_type(t) for t in types):
                    employment = "Contract"
                elif not types and re.search(r"\bcontract\b", f"{item.get('title')}\n{description}", re.I):
                    # Untyped posting that describes itself as a contract.
                    employment = "Contract"
                else:
                    employment = types[0] if types else ""
                out[key] = {
                    "title": item.get("title") or "",
                    "companyName": (company.get("name") if isinstance(company, dict) else "") or item.get("jobSourceName") or "",
                    # Indeed writes remote jobs as a bare "Remote" -- give the
                    # country filter something to match.
                    "location": "Remote, USA" if location.strip().lower() == "remote" else location,
                    "description": description,
                    "link": f"https://www.indeed.com{view}" if view.startswith("/") else view,
                    "postedAt": str(item.get("pubDate") or ""),
                    "employmentType": employment,
                    "source": "Indeed",
                }
    print(f"Indeed: {len(out)} postings")
    return list(out.values())


FETCHERS = {"dice": fetch_dice, "indeed": fetch_indeed}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--raw-out", required=True, type=Path)
    args = parser.parse_args()

    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
    token = os.environ.get("APIFY_TOKEN")
    if not token:
        print("ERROR: APIFY_TOKEN not set in environment", file=sys.stderr)
        return 1

    config = json.loads(args.config.read_text(encoding="utf-8"))
    boards = config.get("boards") or {}
    items: list[dict] = []
    for name, fetch in FETCHERS.items():
        opts = boards.get(name) or {}
        if not opts.get("enabled"):
            continue
        print(f"Fetching {name}...", flush=True)
        items.extend(fetch(token, config, opts))

    args.raw_out.parent.mkdir(parents=True, exist_ok=True)
    args.raw_out.write_text(json.dumps(items, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Wrote {len(items)} raw postings to {args.raw_out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
