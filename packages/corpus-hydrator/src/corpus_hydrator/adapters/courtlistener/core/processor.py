"""CourtListener core processing functions.

This module provides core processing functions for CourtListener data,
including statute processing, docket handling, and document retrieval.
"""

from __future__ import annotations

import json
import re
import time
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional
import os
import math
import csv

import httpx
from loguru import logger

from corpus_types.schemas import CourtListenerConfig
from ..providers.client import CourtListenerClient
from ..parsers.query_builder import STATUTE_QUERIES, QueryBuilder


def slugify(text: str) -> str:
    """Convert text to a filesystem-safe slug."""
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")


def process_and_save(
    client: CourtListenerClient,
    resource_type: str,
    params: dict,
    output_dir: Path,
    limit: int = 10,
):
    """Fetch resource and save results to output_dir."""
    results = client.fetch_resource(resource_type, params, limit=limit)
    output_dir.mkdir(parents=True, exist_ok=True)
    for i, item in enumerate(results):
        with open(output_dir / f"{resource_type}_{i}.json", "w") as f:
            json.dump(item, f, indent=2)
    logger.info(f"Saved {len(results)} {resource_type} to {output_dir}")







def process_recap_fetch(config, post_data, show_url=False, token=None):
    """POST to /api/rest/v4/recap-fetch/ to trigger a PACER fetch. Allows safe, credentialed, free attachment fetch (type=3)."""
    import json
    import httpx

    base_url = "https://www.courtlistener.com/api/rest/v4/recap-fetch/"
    url = base_url
    # Use token from config if not provided
    if not token:
        token = getattr(config, "api_token", None)
    headers = {"Authorization": f"Token {token}"} if token else {}
    if show_url:
        print(f"POST URL: {url}")
        print(f"POST data: {post_data}")
        return
    # Only allow real PACER credentials for request_type=3 (free attachment pages)
    if str(post_data.get("request_type")) != "3" and (
        "pacer_username" in post_data or "pacer_password" in post_data
    ):
        print(
            "[TEST MODE] Not sending real PACER credentials or purchase request except for request_type=3 (free attachment pages)."
        )
        return
    if str(post_data.get("request_type")) == "3" and (
        "pacer_username" in post_data and "pacer_password" in post_data
    ):
        print(
            "[WARNING] You are sending PACER credentials to fetch free attachment pages. This will NOT purchase anything, but your credentials are required for authentication. They are not stored by CourtListener. Proceeding..."
        )
    try:
        with httpx.Client(timeout=60) as client:
            resp = client.post(url, data=post_data, headers=headers)
            if resp.status_code == 429:
                print(f"Rate limit exceeded for RECAP fetch—skipping to avoid further limits.")
                return
            elif resp.status_code == 400:
                print(f"Bad request for RECAP fetch (likely no free attachments)—skipping.")
                return
            resp.raise_for_status()
            data = resp.json()
    except httpx.HTTPStatusError as e:
        if e.response.status_code in (429, 400):
            print(f"RECAP fetch failed with status {e.response.status_code}—skipping.")
            return
        else:
            print(f"Error: {e}")
            raise
    except Exception as e:
        print(f"Unexpected error in RECAP fetch: {e}")
        raise

    print(json.dumps(data, indent=2))

def process_docket_entries(
    config: CourtListenerConfig,
    docket_id: int = None,
    query: str = None,
    order_by: str = "-date_filed",
    pages: int = 1,
    page_size: int = 20,
    output_dir: Optional[str] = None,
    api_mode: str = "standard",
) -> None:
    """Process docket entries and save the results."""
    import time
    import httpx

    client = CourtListenerClient(config, api_mode=api_mode)
    logger.info(
        f"Fetching docket entries with docket_id: {docket_id}, query: {query or 'all'}"
    )
    url = client.endpoints["docket_entries"]
    params = {"docket": docket_id, "order_by": order_by, "page_size": page_size}
    all_entries = []
    backoff = 1.0
    while url:
        try:
            data = client._get(url, params)
        except httpx.HTTPStatusError as e:
            code = e.response.status_code
            if code == 404:
                logger.warning(f"404 on {url}; stopping pagination.")
                break
            if 500 <= code < 600:
                logger.warning(f"Server {code} on {url}; retrying in {backoff}s…")
                time.sleep(backoff)
                backoff = min(backoff * 2, 10)
                continue
            raise
        except httpx.ReadTimeout:
            logger.warning(f"ReadTimeout on {url}; retrying in {backoff}s…")
            time.sleep(backoff)
            backoff = min(backoff * 2, 10)
            continue
        backoff = 1.0
        batch = data.get("results", [])
        if not batch:
            logger.info(f"No results on {url}; done.")
            break
        all_entries.extend(batch)
        url = data.get("next")
        params = None  # Only use params on first request
    logger.info(f"Retrieved {len(all_entries)} docket entries")

    # Create output directory
    if output_dir is None:
        if docket_id:
            output_dir = (
                Path("data")
                / "raw"
                / "courtlistener"
                / "docket_entries"
                / f"docket_{docket_id}"
            )
        else:
            output_dir = (
                Path("data") / "raw" / "courtlistener" / "docket_entries" / "search"
            )
    else:
        output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Save docket entries
    for i, entry in enumerate(all_entries):
        entry_path = output_dir / f"entry_{entry.get('id', i)}_metadata.json"
        with open(entry_path, "w") as f:
            json.dump(entry, f, indent=2)

        if entry.get("recap_documents"):
            docs_dir = output_dir / f"entry_{entry.get('id', i)}_documents"
            docs_dir.mkdir(exist_ok=True)
            for j, doc in enumerate(entry["recap_documents"]):
                doc_meta_path = docs_dir / f"doc_{doc.get('id', j)}_metadata.json"
                with open(doc_meta_path, "w") as f:
                    json.dump(doc, f, indent=2)
                if doc.get("plain_text"):
                    doc_text_path = docs_dir / f"doc_{doc.get('id', j)}_text.txt"
                    with open(doc_text_path, "w") as f:
                        f.write(doc["plain_text"])
    logger.info(f"Saved {len(all_entries)} docket entries to {output_dir}")
