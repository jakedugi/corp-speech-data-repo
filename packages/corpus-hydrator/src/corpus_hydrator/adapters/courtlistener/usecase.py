"""
CourtListener Use Cases with Sophisticated Async Orchestration

This module contains use case classes that orchestrate CourtListener operations
with advanced error handling, rate limiting, and parallel processing capabilities.
"""

from pathlib import Path
from typing import List, Optional
import os
import json
from urllib.parse import urljoin
from httpx import HTTPStatusError
import asyncio

from loguru import logger
from corpus_types.schemas import CourtListenerConfig

from .providers.client import CourtListenerClient, AsyncCourtListenerClient
from .parsers.query_builder import QueryBuilder, build_queries
from .core.processor import process_and_save, process_docket_entries, process_recap_fetch
from .utils.file_io import download, needs_recap_fetch, download_missing_pdfs, load_json, ensure_dir


class CourtListenerUseCase:
    """Main use case for CourtListener operations with sophisticated async orchestration."""

    def __init__(
        self,
        config: CourtListenerConfig,
        statutes: List[str] = None,
        company_file: Optional[Path] = None,
        outdir: Path | str = "CourtListener",
        token: str | None = None,
        pages: int = 1,
        page_size: int = 50,
        date_min: str | None = None,
        api_mode: str = "standard",
        chunk_size: int = 10,
        max_companies: int | None = None,
        max_results: int | None = None,
        max_cases: int | None = None,
    ):
        """Initialize the CourtListener use case.

        Args:
            config: API configuration
            statutes: List of statutes to process
            company_file: Path to CSV file with company names
            outdir: Output directory
            token: API token
            pages: Number of pages to fetch
            page_size: Results per page
            date_min: Minimum filing date
            api_mode: API mode ("standard" or "recap")
            chunk_size: Companies per query chunk
            max_cases: Maximum total cases to process (strict limit)
        """
        self.config = config
        self.statutes = statutes or ["FTC Section 5"]
        self.company_file = Path(company_file) if company_file else None
        self.outdir = Path(outdir)
        self.token = token or os.getenv("COURTLISTENER_API_TOKEN")
        self.pages = pages
        self.page_size = page_size
        self.date_min = date_min
        self.api_mode = api_mode
        self.chunk_size = chunk_size
        self.max_companies = max_companies
        self.max_results = max_results
        self.max_cases = max_cases
        self.total_cases_processed = 0  # Track total cases across all queries

        # Check if PDF downloads should be disabled
        self.disable_pdf_downloads = os.getenv("COURTLISTENER_DISABLE_PDF_DOWNLOADS", "false").lower() == "true"

        # Initialize components
        self.client = CourtListenerClient(config, api_mode=api_mode)
        self.async_client = AsyncCourtListenerClient(
            self.token, max_concurrency=2, rate_limit=3.0, api_mode=self.api_mode
        )
        self.query_builder = QueryBuilder()


    def run(self):
        """
        Orchestrator: process statutes with company chunks based on configuration.
        Supports strict case limits that override all other limits.
        """
        if self.max_cases is not None:
            logger.info(f"🎯 STRICT CASE LIMIT: Max {self.max_cases} total cases")
        elif self.max_companies == 1:
            logger.info("🧪 TEST MODE: 1 company processing")
        else:
            logger.info("📊 BASE MODE: All companies with full pagination")

        for statute in self.statutes:
            queries = build_queries(
                statute, self.company_file, chunk_size=self.chunk_size
            )

            logger.info(f"Processing {len(queries)} query chunks for {statute}")

            for i, query in enumerate(queries):
                # Check if we've reached the case limit
                if self.max_cases is not None and self.total_cases_processed >= self.max_cases:
                    logger.info(f"🎯 Reached case limit ({self.max_cases}), stopping orchestration")
                    break

                remaining_cases = self.max_cases - self.total_cases_processed if self.max_cases else None

                if self.max_cases is not None:
                    logger.info(f"Processing chunk {i+1}/{len(queries)} (need {remaining_cases} more cases): {query}")
                else:
                    logger.info(f"Processing chunk {i+1}/{len(queries)}: {query}")

                search_dir = self.outdir / "search"
                cases_processed = self._search_and_hydrate(query, search_dir)

                if cases_processed > 0:
                    logger.info(f"Chunk {i+1} processed {cases_processed} cases (total: {self.total_cases_processed})")

                # Stop if we've reached the limit
                if self.max_cases is not None and self.total_cases_processed >= self.max_cases:
                    break

        if self.max_cases is not None:
            logger.success(f"CourtListener orchestration completed: {self.total_cases_processed}/{self.max_cases} cases processed")
        else:
            logger.success("CourtListener orchestration completed")



    def _search_and_hydrate(self, query: str, search_dir: Path):
        """
        Run /search/ (type=d) for dockets, save results, and hydrate dockets.
        Respects strict case limits that override all other limits.

        Args:
            query: The search query string
            search_dir: Directory to save search results

        Returns:
            int: Number of cases processed from this query
        """
        from .utils.file_io import ensure_dir

        ensure_dir(search_dir)

        # Calculate how many cases we can still process
        if self.max_cases is not None:
            remaining_capacity = self.max_cases - self.total_cases_processed
            if remaining_capacity <= 0:
                logger.info("🎯 Case limit already reached, skipping this query")
                return 0

            # Use the smaller of our configured limits and remaining capacity
            effective_page_size = min(self.page_size, remaining_capacity)
            effective_max_results = min(self.max_results or 1000, remaining_capacity)
        else:
            effective_page_size = self.page_size
            effective_max_results = self.max_results

        # Search for cases
        if self.max_cases is not None and self.max_cases <= 5:
            # Strict limit mode: Get exactly what we need
            params = {"q": query, "type": "d", "page_size": effective_page_size}
            data = self.client._get(f"{self.client.BASE_URL}/search/", params)
            results = data.get("results", [])[:effective_page_size]
            logger.info(f"🎯 Strict limit: Found {len(results)} cases (needed {remaining_capacity})")
        else:
            # Standard mode: Full pagination with limits
            all_results = []
            page = 1
            params = {"q": query, "type": "d", "page_size": effective_page_size}

            while True:
                data = self.client._get(f"{self.client.BASE_URL}/search/", params)
                batch_results = data.get("results", [])
                if not batch_results:
                    break

                # Add results but respect our limits
                remaining_needed = effective_max_results - len(all_results) if effective_max_results else len(batch_results)
                if remaining_needed > 0:
                    all_results.extend(batch_results[:remaining_needed])

                logger.info(f"📊 Page {page}: Retrieved {len(batch_results)} cases, kept {len(all_results)} total")

                # Check if we've reached our limit or there's no next page
                if (effective_max_results and len(all_results) >= effective_max_results) or not data.get("next"):
                    break

                # Update params for next page
                params["page"] = page + 1
                page += 1

                # Safety check to prevent infinite loops
                if page > 100:
                    logger.warning("Reached maximum page limit (100), stopping pagination")
                    break

            results = all_results
            data = {"results": results, "count": len(results)}

        # Apply final case limit if needed
        if self.max_cases is not None:
            available_slots = self.max_cases - self.total_cases_processed
            if len(results) > available_slots:
                results = results[:available_slots]
                data = {"results": results, "count": len(results)}
                logger.info(f"🎯 Limited to {len(results)} cases to respect total limit")

        # Save search results
        search_path = search_dir / "search_api_results.json"
        with open(search_path, "w") as f:
            json.dump(data, f, indent=2)

        cases_found = len(results)
        logger.info(f"Saved search results ({cases_found} cases) to {search_path}")

        # Process cases in parallel using async
        if results:
            asyncio.run(self._hydrate_dockets_async(results))
            self.total_cases_processed += len(results)

        return cases_found

    async def _hydrate_dockets_async(self, dockets: list):
        """
        Hydrate multiple dockets in parallel using async processing.

        Args:
            dockets: List of docket dictionaries from search results
        """
        logger.info(f"Processing {len(dockets)} dockets in parallel")

        # Create tasks for parallel processing
        tasks = []
        for dk in dockets:
            task = asyncio.create_task(self._hydrate_docket_async(dk))
            tasks.append(task)

        # Wait for all tasks to complete
        await asyncio.gather(*tasks, return_exceptions=True)
        logger.info("Parallel docket processing completed")

    def _hydrate_docket(self, dk: dict):
        """
        Run all legacy steps for a single docket: shell, IA dump, RECAP fetch, entries, filings, clusters, opinions.
        Only creates directories when data is actually found.

        Args:
            dk: Docket JSON from /search/ results
        """
        from .utils.file_io import download, needs_recap_fetch, download_missing_pdfs, load_json, ensure_dir

        dk_id = dk["docket_id"]
        dk_num = dk.get("docketNumber")
        court = dk.get("court_id")
        slug = f"{dk_num}_{court}"
        case_dir = self.outdir / slug
        ensure_dir(case_dir)

        # 2️⃣ Docket shell
        dockets_dir = case_dir / "dockets"
        process_and_save(self.client, "dockets", {"id": dk_id}, dockets_dir, limit=1)

        # 3️⃣ IA dump
        ia_json_path = dockets_dir / "dockets_0.json"
        ia_url = None
        if ia_json_path.exists():
            meta = load_json(ia_json_path)
            ia_url = meta.get("filepath_ia_json")
        if ia_url:
            ia_dump_path = case_dir / "ia_dump.json"
            if not ia_dump_path.exists():
                try:
                    download(ia_url, ia_dump_path)
                except Exception as e:
                    logger.warning(f"Failed to download IA dump for {slug}: {e}")

        # 4️⃣ Free-attachment back-fill if gaps
        ia_dump_path = case_dir / "ia_dump.json"
        if needs_recap_fetch(ia_dump_path):
            pacer_user = os.getenv("PACER_USER")
            pacer_pass = os.getenv("PACER_PASS")

            if not pacer_user or not pacer_pass:
                logger.debug(f"Skipping RECAP fetch for {slug} - PACER credentials not configured")
                if not self.disable_pdf_downloads:
                    logger.debug("To enable PDF downloads, set PACER_USER and PACER_PASS environment variables")
            else:
                logger.info(f"Triggering RECAP free-attachment fetch for {slug}")
                payload = {
                    "request_type": 3,
                    "docket": str(dk_id),  # Use internal docket ID for v4
                    "pacer_username": pacer_user,
                    "pacer_password": pacer_pass,
                }
                try:
                    from .core.processor import process_recap_fetch
                    process_recap_fetch(self.config, payload)
                except HTTPStatusError as e:
                    if e.response.status_code == 400:
                        logger.debug(
                            f"No free RECAP attachments for {slug}; skipping. ({e})"
                        )
                    else:
                        raise

        # 5️⃣ Entries (RECAP mode gives nested docs)
        entries_dir = case_dir / "entries"
        process_docket_entries(
            self.config,
            docket_id=dk_id,
            order_by="recap_sequence_number",
            pages=1,
            page_size=100,
            output_dir=entries_dir,
            api_mode="recap",
        )

        # --- Patch: fetch recap-document metadata and PDFs for each entry ---
        filings_dir = case_dir / "filings"
        ensure_dir(filings_dir)
        for entry_file in entries_dir.glob("*.json"):
            entry = load_json(entry_file)
            for doc_meta in entry.get("recap_documents", []):
                resource = doc_meta.get("resource_uri")
                if resource:
                    try:
                        doc = self.client._get(resource)
                        # Check if doc is None (API request failed)
                        if doc is None:
                            logger.warning(
                                f"Failed to fetch recap-document {resource}—API returned None, skipping."
                            )
                            continue
                        # Check if doc has required fields
                        if not isinstance(doc, dict) or 'id' not in doc:
                            logger.warning(
                                f"Recap-document {resource} returned invalid data—skipping."
                            )
                            continue
                    except HTTPStatusError as e:
                        if e.response.status_code == 503:
                            logger.warning(
                                f"Recap-document {resource} temporarily unavailable—skipping."
                            )
                            continue
                        elif e.response.status_code == 429:
                            logger.warning(
                                f"Rate limit exceeded for {resource}—skipping to avoid further limits."
                            )
                            continue
                        else:
                            logger.warning(
                                f"HTTP error {e.response.status_code} for {resource}—skipping."
                            )
                            continue
                    except Exception as e:
                        logger.warning(
                            f"Unexpected error fetching {resource}: {e}—skipping."
                        )
                        continue

                    doc_path = entries_dir / f"doc_{doc['id']}.json"
                    with doc_path.open("w") as f:
                        json.dump(doc, f, indent=2)
                    # Download PDF if available and allowed
                    if not self.disable_pdf_downloads and doc.get("filepath_local"):
                        if doc.get("is_available") is False:
                            logger.warning(
                                f"PDF {doc['id']} is marked unavailable—skipping."
                            )
                            continue
                        pdf_url = urljoin(
                            "https://www.courtlistener.com/", doc["filepath_local"]
                        )
                        pdf_dest = filings_dir / f"{doc['id']}.pdf"
                        if not pdf_dest.exists():
                            if not pdf_url.startswith("http"):
                                logger.warning(f"Skipping invalid URL: {pdf_url}")
                            else:
                                try:
                                    download(pdf_url, pdf_dest)
                                except HTTPStatusError as e:
                                    code = e.response.status_code
                                    if code in (403, 429):
                                        logger.warning(
                                            f"PDF {doc['id']} returned HTTP {code}—skipping."
                                        )
                                    else:
                                        raise
                                except Exception as e:
                                    logger.warning(
                                        f"Failed to download PDF for doc {doc['id']}: {e}"
                                    )

        # 6️⃣ Clusters → Opinions (only create directories if data exists)
        clusters = self.client.fetch_resource("clusters", {"docket": dk_id}, limit=100)
        if clusters:
            clusters_dir = case_dir / "clusters"
            ensure_dir(clusters_dir)
            for i, cluster in enumerate(clusters):
                with open(clusters_dir / f"clusters_{i}.json", "w") as f:
                    json.dump(cluster, f, indent=2)

            # Fetch opinions for each cluster
            opinions_dir = case_dir / "opinions"
            ensure_dir(opinions_dir)
            for cluster in clusters:
                cl_id = cluster["id"]
                opinions = self.client.fetch_resource("opinions", {"cluster": cl_id}, limit=100)
                if opinions:
                    for j, opinion in enumerate(opinions):
                        with open(opinions_dir / f"opinions_{j}.json", "w") as f:
                            json.dump(opinion, f, indent=2)
        else:
            logger.info(f"No clusters found for docket {dk_id} ({slug})")

    async def _hydrate_docket_async(self, dk: dict):
        """
        Fully async version: ALL operations use async processing for maximum parallelism.
        Only creates directories when data is actually found.

        Args:
            dk: Docket JSON from /search/ results
        """
        from .utils.file_io import download, needs_recap_fetch, download_missing_pdfs, load_json, ensure_dir
        import json
        from urllib.parse import urljoin
        from httpx import HTTPStatusError

        dk_id = dk["docket_id"]
        dk_num = dk.get("docketNumber")
        court = dk.get("court_id")
        slug = f"{dk_num}_{court}"
        case_dir = self.outdir / slug
        ensure_dir(case_dir)

        # 2️⃣ Docket shell (async)
        dockets_dir = case_dir / "dockets"
        docket_data = await self.async_client.fetch_resource_async("dockets", {"id": dk_id}, limit=1)
        if docket_data:
            ensure_dir(dockets_dir)
            with open(dockets_dir / "dockets_0.json", "w") as f:
                json.dump(docket_data[0], f, indent=2)

        # 3️⃣ IA dump
        ia_json_path = dockets_dir / "dockets_0.json"
        ia_url = None
        if ia_json_path.exists():
            meta = load_json(ia_json_path)
            ia_url = meta.get("filepath_ia_json")
        if ia_url:
            ia_dump_path = case_dir / "ia_dump.json"
            if not ia_dump_path.exists():
                try:
                    download(ia_url, ia_dump_path)
                except Exception as e:
                    logger.warning(f"Failed to download IA dump for {slug}: {e}")

        # 4️⃣ Free-attachment back-fill if gaps
        ia_dump_path = case_dir / "ia_dump.json"
        if needs_recap_fetch(ia_dump_path):
            pacer_user = os.getenv("PACER_USER")
            pacer_pass = os.getenv("PACER_PASS")

            if not pacer_user or not pacer_pass:
                logger.debug(f"Skipping RECAP fetch for {slug} - PACER credentials not configured")
                if not self.disable_pdf_downloads:
                    logger.debug("To enable PDF downloads, set PACER_USER and PACER_PASS environment variables")
            else:
                logger.info(f"Triggering RECAP free-attachment fetch for {slug}")
                payload = {
                    "request_type": 3,
                    "docket": str(dk_id),
                    "pacer_username": pacer_user,
                    "pacer_password": pacer_pass,
                }
                try:
                    from .core.processor import process_recap_fetch
                    process_recap_fetch(self.config, payload)
                except HTTPStatusError as e:
                    if e.response.status_code == 400:
                        logger.debug(
                            f"No free RECAP attachments for {slug}; skipping. ({e})"
                        )
                    else:
                        raise

        # 5️⃣ Entries (async RECAP mode)
        entries_dir = case_dir / "entries"
        entries_data = await self.async_client.fetch_resource_async(
            "docket_entries",
            {"docket": dk_id, "order_by": "recap_sequence_number", "page_size": 100},
            limit=None
        )
        if entries_data:
            ensure_dir(entries_dir)
            for i, entry in enumerate(entries_data):
                entry_path = entries_dir / f"entry_{entry.get('id', i)}_metadata.json"
                with open(entry_path, "w") as f:
                    json.dump(entry, f, indent=2)

                # Save nested recap documents
                if entry.get("recap_documents"):
                    docs_dir = entries_dir / f"entry_{entry.get('id', i)}_documents"
                    ensure_dir(docs_dir)
                    for j, doc in enumerate(entry["recap_documents"]):
                        doc_meta_path = docs_dir / f"doc_{doc.get('id', j)}_metadata.json"
                        with open(doc_meta_path, "w") as f:
                            json.dump(doc, f, indent=2)

            logger.info(f"Saved {len(entries_data)} docket entries to {entries_dir}")

        # 6️⃣ RECAP Documents (fully async parallel fetching)
        filings_dir = case_dir / "filings"
        ensure_dir(filings_dir)

        # Gather all recap document URIs
        doc_uris = []
        if entries_data:
            for entry in entries_data:
                for doc_meta in entry.get("recap_documents", []):
                    resource = doc_meta.get("resource_uri")
                    if resource:
                        doc_uris.append(resource)

        # Fetch all recap documents in parallel batches
        if doc_uris:
            logger.info(f"Fetching {len(doc_uris)} recap documents in parallel for {slug}")
            results = []
            batch_size = 5  # Larger batches for better parallelism

            for i in range(0, len(doc_uris), batch_size):
                batch = doc_uris[i : i + batch_size]
                batch_results = await self.async_client.fetch_docs(batch)
                results.extend(batch_results)

                # Rate limiting between batches
                if i + batch_size < len(doc_uris):
                    await asyncio.sleep(0.3)

            # Process all results
            for resource, doc in zip(doc_uris, results):
                if isinstance(doc, Exception):
                    logger.warning(f"Failed to fetch recap document {resource}: {doc}")
                    continue
                if doc is None or not isinstance(doc, dict) or "id" not in doc:
                    logger.warning(f"Invalid recap document data for {resource}")
                    continue

                # Save document metadata
                doc_path = entries_dir / f"doc_{doc['id']}.json"
                with doc_path.open("w") as f:
                    json.dump(doc, f, indent=2)

                # Download PDF if available (async-compatible)
                # NOTE: Most RECAP PDFs require PACER authentication and are not freely available
                if not self.disable_pdf_downloads and doc.get("filepath_local"):
                    if doc.get("is_available") is False:
                        logger.debug(f"PDF {doc['id']} is marked unavailable—skipping.")
                        continue

                    pdf_url = urljoin("https://www.courtlistener.com/", doc["filepath_local"])
                    pdf_dest = filings_dir / f"{doc['id']}.pdf"

                    if not pdf_dest.exists():
                        if not pdf_url.startswith("http"):
                            logger.debug(f"Skipping invalid PDF URL: {pdf_url}")
                        else:
                            try:
                                # Add basic auth header for CourtListener API access
                                headers = {}
                                if self.token:
                                    headers["Authorization"] = f"Token {self.token}"

                                # Use httpx with auth headers
                                import httpx
                                with httpx.Client(timeout=30, headers=headers) as client:
                                    resp = client.get(pdf_url)
                                    resp.raise_for_status()
                                    pdf_dest.parent.mkdir(parents=True, exist_ok=True)
                                    pdf_dest.write_bytes(resp.content)
                                    logger.info(f"Downloaded PDF {doc['id']} for {slug}")
                            except httpx.HTTPStatusError as e:
                                code = e.response.status_code
                                if code == 403:
                                    logger.debug(f"PDF {doc['id']} requires authentication (PACER)—skipping.")
                                elif code == 429:
                                    logger.warning(f"Rate limited downloading PDF {doc['id']}")
                                else:
                                    logger.debug(f"HTTP {code} downloading PDF {doc['id']}")
                            except Exception as e:
                                logger.debug(f"Failed to download PDF {doc['id']}: {type(e).__name__}")

        # 7️⃣ Clusters & Opinions (fully async)
        clusters_data = await self.async_client.fetch_resource_async(
            "clusters", {"docket": dk_id}, limit=100
        )

        if clusters_data:
            logger.info(f"Found {len(clusters_data)} clusters for {slug}")
            clusters_dir = case_dir / "clusters"
            ensure_dir(clusters_dir)

            # Save clusters
            for i, cluster in enumerate(clusters_data):
                with open(clusters_dir / f"clusters_{i}.json", "w") as f:
                    json.dump(cluster, f, indent=2)

            # Fetch opinions for all clusters in parallel
            opinions_dir = case_dir / "opinions"
            ensure_dir(opinions_dir)

            # Parallel opinion fetching
            opinion_tasks = []
            for cluster in clusters_data:
                cl_id = cluster["id"]
                task = self.async_client.fetch_resource_async(
                    "opinions", {"cluster": cl_id}, limit=100
                )
                opinion_tasks.append((cl_id, task))

            # Execute all opinion fetches in parallel
            opinion_results = await asyncio.gather(
                *[task for _, task in opinion_tasks],
                return_exceptions=True
            )

            # Save opinions
            for (cl_id, _), opinions in zip(opinion_tasks, opinion_results):
                if isinstance(opinions, Exception):
                    logger.warning(f"Failed to fetch opinions for cluster {cl_id}: {opinions}")
                    continue
                if opinions:
                    for j, opinion in enumerate(opinions):
                        with open(opinions_dir / f"opinions_{j}.json", "w") as f:
                            json.dump(opinion, f, indent=2)

            logger.info(f"Saved opinions for {len(clusters_data)} clusters in {slug}")
        else:
            logger.info(f"No clusters found for docket {dk_id} ({slug})")

        logger.info(f"✅ Completed full async hydration for {slug}")
