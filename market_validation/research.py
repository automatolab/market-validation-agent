from __future__ import annotations

import html as html_lib
import json
import re
import time
from dataclasses import dataclass
from typing import Any, Protocol
from urllib.error import URLError
from urllib.parse import parse_qs, urlencode, urlparse, urlsplit
from urllib.request import Request, urlopen

from .llm import OllamaClient
from .models import (
    EvidenceBasis,
    EvidenceInput,
    MarketSearchRequest,
    RawSourceRecord,
    StructuredEvidenceItem,
    ValidationRequest,
)


@dataclass(frozen=True)
class WebResult:
    title: str
    url: str
    snippet: str
    query_label: str


@dataclass(frozen=True)
class PageSummary:
    url: str
    title: str
    description: str
    excerpt: str


@dataclass(frozen=True)
class ResearchMissionOutput:
    web_results: list[WebResult]
    page_summaries: dict[str, PageSummary]
    raw_sources: list[RawSourceRecord]
    structured_evidence: list[StructuredEvidenceItem]
    diagnostics: dict[str, Any]


class WebSearcher(Protocol):
    def search(self, query: str, max_results: int) -> list[WebResult]:
        ...


class PageFetcher(Protocol):
    def fetch(self, url: str) -> PageSummary | None:
        ...


class DuckDuckGoSearcher:
    """DuckDuckGo-based search adapter used for live market evidence gathering."""

    def __init__(self, timeout_seconds: float = 6.0, max_bytes: int = 450_000) -> None:
        self._timeout_seconds = timeout_seconds
        self._max_bytes = max_bytes
        self.last_errors: list[str] = []

    def search(self, query: str, max_results: int) -> list[WebResult]:
        self.last_errors = []
        
        # Use a small sleep between distinct queries to avoid rate limiting
        time.sleep(0.25)

        ddgs_results = self._search_with_ddgs(query=query, max_results=max_results)
        if ddgs_results:
            return ddgs_results

        html_results = self._search_via_html(query=query, max_results=max_results)
        if html_results:
            return html_results

        return []

    def _search_with_ddgs(self, query: str, max_results: int) -> list[WebResult]:
        try:
            from duckduckgo_search import DDGS
        except ImportError as exc:
            self.last_errors.append(f"ddgs_import_error:{exc.__class__.__name__}")
            return []

        backends = ("lite", "html") # Dropped "api" as it is often more restrictive
        seen_urls: set[str] = set()

        for backend in backends:
            try:
                results: list[WebResult] = []
                # Use a tighter timeout for the library calls
                with DDGS(timeout=self._timeout_seconds) as ddgs:
                    for item in ddgs.text(
                        query,
                        max_results=max_results,
                        backend=backend,
                        safesearch="moderate",
                    ):
                        title = str(item.get("title", "")).strip()
                        raw_url = str(item.get("href", "")).strip()
                        snippet = str(item.get("body", "")).strip()
                        
                        # Fix 1: Always resolve redirect URLs
                        url = self._resolve_result_url(raw_url)
                        
                        if not (title and url):
                            continue
                        if url in seen_urls:
                            continue
                        seen_urls.add(url)
                        results.append(
                            WebResult(
                                title=title,
                                url=url,
                                snippet=snippet,
                                query_label=query,
                            )
                        )
                if results:
                    return results
            except Exception as exc:
                self.last_errors.append(f"ddgs:{backend}:{exc.__class__.__name__}")
                time.sleep(0.5) # Increased backoff
                continue

        return []

    def _build_query_variants(self, query: str) -> list[str]:
        # Fix 4: Reduced query variants to avoid crushing the search layer
        base = query.strip().lower()
        variants = []
        
        if "pricing" not in base and "price" not in base:
            variants.append(f"{query} pricing")
        if "review" not in base and "reviews" not in base:
            variants.append(f"{query} reviews")
            
        return variants

    def _search_via_html(self, query: str, max_results: int) -> list[WebResult]:
        search_url = f"https://duckduckgo.com/html/?{urlencode({'q': query})}"
        request = Request(
            search_url,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
                )
            },
        )

        try:
            with urlopen(request, timeout=self._timeout_seconds) as response:
                raw = response.read(self._max_bytes)
                encoding = response.headers.get_content_charset() or "utf-8"
        except (URLError, TimeoutError, ValueError, OSError) as exc:
            self.last_errors.append(f"ddg_html_error:{exc.__class__.__name__}")
            return []

        try:
            html_text = raw.decode(encoding, errors="ignore")
        except LookupError:
            html_text = raw.decode("utf-8", errors="ignore")

        matches = list(
            re.finditer(
                r'<a[^>]*class="result__a"[^>]*href="([^"]+)"[^>]*>(.*?)</a>',
                html_text,
                flags=re.I | re.S,
            )
        )
        if not matches:
            self.last_errors.append("ddg_html_no_matches")
            return []

        results: list[WebResult] = []
        seen_urls: set[str] = set()

        for match in matches:
            href = match.group(1).strip()
            title_html = match.group(2).strip()
            resolved = self._resolve_result_url(href)
            if not resolved or not resolved.startswith("http"):
                continue
            
            # Fix 1: Canonicalize final URL (strip query params that are just tracking)
            parsed = urlsplit(resolved)
            canonical_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
            
            if canonical_url in seen_urls:
                continue
            seen_urls.add(canonical_url)

            title = _normalize_whitespace(_strip_html(title_html)) or "Search result"
            snippet_window = html_text[match.end() : match.end() + 600]
            snippet = _first_match(
                r'<a[^>]*class="result__snippet"[^>]*>(.*?)</a>',
                snippet_window,
                flags=re.I | re.S,
            )
            if not snippet:
                snippet = _first_match(
                    r'<div[^>]*class="result__snippet"[^>]*>(.*?)</div>',
                    snippet_window,
                    flags=re.I | re.S,
                )
            cleaned_snippet = _normalize_whitespace(_strip_html(snippet))

            results.append(
                WebResult(
                    title=title,
                    url=canonical_url,
                    snippet=cleaned_snippet or title,
                    query_label=query,
                )
            )
            if len(results) >= max_results:
                break

        if not results:
            self.last_errors.append("ddg_html_no_usable_results")

        return results

    def _resolve_result_url(self, href: str) -> str:
        # Fix 1: unwrap redirect properly
        if "/l/?" in href:
            try:
                parsed = urlsplit(href)
                params = parse_qs(parsed.query)
                target = params.get("uddg")
                if target and target[0]:
                    return target[0]
            except Exception:
                pass

        if href.startswith("//"):
            return f"https:{href}"

        if href.startswith("http://") or href.startswith("https://"):
            return href

        return ""


class HtmlPageFetcher:
    """Lightweight page fetcher that extracts page title, description, and visible text."""

    def __init__(self, timeout_seconds: float = 4.0, max_bytes: int = 250_000) -> None:
        self._timeout_seconds = timeout_seconds
        self._max_bytes = max_bytes

    def fetch(self, url: str) -> PageSummary | None:
        request = Request(
            url,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
                )
            },
        )

        try:
            with urlopen(request, timeout=self._timeout_seconds) as response:
                content_type = response.headers.get_content_type()
                if content_type and content_type not in {"text/html", "application/xhtml+xml"}:
                    return None
                raw = response.read(self._max_bytes)
                encoding = response.headers.get_content_charset() or "utf-8"
        except (URLError, TimeoutError, ValueError, OSError):
            return None

        try:
            html_text = raw.decode(encoding, errors="ignore")
        except LookupError:
            html_text = raw.decode("utf-8", errors="ignore")

        title = _first_match(r"<title[^>]*>(.*?)</title>", html_text, flags=re.I | re.S)
        description = _first_match(
            r'<meta[^>]+name=["\']description["\'][^>]+content=["\'](.*?)["\']',
            html_text,
            flags=re.I | re.S,
        ) or _first_match(
            r'<meta[^>]+property=["\']og:description["\'][^>]+content=["\'](.*?)["\']',
            html_text,
            flags=re.I | re.S,
        )
        excerpt = _html_to_excerpt(html_text)

        cleaned_title = _normalize_whitespace(_strip_html(title))
        cleaned_description = _normalize_whitespace(_strip_html(description))
        cleaned_excerpt = _normalize_whitespace(excerpt)

        if not (cleaned_title or cleaned_description or cleaned_excerpt):
            return None

        return PageSummary(
            url=url,
            title=cleaned_title,
            description=cleaned_description,
            excerpt=cleaned_excerpt,
        )


GENERIC_ENTITY_NAMES = {
    "home",
    "about",
    "blog",
    "menu",
    "pricing",
    "review",
    "reviews",
    "reddit",
    "linkedin",
    "facebook",
    "instagram",
    "youtube",
    "directory",
    "search",
    "results",
    "result",
    "duckduckgo",
    "google",
    "bing",
    "yelp",
    "tripadvisor",
    "trustpilot",
    "g2",
    "capterra",
    "quora",
    "x",
    "twitter",
}

GENERIC_ENTITY_STOPWORDS = {
    "official",
    "homepage",
    "site",
    "website",
    "best",
    "top",
    "list",
    "directory",
    "market",
    "option",
    "options",
    "listing",
    "rankings",
    "review",
    "reviews",
    "pricing",
    "prices",
    "complaint",
    "complaints",
    "thread",
}

COMPLAINT_THEME_KEYWORDS: dict[str, tuple[str, ...]] = {
    "late_delivery": ("late", "delay", "slow", "not on time"),
    "high_price": ("expensive", "overpriced", "too costly", "pricey"),
    "minimum_order": ("minimum order", "order minimum", "min order"),
    "inconsistent_quality": ("inconsistent", "dry", "cold", "bland", "quality issue"),
    "poor_communication": ("no response", "unresponsive", "communication", "did not reply"),
}

PRAISE_THEME_KEYWORDS: dict[str, tuple[str, ...]] = {
    "food_quality": ("delicious", "tender", "juicy", "great taste", "excellent food"),
    "professional_service": ("friendly", "professional", "easy to work", "helpful"),
    "on_time": ("on time", "punctual", "timely", "arrived early"),
    "value_for_money": ("good value", "worth it", "fair price"),
}

EVENT_TYPE_KEYWORDS: tuple[str, ...] = (
    "wedding",
    "corporate",
    "office lunch",
    "private party",
    "birthday",
    "festival",
    "graduation",
    "backyard",
)


class MarketResearchService:
    def __init__(
        self,
        searcher: WebSearcher | None = None,
        fetcher: PageFetcher | None = None,
        llm_client: OllamaClient | None = None,
    ) -> None:
        self._searcher = searcher or DuckDuckGoSearcher()
        self._fetcher = fetcher or HtmlPageFetcher()
        self._llm_client = llm_client or OllamaClient()

    def build_validation_request(self, payload: MarketSearchRequest) -> ValidationRequest:
        query_plan = self._build_query_plan(payload)
        collection_plan = self._build_collection_plan(payload)
        mission = self._run_research_mission(payload, query_plan, collection_plan)

        has_fetched_evidence = bool(mission.page_summaries) or int(mission.diagnostics.get("fetch_success", 0)) > 0
        llm_context = None
        if has_fetched_evidence:
            llm_context = self._synthesize_context_with_llm(
                payload,
                mission.web_results,
                mission.page_summaries,
                mission.structured_evidence,
            )

        llm_facts = self._llm_extract_structured_facts(payload, mission.raw_sources)
        structured_evidence = self._merge_structured_evidence(
            mission.structured_evidence,
            llm_facts,
            limit=int(collection_plan["target_structured_evidence"]),
        )

        evidence_inputs = self._structured_to_evidence_inputs(structured_evidence, mission.raw_sources)
        evidence_inputs = self._ensure_minimum_evidence(
            payload,
            evidence_inputs,
            mission.web_results,
            mission.page_summaries,
        )

        competitors = self._extract_competitors(
            payload.market,
            mission.web_results,
            mission.page_summaries,
            llm_context,
            structured_evidence,
        )

        target_customer = (
            payload.target_customer
            or self._llm_target_customer(llm_context)
            or self._infer_target_customer(
                payload,
                mission.web_results,
                mission.page_summaries,
                structured_evidence,
            )
        )

        llm_assumptions = self._llm_assumptions(llm_context)
        assumptions = payload.assumptions or llm_assumptions or self._derive_assumptions_from_evidence(
            structured_evidence,
            payload.market,
        )
        if not assumptions:
            assumptions = [
                f"There is active demand for {payload.market}.",
                "Buyers have enough pain or desire to pay for better alternatives.",
            ]

        business_model = self._llm_business_model(llm_context) or self._infer_business_model(
            payload.market,
            mission.web_results,
            mission.page_summaries,
        )

        return ValidationRequest(
            idea=f"Business focused on {payload.market}",
            target_customer=target_customer,
            geography=payload.geography,
            business_model=business_model,
            competitors=competitors,
            pricing_guess=payload.pricing_guess,
            assumptions=assumptions,
            constraints=payload.constraints,
            profile=payload.profile,
            template=payload.template,
            evidence_inputs=evidence_inputs,
            raw_sources=mission.raw_sources[:30],
            structured_evidence=structured_evidence[:120],
            research_diagnostics=mission.diagnostics,
        )

    def _run_research_mission(
        self,
        payload: MarketSearchRequest,
        query_plan: list[tuple[str, str]],
        collection_plan: dict[str, int | float],
    ) -> ResearchMissionOutput:
        all_results: list[WebResult] = []
        all_pages: dict[str, PageSummary] = {}
        seen_urls: set[str] = set()
        round_queries = query_plan
        diagnostics: dict[str, Any] = {
            "queries_planned": len(query_plan),
            "queries_attempted": 0,
            "queries_with_results": 0,
            "search_errors": [],
            "rounds_executed": 0,
            "fetch_attempted": 0,
            "fetch_success": 0,
        }

        max_rounds = int(collection_plan["max_rounds"])
        round_limit = int(collection_plan["round_limit"])
        max_passes = int(collection_plan["max_passes_per_round"])
        backoff_seconds = float(collection_plan["backoff_seconds"])

        for round_index in range(max_rounds):
            round_plan: dict[str, int | float] = {
                "max_search_results": round_limit,
                "max_queries": len(round_queries),
                "max_passes": max_passes,
                "backoff_seconds": backoff_seconds,
            }
            round_results, round_meta = self._collect_web_results(round_queries, round_plan)
            diagnostics["queries_attempted"] = int(diagnostics["queries_attempted"]) + int(round_meta["queries_attempted"])
            diagnostics["queries_with_results"] = int(diagnostics["queries_with_results"]) + int(round_meta["queries_with_results"])
            diagnostics["rounds_executed"] = int(diagnostics["rounds_executed"]) + 1

            search_errors = diagnostics.get("search_errors")
            if isinstance(search_errors, list):
                for error in round_meta.get("search_errors", []):
                    if isinstance(error, str) and error not in search_errors:
                        search_errors.append(error)

            newly_added: list[WebResult] = []
            for item in round_results:
                if item.url in seen_urls:
                    continue
                seen_urls.add(item.url)
                all_results.append(item)
                newly_added.append(item)

            fetch_plan: dict[str, int | float] = {
                "max_fetches": float(min(
                    int(collection_plan["max_fetches_per_round"]),
                    len(newly_added),
                ))
            }
            diagnostics["fetch_attempted"] = int(diagnostics["fetch_attempted"]) + int(fetch_plan["max_fetches"])
            fetched_pages = self._fetch_page_summaries(newly_added, fetch_plan)
            diagnostics["fetch_success"] = int(diagnostics["fetch_success"]) + len(fetched_pages)
            all_pages.update(fetched_pages)

            raw_sources = self._build_raw_sources(all_results, all_pages)
            structured = self._extract_structured_evidence(raw_sources, payload.market)
            coverage = self._coverage_snapshot(raw_sources, structured)

            if self._coverage_complete(coverage, collection_plan):
                break

            round_queries = self._build_gap_queries(payload, coverage, round_index)
            if not round_queries:
                break

            if round_index < max_rounds - 1:
                time.sleep(backoff_seconds * (round_index + 1))

        final_raw_sources = self._build_raw_sources(all_results, all_pages)
        final_structured = self._extract_structured_evidence(final_raw_sources, payload.market)
        diagnostics["raw_source_count"] = len(final_raw_sources)
        diagnostics["fetched_source_count"] = sum(1 for source in final_raw_sources if source.fetched)
        diagnostics["snippet_source_count"] = sum(1 for source in final_raw_sources if not source.fetched)
        diagnostics["structured_evidence_count"] = len(final_structured)
        diagnostics["coverage"] = self._coverage_snapshot(final_raw_sources, final_structured)

        if len(final_raw_sources) == 0 and int(diagnostics.get("queries_attempted", 0)) > 0:
            diagnostics["status"] = "external_search_failed"
        elif len(final_raw_sources) > 0:
            diagnostics["status"] = "external_sources_collected"
        else:
            diagnostics["status"] = "no_queries_executed"

        return ResearchMissionOutput(
            web_results=all_results,
            page_summaries=all_pages,
            raw_sources=final_raw_sources,
            structured_evidence=final_structured,
            diagnostics=diagnostics,
        )

    def _build_collection_plan(self, payload: MarketSearchRequest) -> dict[str, int | float]:
        is_deep = payload.research_mode == "deep"
        round_limit = max(8, payload.max_search_results)
        target_raw_sources = min(30, max(10, round_limit * (2 if is_deep else 1)))
        minimum_rows = payload.minimum_evidence_rows if is_deep else max(6, min(payload.minimum_evidence_rows, 10))
        target_structured = min(100, max(20, target_raw_sources * 3))

        return {
            "round_limit": round_limit,
            "max_fetches_per_round": 12 if is_deep else 7,
            "max_passes_per_round": 2 if is_deep else 1,
            "max_rounds": 4 if is_deep else 3,
            "backoff_seconds": 0.45 if is_deep else 0.2,
            "minimum_evidence_rows": minimum_rows,
            "target_raw_sources": target_raw_sources,
            "target_structured_evidence": target_structured,
        }

    def _build_query_plan(self, payload: MarketSearchRequest) -> list[tuple[str, str]]:
        market = payload.market.strip()
        geography = payload.geography.strip()
        deep_mode = payload.research_mode == "deep"

        # Fix 4: Reduced query plan to avoid rate limits
        queries: list[tuple[str, str]] = [
            ("core", market),
            ("competitors", f"{market} competitors"),
            ("pricing", f"{market} pricing"),
            ("reviews", f"{market} reviews"),
        ]

        if geography and geography.lower() not in {"global", "worldwide"}:
            queries.insert(1, ("geo", f"{market} {geography}"))

        if deep_mode:
            queries.extend(
                [
                    ("directory", f"{market} directory listings"),
                    ("trends", f"{market} trends growth"),
                ]
            )

        # Only add LLM expansions in deep mode to save on requests
        if deep_mode:
            queries.extend(self._llm_generate_query_expansions(payload))

        deduped: list[tuple[str, str]] = []
        seen: set[str] = set()
        for label, query in queries:
            normalized = query.lower().strip()
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            deduped.append((label, query))

        return deduped[:12] # Reduced from 20 to 12

    def _llm_generate_query_expansions(self, payload: MarketSearchRequest) -> list[tuple[str, str]]:
        if not self._llm_client.enabled:
            return []

        system_prompt = (
            "Generate additional web research queries for market validation. Return strict JSON: "
            "{\"queries\": [{\"label\": \"string\", \"query\": \"string\"}]}. "
            "Keep labels short and queries highly specific to pricing, reviews, directories, and trends."
        )
        user_prompt = json.dumps(
            {
                "market": payload.market,
                "geography": payload.geography,
                "profile": payload.profile,
                "template": payload.template,
                "research_mode": payload.research_mode,
            },
            ensure_ascii=True,
        )

        data = self._llm_client.chat_json(system_prompt=system_prompt, user_prompt=user_prompt)
        if not isinstance(data, dict):
            return []

        raw_queries = data.get("queries")
        if not isinstance(raw_queries, list):
            return []

        expansions: list[tuple[str, str]] = []
        for item in raw_queries:
            if not isinstance(item, dict):
                continue
            label = str(item.get("label", "llm_query")).strip().lower().replace(" ", "_")
            query = str(item.get("query", "")).strip()
            if not query:
                continue
            if len(query) < 8:
                continue
            expansions.append((label[:28] or "llm_query", query[:140]))
            if len(expansions) >= 6:
                break

        return expansions

    def _collect_web_results(
        self,
        query_plan: list[tuple[str, str]],
        collection_plan: dict[str, int | float],
    ) -> tuple[list[WebResult], dict[str, Any]]:
        total_limit = int(collection_plan["max_search_results"])
        max_queries = int(collection_plan["max_queries"])
        per_query_limit = max(2, min(5, total_limit // max(1, len(query_plan) // 2)))
        max_passes = int(collection_plan["max_passes"])
        backoff_seconds = float(collection_plan["backoff_seconds"])
        results: list[WebResult] = []
        seen_urls: set[str] = set()
        queries_attempted = 0
        queries_with_results = 0
        search_errors: list[str] = []

        for pass_index in range(max_passes):
            for query_index, (label, query) in enumerate(query_plan[:max_queries]):
                if len(results) >= total_limit:
                    return results, {
                        "queries_attempted": queries_attempted,
                        "queries_with_results": queries_with_results,
                        "search_errors": search_errors,
                    }

                queries_attempted += 1

                try:
                    query_results = self._searcher.search(query=query, max_results=per_query_limit)
                    if query_results:
                        queries_with_results += 1
                except Exception as exc:
                    search_errors.append(f"{label}:{exc.__class__.__name__}")
                    query_results = []

                candidate_errors = getattr(self._searcher, "last_errors", None)
                if isinstance(candidate_errors, list):
                    for error in candidate_errors:
                        if isinstance(error, str):
                            composed = f"{label}:{error}"
                            if composed not in search_errors:
                                search_errors.append(composed)

                for item in query_results:
                    if item.url in seen_urls:
                        continue
                    seen_urls.add(item.url)
                    results.append(
                        WebResult(
                            title=item.title,
                            url=item.url,
                            snippet=item.snippet,
                            query_label=label,
                        )
                    )
                    if len(results) >= total_limit:
                        return results, {
                            "queries_attempted": queries_attempted,
                            "queries_with_results": queries_with_results,
                            "search_errors": search_errors,
                        }

                if pass_index == 0 and query_index >= max(4, len(query_plan) // 2) and len(results) >= max(4, total_limit // 3):
                    break

            if len(results) >= total_limit:
                return results, {
                    "queries_attempted": queries_attempted,
                    "queries_with_results": queries_with_results,
                    "search_errors": search_errors,
                }
            if pass_index < max_passes - 1:
                time.sleep(backoff_seconds * (pass_index + 1))

        return results, {
            "queries_attempted": queries_attempted,
            "queries_with_results": queries_with_results,
            "search_errors": search_errors,
        }

    def _fetch_page_summaries(
        self,
        results: list[WebResult],
        collection_plan: dict[str, int | float],
    ) -> dict[str, PageSummary]:
        summaries: dict[str, PageSummary] = {}
        fetch_targets = results[: min(int(collection_plan["max_fetches"]), len(results))]

        for result in fetch_targets:
            try:
                page_summary = self._fetcher.fetch(result.url)
            except Exception:
                page_summary = None
            if page_summary is None:
                continue
            summaries[result.url] = page_summary

        return summaries

    def _build_gap_queries(
        self,
        payload: MarketSearchRequest,
        coverage: dict[str, int],
        round_index: int,
    ) -> list[tuple[str, str]]:
        market = payload.market.strip()
        geography = payload.geography.strip()
        queries: list[tuple[str, str]] = []

        if coverage.get("competitor_sources", 0) < 3:
            queries.extend(
                [
                    ("competitor_gap", f"{market} top competitors"),
                    ("directory_gap", f"{market} local directory"),
                ]
            )

        if coverage.get("pricing_sources", 0) < 2:
            queries.extend(
                [
                    ("pricing_gap", f"{market} menu price"),
                    ("pricing_gap_alt", f"{market} package pricing"),
                    ("min_order_gap", f"{market} minimum order requirement"),
                ]
            )

        if coverage.get("review_sources", 0) < 2:
            queries.extend(
                [
                    ("reviews_gap", f"{market} customer reviews"),
                    ("forums_gap", f"{market} reddit complaints"),
                ]
            )

        if coverage.get("raw_sources", 0) < 10:
            queries.extend(
                [
                    ("editorial_gap", f"best {market} list"),
                    ("public_data_gap", f"{market} public statistics"),
                ]
            )

        if geography and geography.lower() not in {"global", "worldwide"} and round_index <= 1:
            queries.append(("geo_gap", f"{market} {geography}"))

        deduped: list[tuple[str, str]] = []
        seen: set[str] = set()
        for label, query in queries:
            normalized = query.lower()
            if normalized in seen:
                continue
            seen.add(normalized)
            deduped.append((label, query))

        return deduped[:10]

    def _coverage_snapshot(
        self,
        raw_sources: list[RawSourceRecord],
        structured: list[StructuredEvidenceItem],
    ) -> dict[str, int]:
        competitor_sources = {
            source.source_url
            for source in raw_sources
            if source.source_type in {"company_website", "directory_listing", "local_editorial"}
        }
        pricing_sources = {
            source.source_url
            for source in raw_sources
            if source.source_type == "pricing_page"
        }
        review_sources = {
            source.source_url
            for source in raw_sources
            if source.source_type in {"review_site", "forum_social", "customer_complaint"}
        }

        return {
            "raw_sources": len(raw_sources),
            "competitor_sources": len(competitor_sources),
            "pricing_sources": len(pricing_sources),
            "review_sources": len(review_sources),
            "structured_evidence": len(structured),
        }

    def _coverage_complete(
        self,
        coverage: dict[str, int],
        collection_plan: dict[str, int | float],
    ) -> bool:
        return (
            coverage.get("raw_sources", 0) >= int(collection_plan["target_raw_sources"])
            and coverage.get("competitor_sources", 0) >= 3
            and coverage.get("pricing_sources", 0) >= 2
            and coverage.get("review_sources", 0) >= 2
            and coverage.get("structured_evidence", 0) >= int(collection_plan["target_structured_evidence"] // 2)
        )

    def _build_raw_sources(
        self,
        results: list[WebResult],
        page_summaries: dict[str, PageSummary],
    ) -> list[RawSourceRecord]:
        raw_sources: list[RawSourceRecord] = []

        for index, result in enumerate(results, start=1):
            page_summary = page_summaries.get(result.url)
            source_type = self._infer_source_type(result, page_summary)
            snippet = _normalize_whitespace(result.snippet)
            cleaned_text_parts = [result.title]
            if page_summary is not None:
                cleaned_text_parts.extend(
                    [
                        page_summary.title,
                        page_summary.description,
                        page_summary.excerpt,
                    ]
                )
            else:
                cleaned_text_parts.append(snippet)
            cleaned_text = _normalize_whitespace(" ".join(part for part in cleaned_text_parts if part))

            raw_sources.append(
                RawSourceRecord(
                    id=f"S{index}",
                    query_label=result.query_label,
                    source_type=source_type,
                    source_title=result.title,
                    source_url=result.url,
                    snippet=snippet,
                    cleaned_text=cleaned_text[:2000],
                    fetched=page_summary is not None,
                    trust_weight=self._source_trust_weight(source_type),
                )
            )

        return raw_sources

    def _extract_structured_evidence(
        self,
        raw_sources: list[RawSourceRecord],
        market: str,
    ) -> list[StructuredEvidenceItem]:
        extracted: list[StructuredEvidenceItem] = []
        seen_keys: set[tuple[str, str, str, str]] = set()
        confidence_caps = {
            "fetched_page": 0.92,
            "search_snippet": 0.58,
            "direct_source": 0.82,
            "unknown": 0.62,
        }

        def add_fact(
            source: RawSourceRecord,
            entity: str,
            fact_type: str,
            value: str,
            excerpt: str,
            confidence_boost: float = 0.0,
        ) -> None:
            normalized_entity = _normalize_whitespace(entity) or "Unknown entity"
            normalized_value = self._normalize_fact_value(value)
            if not normalized_value:
                return
            key = (
                normalized_entity.lower(),
                fact_type.lower(),
                normalized_value.lower(),
                (source.source_url or "").lower(),
            )
            if key in seen_keys:
                return
            seen_keys.add(key)
            evidence_basis = self._source_evidence_basis(source)
            confidence_cap = confidence_caps.get(evidence_basis, 0.62)
            confidence = min(_clamp01(source.trust_weight + confidence_boost), confidence_cap)
            extracted.append(
                StructuredEvidenceItem(
                    id=f"F{len(extracted) + 1}",
                    source_id=source.id,
                    source_type=source.source_type,
                    entity=normalized_entity[:80],
                    fact_type=fact_type,
                    value=normalized_value[:140],
                    excerpt=_normalize_whitespace(excerpt)[:240],
                    url=source.source_url,
                    confidence=round(confidence, 3),
                    evidence_basis=evidence_basis,
                )
            )

        for source in raw_sources:
            text = source.cleaned_text if source.fetched else _normalize_whitespace(f"{source.source_title} {source.snippet}")
            lower_text = text.lower()
            entity = self._infer_entity_from_source(source, market)
            source_is_fetched = source.fetched

            if source.source_type in {
                "company_website",
                "directory_listing",
                "local_editorial",
            }:
                positioning = self._infer_positioning(lower_text)
                if positioning == "general":
                    positioning = ""
                add_fact(
                    source,
                    entity,
                    "competitor_positioning",
                    positioning,
                    text,
                    confidence_boost=0.05 if source_is_fetched else 0.0,
                )

            for price_match in self._find_prices(text):
                value_lower = price_match.lower()
                fact_type = "price_per_head" if any(token in value_lower for token in ("person", "head", "pp", "plate")) else "price_point"
                add_fact(
                    source,
                    entity,
                    fact_type,
                    price_match,
                    text,
                    confidence_boost=0.1,
                )

            min_order = self._find_minimum_order(lower_text)
            if min_order:
                add_fact(
                    source,
                    entity,
                    "minimum_order_size",
                    min_order,
                    text,
                    confidence_boost=0.08,
                )

            delivery_fee = self._find_delivery_fee(lower_text)
            if delivery_fee:
                add_fact(
                    source,
                    entity,
                    "delivery_fee",
                    delivery_fee,
                    text,
                    confidence_boost=0.08,
                )

            booking_lead = self._find_booking_lead(lower_text)
            if booking_lead:
                add_fact(
                    source,
                    entity,
                    "booking_lead_time",
                    booking_lead,
                    text,
                    confidence_boost=0.08,
                )

            complaint_themes = self._match_themes(lower_text, COMPLAINT_THEME_KEYWORDS)
            for theme in complaint_themes:
                add_fact(
                    source,
                    entity,
                    "review_complaint_theme",
                    theme,
                    text,
                    confidence_boost=0.06,
                )

            praise_themes = self._match_themes(lower_text, PRAISE_THEME_KEYWORDS)
            for theme in praise_themes:
                add_fact(
                    source,
                    entity,
                    "review_praise_theme",
                    theme,
                    text,
                    confidence_boost=0.06,
                )

            for event_type in EVENT_TYPE_KEYWORDS:
                if event_type in lower_text:
                    add_fact(
                        source,
                        entity,
                        "event_type_served",
                        event_type,
                        text,
                        confidence_boost=0.04,
                    )

            if source.source_type in {"trend_signal", "market_report", "public_data"} and any(
                token in lower_text for token in ("demand", "growth", "increasing", "uptrend")
            ):
                add_fact(
                    source,
                    entity,
                    "demand_signal",
                    "rising_demand",
                    text,
                    confidence_boost=0.08,
                )

            if source.source_type == "job_post" and any(
                token in lower_text for token in ("hiring", "open role", "apply", "positions")
            ):
                add_fact(
                    source,
                    entity,
                    "hiring_signal",
                    "active_hiring",
                    text,
                    confidence_boost=0.06,
                )

            market_size_mentions = self._find_market_size(lower_text)
            for size in market_size_mentions:
                add_fact(
                    source,
                    entity,
                    "market_size",
                    size,
                    text,
                    confidence_boost=0.1,
                )

            growth_rates = self._find_growth_rate(lower_text)
            for rate in growth_rates:
                add_fact(
                    source,
                    entity,
                    "growth_rate",
                    rate,
                    text,
                    confidence_boost=0.1,
                )

            if source_is_fetched or len(source.snippet) >= 40:
                add_fact(
                    source,
                    entity,
                    "market_signal",
                    source.source_type,
                    source.snippet or source.cleaned_text,
                    confidence_boost=0.0,
                )

        return extracted[:140]

    def _llm_extract_structured_facts(
        self,
        payload: MarketSearchRequest,
        raw_sources: list[RawSourceRecord],
    ) -> list[StructuredEvidenceItem]:
        fetched_sources = [source for source in raw_sources if source.fetched]
        if not self._llm_client.enabled or not fetched_sources:
            return []

        compact_sources = [
            {
                "source_id": source.id,
                "source_type": source.source_type,
                "title": source.source_title,
                "url": source.source_url,
                "text": source.cleaned_text[:320],
            }
            for source in fetched_sources[:12]
        ]

        system_prompt = (
            "Extract structured market facts from source snippets. Return strict JSON: "
            "{\"facts\": [{\"source_id\": \"S1\", \"entity\": \"...\", "
            "\"fact_type\": \"...\", \"value\": \"...\", \"excerpt\": \"...\", "
            "\"confidence\": 0.0}]}"
        )
        user_prompt = json.dumps(
            {
                "market": payload.market,
                "geography": payload.geography,
                "sources": compact_sources,
            },
            ensure_ascii=True,
        )

        llm_payload = self._llm_client.chat_json(system_prompt=system_prompt, user_prompt=user_prompt)
        if not isinstance(llm_payload, dict):
            return []

        raw_facts = llm_payload.get("facts")
        if not isinstance(raw_facts, list):
            return []

        source_lookup = {item.id: item for item in fetched_sources}
        llm_facts: list[StructuredEvidenceItem] = []

        for item in raw_facts:
            if not isinstance(item, dict):
                continue

            source_id = str(item.get("source_id", "")).strip()
            source = source_lookup.get(source_id)
            if source is None:
                continue

            entity = _normalize_whitespace(str(item.get("entity", ""))) or self._infer_entity_from_source(source, payload.market)
            fact_type = _normalize_whitespace(str(item.get("fact_type", ""))).lower().replace(" ", "_")
            value = self._normalize_fact_value(str(item.get("value", "")))
            excerpt = _normalize_whitespace(str(item.get("excerpt", ""))) or source.snippet

            if not (fact_type and value):
                continue

            try:
                confidence_raw = float(item.get("confidence", source.trust_weight))
            except (TypeError, ValueError):
                confidence_raw = source.trust_weight

            llm_facts.append(
                StructuredEvidenceItem(
                    id=f"L{len(llm_facts) + 1}",
                    source_id=source.id,
                    source_type=source.source_type,
                    entity=entity[:80],
                    fact_type=fact_type[:40],
                    value=value[:140],
                    excerpt=excerpt[:240],
                    url=source.source_url,
                    confidence=round(min(_clamp01(confidence_raw), 0.9), 3),
                    evidence_basis=self._source_evidence_basis(source),
                )
            )
            if len(llm_facts) >= 50:
                break

        return llm_facts

    def _merge_structured_evidence(
        self,
        deterministic_facts: list[StructuredEvidenceItem],
        llm_facts: list[StructuredEvidenceItem],
        limit: int,
    ) -> list[StructuredEvidenceItem]:
        merged: list[StructuredEvidenceItem] = []
        seen: set[tuple[str, str, str, str]] = set()

        for item in [*deterministic_facts, *llm_facts]:
            key = (
                item.entity.lower(),
                item.fact_type.lower(),
                item.value.lower(),
                (item.url or "").lower(),
            )
            if key in seen:
                continue
            seen.add(lowered := key) # actually I just need the key
            merged.append(item)
            if len(merged) >= limit:
                break

        renumbered: list[StructuredEvidenceItem] = []
        for index, item in enumerate(merged, start=1):
            renumbered.append(
                StructuredEvidenceItem(
                    id=f"F{index}",
                    source_id=item.source_id,
                    source_type=item.source_type,
                    entity=item.entity,
                    fact_type=item.fact_type,
                    value=item.value,
                    excerpt=item.excerpt,
                    url=item.url,
                    confidence=item.confidence,
                    evidence_basis=item.evidence_basis,
                )
            )

        return renumbered

    def _structured_to_evidence_inputs(
        self,
        structured_evidence: list[StructuredEvidenceItem],
        raw_sources: list[RawSourceRecord],
    ) -> list[EvidenceInput]:
        source_lookup = {item.id: item for item in raw_sources}
        evidence_inputs: list[EvidenceInput] = []

        for item in structured_evidence:
            source = source_lookup.get(item.source_id)
            source_title = source.source_title if source is not None else f"{item.entity} source"
            observed_fact = _normalize_whitespace(
                f"{item.entity} | {item.fact_type}: {item.value}. Evidence: {item.excerpt}"
            )

            confidence = item.confidence
            if item.evidence_basis == "search_snippet":
                confidence = min(confidence, 0.58)
            if confidence >= 0.78:
                strength = "high"
            elif confidence >= 0.55:
                strength = "medium"
            else:
                strength = "low"

            evidence_inputs.append(
                EvidenceInput(
                    source_type=item.source_type,
                    source_title=source_title[:140],
                    source_url=item.url,
                    observed_fact=observed_fact[:400],
                    strength=strength,
                    evidence_basis=item.evidence_basis,
                )
            )

        return evidence_inputs

    def _ensure_minimum_evidence(
        self,
        payload: MarketSearchRequest,
        evidence_inputs: list[EvidenceInput],
        web_results: list[WebResult],
        page_summaries: dict[str, PageSummary],
    ) -> list[EvidenceInput]:
        threshold = payload.minimum_evidence_rows if payload.research_mode == "deep" else max(6, min(payload.minimum_evidence_rows, 10))
        if len(evidence_inputs) >= threshold:
            return evidence_inputs

        supplemental: list[EvidenceInput] = []
        has_fetched_pages = bool(page_summaries)
        snippet_supplements = 0
        for result in web_results:
            if len(evidence_inputs) + len(supplemental) >= threshold:
                break
            page_summary = page_summaries.get(result.url)
            if page_summary is None and not has_fetched_pages:
                continue
            if page_summary is None and snippet_supplements >= 2:
                continue
            observed = self._compose_observed_fact(result, page_summary)
            evidence_basis = "fetched_page" if page_summary is not None else "search_snippet"
            if evidence_basis == "search_snippet":
                snippet_supplements += 1
            supplemental.append(
                EvidenceInput(
                    source_type=self._infer_source_type(result, page_summary),
                    source_title=f"Supplemental web signal: {result.title}",
                    source_url=result.url,
                    observed_fact=observed[:400],
                    strength="medium" if page_summary is not None else "low",
                    evidence_basis=evidence_basis,
                )
            )

        return evidence_inputs + supplemental

    def _source_trust_weight(self, source_type: str) -> float:
        return {
            "market_report": 0.9,
            "public_data": 0.9,
            "pricing_page": 0.85,
            "review_site": 0.8,
            "directory_listing": 0.75,
            "local_editorial": 0.72,
            "customer_complaint": 0.7,
            "trend_signal": 0.7,
            "forum_social": 0.65,
            "job_post": 0.65,
            "company_website": 0.6,
        }.get(source_type, 0.55)

    def _source_evidence_basis(self, source: RawSourceRecord) -> EvidenceBasis:
        return "fetched_page" if source.fetched else "search_snippet"

    def _clean_entity_candidate(self, candidate: str) -> str:
        # Fix 2: Stronger entity normalization
        if not candidate:
            return ""
        
        # Strip common search-engine and aggregator noise
        noise_patterns = [
            r"\|.*", r"\-.*", r":.*", 
            r"\b(yelp|tripadvisor|g2|capterra|trustpilot|reddit|facebook|instagram|linkedin|twitter|youtube|duckduckgo|google|bing)\b.*",
            r"\b(best|top|affordable|official|website|homepage|site|directory|listing|reviews?|pricing|prices?|competitors?|alternatives?)\b"
        ]
        
        cleaned = candidate
        for pattern in noise_patterns:
            cleaned = re.sub(pattern, " ", cleaned, flags=re.I).strip()
            
        cleaned = _normalize_whitespace(re.sub(r"[^A-Za-z0-9&+\- ]", " ", cleaned)).strip("- ")
        
        if len(cleaned) < 2:
            return ""
        
        tokens = [token for token in re.split(r"\s+", cleaned.lower()) if token]
        if not tokens or len(tokens) > 5:
            return ""

        meaningful = [
            token
            for token in tokens
            if token not in GENERIC_ENTITY_STOPWORDS
        ]
        if not meaningful:
            return ""
        if all(token in GENERIC_ENTITY_NAMES for token in meaningful):
            return ""

        return cleaned

    def _entity_allowed(self, candidate: str, market: str) -> bool:
        cleaned = self._clean_entity_candidate(candidate)
        if not cleaned:
            return False

        lowered = cleaned.lower()
        if lowered in GENERIC_ENTITY_NAMES:
            return False

        # Don't allow the market name itself to be an entity
        candidate_tokens = {token for token in re.findall(r"[a-z0-9]+", lowered)}
        market_tokens = {token for token in re.findall(r"[a-z0-9]+", market.lower())}
        
        if not candidate_tokens:
            return False
            
        # If candidate is a subset of market or vice versa, it's likely too generic
        if candidate_tokens.issubset(market_tokens) or market_tokens.issubset(candidate_tokens):
            return False

        return True

    def _canonicalize_company_name(self, title: str, url: str) -> str:
        # Fix 2: Canonicalize company name from title and URL
        host = urlparse(url).netloc.replace("www.", "").strip().lower()
        host_token = host.split(".")[0].replace("-", " ")
        
        # Prefer a cleaned version of the title if it starts with the host token
        title_first_part = re.split(r"[|:\-]", title)[0].strip()
        if host_token in title_first_part.lower() or title_first_part.lower() in host_token:
            cleaned = self._clean_entity_candidate(title_first_part)
            if cleaned:
                return cleaned.title()
                
        cleaned_host = self._clean_entity_candidate(host_token)
        if cleaned_host:
            return cleaned_host.title()
            
        return ""

    def _infer_entity_from_source(self, source: RawSourceRecord, market: str) -> str:
        # Fix 1 & 2: Use canonicalization
        canonical = self._canonicalize_company_name(source.source_title, source.source_url)
        if canonical and self._entity_allowed(canonical, market):
            return canonical

        return f"{market.title()} signal"

    def _infer_positioning(self, text: str) -> str:
        if any(word in text for word in ("premium", "high-end", "luxury", "exclusive")):
            return "premium"
        if any(word in text for word in ("budget", "affordable", "value", "cheap")):
            return "budget"
        if any(word in text for word in ("fast", "same-day", "quick", "rapid")):
            return "speed-focused"
        if any(word in text for word in ("local", "neighborhood", "regional")):
            return "local-focused"
        return "general"

    def _find_prices(self, text: str) -> list[str]:
        pattern = r"\$\s*\d+(?:\.\d+)?(?:\s*(?:/|per)\s*(?:person|head|pp|plate|month|mo|year|yr|event|hour))?"
        matches = [match.strip() for match in re.findall(pattern, text, flags=re.I)]
        unique: list[str] = []
        seen: set[str] = set()
        for match in matches:
            normalized = match.lower().replace(" ", "")
            if normalized in seen:
                continue
            seen.add(normalized)
            unique.append(match)
            if len(unique) >= 5:
                break
        return unique

    def _find_minimum_order(self, text: str) -> str | None:
        match = re.search(r"minimum\s+order\s*(?:of|is|:)?\s*\$?\s*\d+", text)
        if not match:
            match = re.search(r"min\s+order\s*(?:of|is|:)?\s*\$?\s*\d+", text)
        if not match:
            return None
        return _normalize_whitespace(match.group(0))

    def _find_delivery_fee(self, text: str) -> str | None:
        match = re.search(r"delivery\s+fee\s*(?:of|is|:)?\s*\$\s*\d+(?:\.\d+)?", text)
        if not match:
            return None
        return _normalize_whitespace(match.group(0))

    def _find_booking_lead(self, text: str) -> str | None:
        match = re.search(r"(\d+)\s*(day|days|week|weeks)\s*(?:in\s+advance|lead\s+time)", text)
        if not match:
            match = re.search(r"book\s*(?:at\s+least\s+)?(\d+)\s*(day|days|week|weeks)\s*(?:ahead|in\s+advance)", text)
        if not match:
            return None
        return _normalize_whitespace(match.group(0))

    def _match_themes(self, text: str, theme_map: dict[str, tuple[str, ...]]) -> list[str]:
        matched: list[str] = []
        for theme, keywords in theme_map.items():
            if any(keyword in text for keyword in keywords):
                matched.append(theme)
        return matched

    def _find_market_size(self, text: str) -> list[str]:
        patterns = [
            r"(?:market\s+(?:size|value)|tam)\s*(?:is|of|:|equals?\s*)?\s*(?:\$\s*)?(\d+(?:\.\d+)?(?:\s*(?:billion|million|trillion|B|M|T))?)",
            r"(?:estimated|projected)\s+(?:market\s+)?(?:size|value)\s*(?:of|:)?\s*(?:\$\s*)?(\d+(?:\.\d+)?(?:\s*(?:billion|million|trillion|B|M|T))?)",
            r"(?:worth|valued)\s+(?:at)?\s*(?:\$\s*)?(\d+(?:\.\d+)?(?:\s*(?:billion|million|trillion|B|M|T))?)",
            r"(\d+(?:\.\d+)?)\s*(?:billion|million|trillion|B|M|T)\s*(?:market|industry| sector)",
        ]
        matches: list[str] = []
        for pattern in patterns:
            for match in re.findall(pattern, text, flags=re.I):
                if isinstance(match, tuple):
                    match = match[0] if match[0] else match[1] if len(match) > 1 else ""
                if match:
                    matches.append(match.strip())
        return matches[:3]

    def _find_growth_rate(self, text: str) -> list[str]:
        patterns = [
            r"(?:grow(?:ing|th)?|cagr)\s*(?:of|:)?\s*(\d+(?:\.\d+)?)\s*%",
            r"(\d+(?:\.\d+)?)\s*%\s*(?:annual|yearly)?\s*(?:grow(?:ing|th)?|cagr)",
            r"expected\s+to\s+(?:grow|reach)\s+(\d+(?:\.\d+)?)\s*%",
        ]
        matches: list[str] = []
        for pattern in patterns:
            for match in re.findall(pattern, text, flags=re.I):
                if match:
                    matches.append(f"{match}%")
        return matches[:3]

    def _normalize_fact_value(self, value: str) -> str:
        cleaned = _normalize_whitespace(value)
        cleaned = cleaned.replace("\n", " ")
        return cleaned.strip(" .,")

    def _extract_competitors(
        self,
        market: str,
        results: list[WebResult],
        page_summaries: dict[str, PageSummary],
        llm_context: dict[str, Any] | None = None,
        structured_evidence: list[StructuredEvidenceItem] | None = None,
    ) -> list[str]:
        competitors: list[str] = []
        seen: set[str] = set()

        def add(name: str) -> None:
            candidate = self._clean_entity_candidate(name)
            if not self._entity_allowed(candidate, market):
                return
            lowered = candidate.lower()
            if lowered in seen:
                return
            seen.add(lowered)
            competitors.append(candidate)

        for fact in structured_evidence or []:
            if not fact.entity:
                continue
            if fact.evidence_basis == "search_snippet":
                continue
            add(fact.entity)
            if len(competitors) >= 10:
                return competitors[:10]

        for result in results:
            if result.url not in page_summaries:
                continue
            host = urlparse(result.url).netloc.replace("www.", "").strip()
            if not host:
                continue
            candidate = host.split(".")[0]
            add(candidate.replace("-", " ").title())
            if len(competitors) >= 10:
                return competitors[:10]

        if len(competitors) < 3:
            for page_summary in page_summaries.values():
                for token in re.findall(r"[A-Z][A-Za-z0-9&+\-]{2,}", page_summary.title):
                    add(token)
                    if len(competitors) >= 10:
                        return competitors[:10]

        if page_summaries:
            for candidate in self._llm_competitor_names(llm_context):
                add(candidate)
                if len(competitors) >= 10:
                    break

        return competitors[:10]

    def _synthesize_context_with_llm(
        self,
        payload: MarketSearchRequest,
        web_results: list[WebResult],
        page_summaries: dict[str, PageSummary],
        structured_evidence: list[StructuredEvidenceItem],
    ) -> dict[str, Any] | None:
        if not self._llm_client.enabled:
            return None

        fetched_urls = set(page_summaries.keys())
        compact_results = [
            {
                "title": item.title,
                "url": item.url,
                "snippet": item.snippet,
                "label": item.query_label,
            }
            for item in web_results
            if item.url in fetched_urls
        ][:12]
        if not compact_results and not page_summaries:
            return None

        compact_pages = [
            {
                "url": page.url,
                "title": page.title,
                "description": page.description,
                "excerpt": page.excerpt,
            }
            for page in list(page_summaries.values())[:8]
        ]
        compact_facts = [
            {
                "entity": fact.entity,
                "fact_type": fact.fact_type,
                "value": fact.value,
                "confidence": fact.confidence,
            }
            for fact in structured_evidence
            if fact.evidence_basis != "search_snippet"
        ][:18]

        system_prompt = (
            "You are a market research analyst. Infer likely buyers, competitors, and practical assumptions "
            "from the evidence. Return strict JSON only with keys: target_customer (string), competitor_names "
            "(array), assumptions (array), business_model (string)."
        )
        user_prompt = json.dumps(
            {
                "market": payload.market,
                "geography": payload.geography,
                "profile": payload.profile,
                "template": payload.template,
                "results": compact_results,
                "pages": compact_pages,
                "structured_facts": compact_facts,
            },
            ensure_ascii=True,
        )
        return self._llm_client.chat_json(system_prompt=system_prompt, user_prompt=user_prompt)

    def _llm_target_customer(self, llm_context: dict[str, Any] | None) -> str | None:
        if not isinstance(llm_context, dict):
            return None
        raw = llm_context.get("target_customer")
        if not isinstance(raw, str):
            return None
        cleaned = re.sub(r"\s+", " ", raw).strip()
        if len(cleaned) < 3:
            return None
        return cleaned[:180]

    def _llm_assumptions(self, llm_context: dict[str, Any] | None) -> list[str]:
        if not isinstance(llm_context, dict):
            return []

        assumptions = llm_context.get("assumptions")
        if not isinstance(assumptions, list):
            return []

        cleaned_items: list[str] = []
        seen: set[str] = set()
        for item in assumptions:
            if not isinstance(item, str):
                continue
            cleaned = re.sub(r"\s+", " ", item).strip()
            if len(cleaned) < 6:
                continue
            lowered = cleaned.lower()
            if lowered in seen:
                continue
            seen.add(lowered)
            cleaned_items.append(cleaned[:220])
            if len(cleaned_items) >= 4:
                break

        return cleaned_items

    def _derive_assumptions_from_evidence(
        self,
        structured_evidence: list[StructuredEvidenceItem],
        market: str,
    ) -> list[str]:
        assumptions: list[str] = []

        complaint_themes = [
            item.value
            for item in structured_evidence
            if item.fact_type == "review_complaint_theme" and item.evidence_basis != "search_snippet"
        ]
        pricing_points = [
            item.value
            for item in structured_evidence
            if item.fact_type in {"price_per_head", "price_point"} and item.evidence_basis != "search_snippet"
        ]
        event_types = [
            item.value
            for item in structured_evidence
            if item.fact_type == "event_type_served" and item.evidence_basis != "search_snippet"
        ]

        if complaint_themes:
            assumptions.append(
                "Recurring complaint themes suggest customers are dissatisfied with current alternatives."
            )
        if pricing_points:
            assumptions.append(
                "Observed market price points provide anchors for willingness-to-pay experiments."
            )
        if event_types:
            assumptions.append(
                "Event-type coverage suggests demand can be segmented by use case and booking context."
            )

        if not assumptions:
            assumptions.append(f"There is active demand for {market}.")
            assumptions.append("Buyers have enough pain or desire to pay for better alternatives.")

        return assumptions[:4]

    def _llm_business_model(self, llm_context: dict[str, Any] | None) -> str | None:
        if not isinstance(llm_context, dict):
            return None
        raw = llm_context.get("business_model")
        if not isinstance(raw, str):
            return None

        text = raw.lower()
        if re.search(r"\bsaas|subscription|software|app\b", text):
            return "B2B SaaS subscription"
        if re.search(r"\brestaurant|catering|food|local\b", text):
            return "Local business"
        if re.search(r"\bagency|service|consulting\b", text):
            return "Service business"
        if re.search(r"\becommerce|shopify|store|d2c\b", text):
            return "Ecommerce"
        if re.search(r"\bgeneral\b", text):
            return "General business model"
        return None

    def _llm_competitor_names(self, llm_context: dict[str, Any] | None) -> list[str]:
        if not isinstance(llm_context, dict):
            return []

        raw_names = llm_context.get("competitor_names")
        if not isinstance(raw_names, list):
            return []

        names: list[str] = []
        seen: set[str] = set()
        for item in raw_names:
            if not isinstance(item, str):
                continue
            cleaned = re.sub(r"\s+", " ", item.replace("|", " ")).strip()
            if len(cleaned) < 2:
                continue
            lowered = cleaned.lower()
            if lowered in seen:
                continue
            seen.add(lowered)
            names.append(cleaned[:80])
            if len(names) >= 10:
                break

        return names

    def _infer_target_customer(
        self,
        payload: MarketSearchRequest,
        web_results: list[WebResult],
        page_summaries: dict[str, PageSummary],
        structured_evidence: list[StructuredEvidenceItem],
    ) -> str:
        market_text = payload.market.lower()
        fetched_urls = set(page_summaries.keys())
        corpus = " ".join(
            [
                market_text,
                *[
                    f"{item.title} {item.snippet}"
                    for item in web_results
                    if item.url in fetched_urls
                ],
                *[
                    f"{summary.title} {summary.description} {summary.excerpt}"
                    for summary in page_summaries.values()
                ],
                *[
                    f"{item.fact_type} {item.value}"
                    for item in structured_evidence[:40]
                    if item.evidence_basis != "search_snippet"
                ],
            ]
        ).lower()

        if payload.profile == "local_business" or payload.template == "restaurant" or any(
            keyword in market_text for keyword in ("catering", "bbq", "brisket", "restaurant", "food")
        ):
            personas = [
                "office managers",
                "wedding planners",
                "private party hosts",
                "venue managers",
            ]
            if "festival" in corpus:
                personas.append("festival organizers")
            geography = payload.geography if payload.geography.lower() not in {"global", "worldwide"} else "target local region"
            return f"{', '.join(personas)} in {geography}"

        if "b2b" in corpus or "saas" in corpus or "software" in corpus:
            return f"Decision-makers evaluating {payload.market} solutions"

        return f"Customers actively exploring {payload.market} options"

    def _compose_observed_fact(self, result: WebResult, page_summary: PageSummary | None) -> str:
        if page_summary is None:
            return result.snippet or f"Relevant result for {result.title}."

        details = [page_summary.title, page_summary.description, page_summary.excerpt, result.snippet]
        fact = " ".join(part for part in details if part).strip()
        return fact or result.title

    def _infer_source_type(self, result: WebResult, page_summary: PageSummary | None = None) -> str:
        host = urlparse(result.url).netloc.lower()
        query_label = result.query_label.lower()
        text = " ".join(
            part
            for part in (
                result.query_label,
                result.title,
                result.snippet,
                page_summary.title if page_summary else "",
                page_summary.description if page_summary else "",
                page_summary.excerpt if page_summary else "",
            )
            if part
        ).lower()

        if query_label in {"pricing", "menu_prices", "pricing_gap", "pricing_gap_alt"}:
            return "pricing_page"
        if query_label in {"reviews", "reviews_gap"} or any(domain in host for domain in ("g2.com", "capterra", "trustpilot", "yelp", "tripadvisor")):
            return "review_site"
        if query_label in {"complaints", "complaints_alt", "forums_gap"}:
            return "customer_complaint"
        if query_label in {"jobs"} or any(domain in host for domain in ("glassdoor", "indeed", "linkedin.com")):
            return "job_post"
        if query_label in {"trends"} or any(domain in host for domain in ("statista", "ibisworld", "mckinsey", "forrester", "google.com/trends")):
            return "market_report"
        if query_label in {"directory", "competitors", "alternatives", "geo", "directory_gap", "competitor_gap"}:
            return "directory_listing"
        if query_label in {"forums"} or any(domain in host for domain in ("reddit.com", "news.ycombinator", "quora.com")):
            return "forum_social"
        if query_label in {"editorial", "editorial_gap"} or any(domain in host for domain in ("eater.com", "thrillist.com", "timeout.com", "infatuation")):
            return "local_editorial"
        if query_label in {"public_data", "public_data_gap"} or any(domain in host for domain in ("census", "data.", "bureau", ".gov")):
            return "public_data"
        if "trend" in text or "growth" in text or "rising" in text or "demand" in text:
            return "trend_signal"
        if "price" in text or "pricing" in text or "cost" in text:
            return "pricing_page"

        return "company_website"

    def _infer_strength(self, source_type: str) -> str:
        if source_type in {"review_site", "market_report", "job_post", "pricing_page", "public_data"}:
            return "high"
        if source_type in {"forum_social", "customer_complaint", "directory_listing", "trend_signal", "local_editorial"}:
            return "medium"
        return "low"

    def _infer_business_model(
        self,
        market: str,
        results: list[WebResult],
        page_summaries: dict[str, PageSummary],
    ) -> str:
        page_text = " ".join(
            [f"{summary.title} {summary.description} {summary.excerpt}" for summary in page_summaries.values()]
        )
        fetched_urls = set(page_summaries.keys())
        result_text = " ".join([f"{r.title} {r.snippet}" for r in results if r.url in fetched_urls])
        text = f"{market} {result_text} {page_text}"
        text = text.lower()

        if re.search(r"\bsaas|subscription|software|app\b", text):
            return "B2B SaaS subscription"
        if re.search(r"\bagency|service|consulting\b", text):
            return "Service business"
        if re.search(r"\becommerce|shopify|store|d2c\b", text):
            return "Ecommerce"
        if re.search(r"\brestaurant|catering|food|local\b", text):
            return "Local business"

        return "General business model"


def _first_match(pattern: str, text: str, flags: int = 0) -> str:
    match = re.search(pattern, text, flags)
    if not match:
        return ""
    return html_lib.unescape(match.group(1)).strip()


def _strip_html(text: str) -> str:
    if not text:
        return ""
    text = re.sub(r"(?is)<(script|style|noscript).*?>.*?</\1>", " ", text)
    text = re.sub(r"(?s)<[^>]+>", " ", text)
    return html_lib.unescape(text)


def _normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def _html_to_excerpt(html_text: str, limit: int = 600) -> str:
    stripped = _strip_html(html_text)
    return _normalize_whitespace(stripped)[:limit]


def _clamp01(value: float) -> float:
    return max(0.0, min(0.98, value))
