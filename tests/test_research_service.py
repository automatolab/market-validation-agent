from market_validation.models import MarketSearchRequest
from market_validation.research import DuckDuckGoSearcher, MarketResearchService, PageSummary, WebResult


class _StubLLMClient:
    def __init__(self, payload: dict[str, object]) -> None:
        self.enabled = True
        self._payload = payload

    def chat_json(self, system_prompt: str, user_prompt: str) -> dict[str, object] | None:
        del system_prompt, user_prompt
        return self._payload


class _RecordingSearcher:
    def __init__(self) -> None:
        self.queries: list[str] = []

    def search(self, query: str, max_results: int) -> list[WebResult]:
        self.queries.append(query)
        if "pricing" in query:
            return [
                WebResult(
                    title="Pricing page",
                    url="https://acme.example/pricing",
                    snippet="Plans start at $29 per month",
                    query_label=query,
                )
            ]
        if "reviews" in query:
            return [
                WebResult(
                    title="Review results",
                    url="https://g2.com/products/acme",
                    snippet="Users mention setup friction",
                    query_label=query,
                )
            ]
        if "complaints" in query:
            return [
                WebResult(
                    title="Complaint thread",
                    url="https://reddit.com/r/example",
                    snippet="Customers complain about speed",
                    query_label=query,
                )
            ]
        if "competitors" in query:
            return [
                WebResult(
                    title="Competitor directory",
                    url="https://example.com/competitors",
                    snippet="Top alternatives listed here",
                    query_label=query,
                )
            ]
        return [
            WebResult(
                title="Core result",
                url="https://acme.example",
                snippet="Market demand appears active",
                query_label=query,
            )
        ]


class _RecordingFetcher:
    def __init__(self) -> None:
        self.fetched_urls: list[str] = []

    def fetch(self, url: str) -> PageSummary | None:
        self.fetched_urls.append(url)
        return PageSummary(
            url=url,
            title="Fetched page title",
            description="Customers mention pricing and reviews",
            excerpt="The page discusses pricing, alternatives, and complaints.",
        )


class _NullFetcher:
    def fetch(self, url: str) -> PageSummary | None:
        del url
        return None


class _SnippetOnlySearcher:
    def search(self, query: str, max_results: int) -> list[WebResult]:
        del max_results
        slug = query.lower().replace(" ", "-")[:48]
        return [
            WebResult(
                title=f"{query.title()} results",
                url=f"https://results.example/{slug}",
                snippet="Top options mention pricing around $25 per person and mixed review feedback.",
                query_label=query,
            )
        ]


def test_duckduckgo_searcher_html_fallback_parses_results(monkeypatch) -> None:
    searcher = DuckDuckGoSearcher()

    def _empty_ddgs(*_args, **_kwargs):
        return []

    class _Headers:
        def get_content_charset(self):
            return "utf-8"

    class _FakeResponse:
        headers = _Headers()

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            del exc_type, exc, tb
            return False

        def read(self, _max_bytes: int) -> bytes:
            return b'''
                <html>
                  <body>
                    <a class="result__a" href="/l/?kh=-1&uddg=https%3A%2F%2Fexample.com%2Fcatering">Top Catering Option</a>
                    <div class="result__snippet">Menu starts at $25 per person.</div>
                  </body>
                </html>
            '''

    monkeypatch.setattr(searcher, "_search_with_ddgs", _empty_ddgs)

    import market_validation.research as research_module

    monkeypatch.setattr(research_module, "urlopen", lambda *_args, **_kwargs: _FakeResponse())

    results = searcher.search("brisket catering", max_results=3)

    assert results
    assert results[0].url == "https://example.com/catering"
    assert "Top Catering Option" in results[0].title


def test_deep_research_mode_collects_more_evidence() -> None:
    searcher = _RecordingSearcher()
    fetcher = _RecordingFetcher()
    service = MarketResearchService(searcher=searcher, fetcher=fetcher)

    request = service.build_validation_request(
        MarketSearchRequest(
            market="brisket catering",
            geography="US",
            profile="local_business",
            template="restaurant",
            research_mode="deep",
            max_search_results=12,
            minimum_evidence_rows=14,
        )
    )

    assert len(searcher.queries) >= 10
    assert len(fetcher.fetched_urls) >= 5
    assert len(request.evidence_inputs) >= 8
    assert len(request.raw_sources) >= 5
    assert len(request.structured_evidence) >= 5
    assert any(item.source_type == "pricing_page" for item in request.evidence_inputs)
    assert any(item.source_type == "review_site" for item in request.evidence_inputs)
    assert any(item.evidence_basis == "fetched_page" for item in request.evidence_inputs)
    assert any(
        item.fact_type in {"price_point", "price_per_head"} or item.source_type == "pricing_page"
        for item in request.structured_evidence
    )
    assert any(item.source_type in {"review_site", "customer_complaint", "forum_social"} for item in request.structured_evidence)
    assert any(item.evidence_basis == "fetched_page" for item in request.structured_evidence)
    assert "office managers" in request.target_customer.lower()
    assert "wedding planners" in request.target_customer.lower()


def test_market_research_service_collects_multiple_query_types() -> None:
    searcher = _RecordingSearcher()
    fetcher = _RecordingFetcher()
    service = MarketResearchService(searcher=searcher, fetcher=fetcher)

    request = service.build_validation_request(
        MarketSearchRequest(
            market="brisket catering",
            geography="US",
            profile="local_business",
            template="restaurant",
            max_search_results=10,
        )
    )

    assert any("pricing" in query for query in searcher.queries)
    assert any("reviews" in query for query in searcher.queries)
    assert any("complaints" in query for query in searcher.queries)
    assert any("competitors" in query for query in searcher.queries)
    assert any("public statistics" in query or "best brisket catering list" in query for query in searcher.queries)
    assert fetcher.fetched_urls
    assert len(request.evidence_inputs) >= 6
    assert any(item.source_type == "pricing_page" for item in request.evidence_inputs)
    assert any(item.source_type == "review_site" for item in request.evidence_inputs)
    assert request.raw_sources
    assert request.structured_evidence
    assert any(item.evidence_basis == "fetched_page" for item in request.evidence_inputs)
    assert request.business_model == "Local business"
    assert "private party hosts" in request.target_customer.lower()
    assert "venue managers" in request.target_customer.lower()
    assert request.competitors


def test_market_research_service_uses_llm_context_when_available() -> None:
    searcher = _RecordingSearcher()
    fetcher = _RecordingFetcher()
    llm_client = _StubLLMClient(
        {
            "target_customer": "Office managers, wedding planners, and venue managers in Austin",
            "competitor_names": ["Smokehouse One Catering", "Hill Country BBQ Events"],
            "assumptions": [
                "Corporate lunch demand peaks on Tuesday through Thursday.",
                "Wedding planners prioritize on-time delivery over menu variety.",
            ],
            "business_model": "Local catering service",
        }
    )

    service = MarketResearchService(searcher=searcher, fetcher=fetcher, llm_client=llm_client)
    request = service.build_validation_request(
        MarketSearchRequest(
            market="brisket catering",
            geography="Austin, TX",
            profile="local_business",
            template="restaurant",
            max_search_results=8,
        )
    )

    assert request.target_customer.startswith("Office managers")
    assert request.business_model == "Local business"
    assert any("Corporate lunch demand" in item for item in request.assumptions)
    assert "Smokehouse One Catering" in request.competitors
    assert request.raw_sources
    assert request.structured_evidence


def test_snippet_only_research_is_marked_and_low_strength() -> None:
    service = MarketResearchService(searcher=_SnippetOnlySearcher(), fetcher=_NullFetcher())

    request = service.build_validation_request(
        MarketSearchRequest(
            market="brisket catering",
            geography="US",
            profile="local_business",
            template="restaurant",
            max_search_results=8,
        )
    )

    diagnostics = request.research_diagnostics
    assert diagnostics["queries_attempted"] > 0
    assert diagnostics["fetch_attempted"] > 0
    assert diagnostics["fetch_success"] == 0
    assert diagnostics["snippet_source_count"] >= 1

    snippet_rows = [item for item in request.evidence_inputs if item.evidence_basis == "search_snippet"]
    assert snippet_rows
    assert all(item.strength in {"low", "medium"} for item in snippet_rows)