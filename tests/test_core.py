"""Core unit tests — pure functions, no network/AI/DB."""

import sys
from pathlib import Path

# Ensure the project root is on sys.path so imports resolve.
_project_root = Path(__file__).resolve().parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))


# ---------------------------------------------------------------------------
# 1. market_archetype.detect_archetype
# ---------------------------------------------------------------------------

from market_validation.market_archetype import detect_archetype


class TestDetectArchetype:
    def test_brisket_is_b2b_industrial(self):
        key, conf = detect_archetype("brisket")
        assert key == "b2b-industrial"
        assert conf > 0

    def test_bbq_restaurant_is_local_service(self):
        key, _conf = detect_archetype("BBQ restaurant")
        assert key == "local-service"

    def test_crm_software_is_b2b_saas(self):
        key, _conf = detect_archetype("CRM software for enterprise teams")
        assert key == "b2b-saas"

    def test_dental_is_healthcare(self):
        key, _conf = detect_archetype("dental clinic")
        assert key == "healthcare"

    def test_confidence_in_range(self):
        _key, conf = detect_archetype("organic produce wholesale distribution")
        assert 0 <= conf <= 100

    def test_industrial_automation_is_b2b_industrial(self):
        # Pure software automation stays saas, but anything with hardware /
        # IoT / agritech anchors must route to b2b-industrial — even when the
        # product mentions "automation systems".
        key, conf = detect_archetype(
            "commercial hydroponic growers", "hydroponic plant automation systems"
        )
        assert key == "b2b-industrial"
        assert conf >= 50

    def test_iot_sensors_for_growers_is_b2b_industrial(self):
        key, _ = detect_archetype("hydroponic farms", "iot sensors")
        assert key == "b2b-industrial"

    def test_marketing_automation_platform_stays_b2b_saas(self):
        # Regression: pure-software automation must NOT be flipped to industrial.
        key, _ = detect_archetype("marketing automation platform")
        assert key == "b2b-saas"

    def test_greenhouse_grower_is_b2b_industrial(self):
        key, _ = detect_archetype("greenhouse grower")
        assert key == "b2b-industrial"


# ---------------------------------------------------------------------------
# 1b. infer_market_profile + detect_market_category routing
# ---------------------------------------------------------------------------

from market_validation._helpers.common import infer_market_profile
from market_validation.query_context import detect_market_category


class TestMarketCategoryRouting:
    def test_hydroponic_automation_routes_industrial(self):
        # Was the original bug: "automation" alone routed to saas, dragging
        # the entire pipeline (search queries, qualification context, scoring
        # weights) onto a software path for what is really industrial IoT.
        profile = infer_market_profile(
            "commercial hydroponic growers", "hydroponic plant automation systems"
        )
        assert profile["category"] == "industrial"
        assert detect_market_category(
            "commercial hydroponic growers", "hydroponic plant automation systems"
        ) == "industrial"

    def test_marketing_automation_platform_stays_saas(self):
        profile = infer_market_profile("marketing automation platform", None)
        assert profile["category"] == "saas"
        assert detect_market_category("marketing automation platform", None) == "saas"

    def test_crm_software_stays_saas(self):
        # Regression: don't accidentally flip generic SaaS into industrial.
        assert detect_market_category("CRM software", "enterprise teams") == "saas"

    def test_bbq_restaurant_stays_food(self):
        assert detect_market_category("BBQ restaurant", None) == "food"

    def test_dental_clinic_stays_healthcare(self):
        assert detect_market_category("dental clinic", None) == "healthcare"


# ---------------------------------------------------------------------------
# 2. agent._clean_company_name
# ---------------------------------------------------------------------------

from market_validation.agent import _clean_company_name


class TestCleanCompanyName:
    def test_strip_tiktok_suffix(self):
        assert _clean_company_name("SmokeHouse BBQ | TikTok") == "SmokeHouse BBQ"

    def test_strip_yelp_suffix(self):
        assert _clean_company_name("Joe's Deli - Yelp") == "Joe's Deli"

    def test_strip_youtube_suffix(self):
        assert _clean_company_name("Best Brisket - YouTube") == "Best Brisket"

    def test_strip_prefix(self):
        assert _clean_company_name("Menu | Acme Grill") == "Acme Grill"

    def test_plain_name_unchanged(self):
        assert _clean_company_name("Acme Corporation") == "Acme Corporation"

    def test_empty_string_returns_empty(self):
        # Edge: empty input should not crash
        result = _clean_company_name("")
        assert isinstance(result, str)


# ---------------------------------------------------------------------------
# 3. agent._is_junk_company
# ---------------------------------------------------------------------------

from market_validation.agent import _is_junk_company


class TestIsJunkCompany:
    def test_normal_company_is_not_junk(self):
        assert not _is_junk_company({"company_name": "Acme BBQ", "website": "https://acmebbq.com"})

    def test_yelp_url_is_junk(self):
        assert _is_junk_company({"company_name": "Some Place", "website": "https://www.yelp.com/biz/some-place"})

    def test_yellow_pages_name_is_junk(self):
        assert _is_junk_company({"company_name": "yellow pages listing"})

    def test_top_10_listicle_is_junk(self):
        assert _is_junk_company({"company_name": "top 10 BBQ restaurants"})

    def test_short_name_is_junk(self):
        assert _is_junk_company({"company_name": "ab"})

    def test_tripadvisor_url_is_junk(self):
        assert _is_junk_company({
            "company_name": "Good Place",
            "website": "https://www.tripadvisor.com/Restaurant-good-place",
        })


# ---------------------------------------------------------------------------
# 4. agent._extract_phone_text
# ---------------------------------------------------------------------------

from market_validation.agent import _extract_phone_text


class TestExtractPhoneText:
    def test_standard_format(self):
        assert _extract_phone_text("Call us at (408) 555-1234 today") == "(408) 555-1234"

    def test_dashed_format(self):
        assert _extract_phone_text("Phone: 408-555-1234") == "408-555-1234"

    def test_international_format(self):
        result = _extract_phone_text("+1-408-555-1234")
        assert "408" in result and "1234" in result

    def test_no_phone(self):
        assert _extract_phone_text("no phone here") == ""

    def test_empty_input(self):
        assert _extract_phone_text("") == ""


# ---------------------------------------------------------------------------
# 5. agent._extract_email_text
# ---------------------------------------------------------------------------

from market_validation.agent import _extract_email_text


class TestExtractEmailText:
    def test_simple_email(self):
        assert _extract_email_text("reach us at info@acme.com for details") == "info@acme.com"

    def test_plus_address(self):
        assert _extract_email_text("user+tag@example.com") == "user+tag@example.com"

    def test_no_email(self):
        assert _extract_email_text("no email here") == ""

    def test_empty_input(self):
        assert _extract_email_text("") == ""


# ---------------------------------------------------------------------------
# 6. company_enrichment.generate_email_patterns
# ---------------------------------------------------------------------------

from market_validation.company_enrichment import generate_email_patterns


class TestGenerateEmailPatterns:
    def test_returns_list(self):
        patterns = generate_email_patterns("gmail.com")
        assert isinstance(patterns, list)
        assert len(patterns) > 0

    def test_pattern_format(self):
        patterns = generate_email_patterns("example.com")
        for p in patterns:
            assert "@example.com" in p["email"]
            assert p["pattern_generated"] is True

    def test_common_prefixes(self):
        patterns = generate_email_patterns("test.com")
        emails = [p["email"] for p in patterns]
        assert "info@test.com" in emails
        assert "contact@test.com" in emails
        assert "sales@test.com" in emails

    def test_empty_domain(self):
        assert generate_email_patterns("") == []

    def test_no_dot_domain(self):
        assert generate_email_patterns("localhost") == []


# ---------------------------------------------------------------------------
# 7. company_enrichment.domain_from_url
# ---------------------------------------------------------------------------

from market_validation.company_enrichment import domain_from_url


class TestDomainFromUrl:
    def test_simple_url(self):
        assert domain_from_url("https://www.acmebbq.com/about") == "acmebbq.com"

    def test_no_www(self):
        assert domain_from_url("https://acmebbq.com") == "acmebbq.com"

    def test_http(self):
        assert domain_from_url("http://example.org/page") == "example.org"

    def test_none_input(self):
        assert domain_from_url(None) is None

    def test_empty_string(self):
        assert domain_from_url("") is None

    def test_no_scheme(self):
        # "acmebbq.com/about" — split("//") gives ["acmebbq.com/about"]
        result = domain_from_url("acmebbq.com/about")
        assert result == "acmebbq.com"


# ---------------------------------------------------------------------------
# 8. company_enrichment.verify_email (DNS-based, fast)
# ---------------------------------------------------------------------------

from market_validation.company_enrichment import verify_email


class TestVerifyEmail:
    def test_valid_domain(self):
        result = verify_email("test@gmail.com")
        assert result["valid"] is True
        assert result["email"] == "test@gmail.com"

    def test_fake_domain(self):
        result = verify_email("test@thisisnotarealdomain99999.com")
        assert result["valid"] is False

    def test_no_at_sign(self):
        result = verify_email("notanemail")
        assert result["valid"] is False

    def test_no_tld(self):
        result = verify_email("user@localhost")
        assert result["valid"] is False


# ---------------------------------------------------------------------------
# 9. validation_scorecard.score_market_attractiveness
# ---------------------------------------------------------------------------

from market_validation.validation_scorecard import score_market_attractiveness


class TestScoreMarketAttractiveness:
    def test_returns_float(self):
        sizing = {"tam_low": 1_000_000, "tam_high": 5_000_000, "growth_rate": 0.10}
        demand = {"demand_trend": "rising", "demand_score": 70}
        score = score_market_attractiveness(sizing, demand)
        assert isinstance(score, float)

    def test_in_range(self):
        sizing = {"tam_low": 0, "tam_high": 0, "growth_rate": 0}
        demand = {"demand_trend": "falling", "demand_score": 10}
        score = score_market_attractiveness(sizing, demand)
        assert 0 <= score <= 100

    def test_large_tam_scores_higher(self):
        small = score_market_attractiveness(
            {"tam_low": 100_000, "tam_high": 200_000, "growth_rate": 0.05},
            {"demand_trend": "stable", "demand_score": 50},
        )
        large = score_market_attractiveness(
            {"tam_low": 5_000_000_000, "tam_high": 8_000_000_000, "growth_rate": 0.05},
            {"demand_trend": "stable", "demand_score": 50},
        )
        assert large > small

    def test_rising_trend_beats_falling(self):
        rising = score_market_attractiveness(
            {"tam_low": 1_000_000, "tam_high": 2_000_000, "growth_rate": 0.05},
            {"demand_trend": "rising", "demand_score": 60},
        )
        falling = score_market_attractiveness(
            {"tam_low": 1_000_000, "tam_high": 2_000_000, "growth_rate": 0.05},
            {"demand_trend": "falling", "demand_score": 60},
        )
        assert rising > falling


# ---------------------------------------------------------------------------
# 10. validation_scorecard.compute_scorecard
# ---------------------------------------------------------------------------

from market_validation.validation_scorecard import compute_scorecard


class TestComputeScorecard:
    def _base_inputs(self, **overrides):
        sizing = {"tam_low": 2_000_000, "tam_high": 5_000_000, "growth_rate": 0.12}
        demand = {"demand_trend": "rising", "demand_score": 75, "willingness_to_pay": "high"}
        competition = {"competitive_intensity": 30, "market_concentration": "fragmented"}
        signals = {"regulatory_risks": [], "technology_maturity": "growing", "timing_assessment": "good"}
        d = {"sizing": sizing, "demand": demand, "competition": competition, "signals": signals}
        d.update(overrides)
        return d

    def test_basic_scorecard_keys(self):
        inputs = self._base_inputs()
        sc = compute_scorecard(**inputs)
        assert "overall_score" in sc
        assert "verdict" in sc
        assert "market_attractiveness" in sc

    def test_verdict_strong_go(self):
        inputs = self._base_inputs(
            demand={"demand_trend": "rising", "demand_score": 90, "willingness_to_pay": "high"},
            competition={"competitive_intensity": 10, "market_concentration": "fragmented"},
            signals={"regulatory_risks": [], "technology_maturity": "growing", "timing_assessment": "good", "job_posting_volume": "high"},
        )
        sc = compute_scorecard(**inputs)
        assert sc["verdict"] in ("strong_go", "go")

    def test_verdict_no_go(self):
        inputs = self._base_inputs(
            sizing={"tam_low": 0, "tam_high": 0, "growth_rate": -0.05},
            demand={"demand_trend": "falling", "demand_score": 10, "willingness_to_pay": "low"},
            competition={"competitive_intensity": 95, "market_concentration": "monopolistic"},
            signals={"regulatory_risks": ["a", "b", "c", "d"], "technology_maturity": "declining", "timing_assessment": "poor", "job_posting_volume": "none"},
        )
        sc = compute_scorecard(**inputs)
        assert sc["verdict"] in ("no_go", "cautious")

    def test_overall_score_in_range(self):
        inputs = self._base_inputs()
        sc = compute_scorecard(**inputs)
        assert 0 <= sc["overall_score"] <= 100

    def test_no_ai_means_no_reasoning(self):
        inputs = self._base_inputs()
        sc = compute_scorecard(**inputs, run_ai=None)
        assert "verdict_reasoning" not in sc

    def test_unknown_trend_scores_neutral_not_negative(self):
        # Regression: when pytrends and Wikipedia both fail to return data,
        # demand_trend defaults to "unknown" — the scorecard must NOT treat
        # this as falling demand. "unknown" should give the same neutral
        # score as "stable".
        from market_validation.validation_scorecard import score_market_attractiveness
        unknown = score_market_attractiveness(
            {"tam_low": 1_000_000, "tam_high": 2_000_000, "growth_rate": 0.05},
            {"demand_trend": "unknown", "demand_score": 50},
        )
        stable = score_market_attractiveness(
            {"tam_low": 1_000_000, "tam_high": 2_000_000, "growth_rate": 0.05},
            {"demand_trend": "stable", "demand_score": 50},
        )
        falling = score_market_attractiveness(
            {"tam_low": 1_000_000, "tam_high": 2_000_000, "growth_rate": 0.05},
            {"demand_trend": "falling", "demand_score": 50},
        )
        assert unknown == stable
        assert unknown > falling


# ---------------------------------------------------------------------------
# 11. demand_analysis trend keyword shortening
# ---------------------------------------------------------------------------

from market_validation.demand_analysis import _shorten_for_trends, _trends_keywords


class TestShortenForTrends:
    def test_drops_generic_suffixes(self):
        # Long product phrases must be shortened to the head noun phrase so
        # Google Trends / Wikipedia pageviews can return useful data.
        assert _shorten_for_trends("hydroponic plant automation systems") == "hydroponic plant"

    def test_keeps_short_terms(self):
        assert _shorten_for_trends("hydroponics") == "hydroponics"

    def test_handles_all_stopwords(self):
        # Falls back to bare input when every word is generic — never empty.
        result = _shorten_for_trends("system platform automation")
        assert result  # non-empty
        assert isinstance(result, str)

    def test_uses_market_term_when_provided(self):
        # When market is provided, prefer it as primary keyword (broader,
        # more search volume).
        kws = _trends_keywords(
            "hydroponic plant automation systems",
            archetype="b2b-industrial",
            market="hydroponic farming",
        )
        assert kws[0] == "hydroponic farming"

    def test_returns_max_two_keywords(self):
        kws = _trends_keywords("brisket", archetype="b2b-industrial", market="BBQ restaurants")
        assert len(kws) <= 2

    def test_no_modifier_suffixes(self):
        # Old behaviour appended " supplier" / " software" / " platform"
        # which were too long for trends. Make sure we don't regress.
        kws = _trends_keywords("brisket", archetype="b2b-industrial", market="BBQ restaurants")
        assert all("supplier" not in k for k in kws)
        assert all("wholesale" not in k for k in kws)


# ---------------------------------------------------------------------------
# 12. demand_analysis._consensus_trend
# ---------------------------------------------------------------------------

from market_validation.demand_analysis import _consensus_trend


class TestConsensusTrend:
    def test_all_unknown_returns_unknown(self):
        # No evidence in any direction → unknown, never falling.
        assert _consensus_trend([]) == "unknown"
        assert _consensus_trend(["unknown", "unknown"]) == "unknown"

    def test_single_rising_wins(self):
        # One rising vs nothing else → rising.
        assert _consensus_trend(["rising"]) == "rising"

    def test_majority_falling_wins(self):
        # Multiple falling outweighs single rising.
        assert _consensus_trend(["falling", "falling", "rising"]) == "falling"

    def test_tie_favours_rising(self):
        # Equal counts → bias toward rising. Original pipeline let one falling
        # source pull the entire verdict negative; the consensus rule fixes it.
        assert _consensus_trend(["rising", "falling"]) == "rising"
        assert _consensus_trend(["rising", "falling", "stable"]) == "rising"

    def test_stable_when_no_directional_signal(self):
        assert _consensus_trend(["stable", "stable"]) == "stable"
        assert _consensus_trend(["stable", "unknown"]) == "stable"

    def test_lone_falling_against_stable_is_stable(self):
        # Single falling source with stable companions → stable, not falling.
        assert _consensus_trend(["falling", "stable", "stable"]) == "stable"


# ---------------------------------------------------------------------------
# 13. free_data_sources trend APIs — pure / helpers
# ---------------------------------------------------------------------------

from market_validation.free_data_sources import (
    gdelt_news_timeline,
    github_repo_growth,
    hackernews_volume_timeline,
    openalex_works_timeline,
    wikipedia_pageviews,
)


class TestTrendAPIsAreCallable:
    """Smoke tests that the new trend APIs exist and have the right shape.

    These are not network-bound — they just verify the module exports the
    functions and that all-error fallbacks return the expected `available`
    flag. Live network calls are exercised by separate smoke scripts.
    """

    def test_all_trend_apis_callable(self):
        for fn in (
            wikipedia_pageviews,
            gdelt_news_timeline,
            openalex_works_timeline,
            github_repo_growth,
            hackernews_volume_timeline,
        ):
            assert callable(fn)

    def test_unavailable_payload_is_dict(self):
        # Every trend API must return a dict so callers can safely .get().
        # Even when the underlying network call fails the function returns
        # `{"available": False, "reason": ...}` rather than None.
        from unittest.mock import patch
        with patch("market_validation.free_data_sources._get", return_value=None):
            for fn, args in [
                (wikipedia_pageviews, ("vertical farming",)),
                (gdelt_news_timeline, ("vertical farming",)),
                (openalex_works_timeline, ("vertical farming",)),
                (hackernews_volume_timeline, ("vertical farming",)),
            ]:
                r = fn(*args)
                assert isinstance(r, dict)
                assert r.get("available") in (False, None)


# ---------------------------------------------------------------------------
# 14. _get_trends_data composition (no network)
# ---------------------------------------------------------------------------

from market_validation.demand_analysis import _get_trends_data
from market_validation.free_data_sources import clear_trend_cache


class TestGetTrendsDataComposition:
    def _patch_all_sources(self, monkeypatch, wiki, gdelt, openalex, github, hn):
        """Patch the trend feed module-level functions used by _get_trends_data.

        ``_get_trends_data`` resolves the trend functions via local
        ``from … import …`` at call time, so we patch on the source module
        (free_data_sources) where the bindings actually live.
        """
        import market_validation.free_data_sources as fds
        clear_trend_cache()  # don't let prior tests leak cached results
        monkeypatch.setattr(fds, "wikipedia_pageviews", wiki)
        monkeypatch.setattr(fds, "gdelt_news_timeline", gdelt)
        monkeypatch.setattr(fds, "openalex_works_timeline", openalex)
        monkeypatch.setattr(fds, "github_repo_growth", github)
        monkeypatch.setattr(fds, "hackernews_volume_timeline", hn)

    def test_returns_all_source_keys(self, monkeypatch):
        # Patch every external trend API to return a known shape, then
        # confirm _get_trends_data wires each into the composed dict.
        def _wiki(query, days=365, article_title=None):
            return {
                "available": True, "article": "Vertical farming",
                "samples": 365, "avg_daily_views": 350.0,
                "delta_pct": 5.0, "trend": "stable",
            }

        def _gdelt(query, timespan_months=24):
            return {"available": True, "query": query, "trend": "rising",
                    "samples": 365, "avg_daily_articles": 2.0, "delta_pct": 18.0}

        def _openalex(query, years=5):
            return {"available": True, "query": query, "trend": "rising",
                    "total_works": 5000, "last_year": 2025,
                    "last_year_count": 1500, "prior_avg": 1200, "delta_pct": 25.0}

        def _github(query):
            return {"available": True, "query": query, "trend": "rising",
                    "last_year_count": 100, "prior_year_count": 60, "delta_pct": 67.0}

        def _hn(query):
            return {"available": True, "query": query, "trend": "stable",
                    "last_year_stories": 8, "prior_year_stories": 7, "delta_pct": 14.0}

        self._patch_all_sources(monkeypatch, _wiki, _gdelt, _openalex, _github, _hn)

        out = _get_trends_data("vertical farming", "United States",
                               archetype="b2b-industrial", market="vertical farming")

        # Every source key present
        for key in ("wikipedia_pageviews", "gdelt", "openalex", "github",
                    "hackernews_volume", "per_source_trends", "sources_available"):
            assert key in out, f"missing key {key}"

        # All five available sources logged
        assert set(out["per_source_trends"].keys()) >= {
            "wikipedia_pageviews", "gdelt", "openalex", "github", "hackernews_volume",
        }

        # Consensus picks rising — 3 rising vs 2 stable
        assert out["interest_trend"] == "rising"

    def test_returns_unknown_when_no_sources_available(self, monkeypatch):
        # When every trend source fails, demand_trend must remain "unknown",
        # never "falling" — absence of signal is not negative signal.
        unavailable = lambda *_a, **_kw: {"available": False}
        self._patch_all_sources(
            monkeypatch, unavailable, unavailable, unavailable, unavailable, unavailable
        )

        out = _get_trends_data("foo", "US", market="foo")
        assert out["interest_trend"] == "unknown"
        assert out["primary_source"] == "none"


# ---------------------------------------------------------------------------
# 15. analyze_demand AI prompt — multi-source consensus surfaces correctly
# ---------------------------------------------------------------------------

from market_validation.services.validation import ValidationService


class TestRecoverStageFromDb:
    """Validation stages must rehydrate cleanly from the flattened DB record
    so --from-stage / --skip-stages can resume a partially-completed run."""

    def test_returns_none_on_empty_prior(self):
        for stage in ValidationService._STAGE_ORDER:
            assert ValidationService._recover_stage_from_db(stage, {}) is None

    def test_sizing_recovered_from_columns(self):
        prior = {"tam_low": 1_000_000, "tam_high": 5_000_000, "growth_rate": 0.10}
        out = ValidationService._recover_stage_from_db("sizing", prior)
        assert out == prior

    def test_demand_recovered_from_columns(self):
        prior = {"demand_score": 70, "demand_trend": "rising",
                 "demand_pain_points": ["a", "b"]}
        out = ValidationService._recover_stage_from_db("demand", prior)
        assert out == prior

    def test_porters_recovered_from_json_blob(self):
        # Some stage payloads are stored as a JSON blob column rather than
        # individual columns. _recover should round-trip them transparently.
        import json as _json
        payload = {"structural_attractiveness": 65, "dominant_force": "buyers"}
        prior = {"porters_data": _json.dumps(payload)}
        out = ValidationService._recover_stage_from_db("porters", prior)
        assert out == payload

    def test_timing_renames_persisted_columns(self):
        # DB columns are timing_enablers / timing_headwinds; the in-memory
        # shape callers expect uses bare 'enablers' / 'headwinds' keys.
        prior = {
            "timing_score": 80, "timing_verdict": "good",
            "timing_enablers": ["regulation"], "timing_headwinds": ["recession"],
        }
        out = ValidationService._recover_stage_from_db("timing", prior)
        assert out["timing_score"] == 80
        assert out["enablers"] == ["regulation"]
        assert out["headwinds"] == ["recession"]
        assert "timing_enablers" not in out

    def test_unknown_stage_returns_none(self):
        assert ValidationService._recover_stage_from_db("nonsense", {"foo": 1}) is None


from market_validation.demand_analysis import analyze_demand


class TestAnalyzeDemandPrompt:
    def test_prompt_contains_all_source_lines(self, monkeypatch):
        """End-to-end check that the AI prompt includes one line per
        available trend source, with the consensus block and per-source
        labels filled in. Catches regressions where a source is fetched
        but never serialized into the prompt.
        """
        import market_validation.free_data_sources as fds
        clear_trend_cache()

        def _wiki(query, days=365, article_title=None):
            return {
                "available": True, "article": "TestTopic",
                "samples": 365, "avg_daily_views": 100.0,
                "delta_pct": 12.0, "trend": "rising",
            }
        def _gdelt(query, timespan_months=24):
            return {"available": True, "query": query, "trend": "rising",
                    "samples": 365, "avg_daily_articles": 5.0, "delta_pct": 20.0}
        def _openalex(query, years=5):
            return {"available": True, "query": query, "trend": "rising",
                    "total_works": 1000, "last_year": 2025,
                    "last_year_count": 300, "prior_avg": 200, "delta_pct": 50.0}
        def _github(query):
            return {"available": True, "query": query, "trend": "rising",
                    "last_year_count": 80, "prior_year_count": 40, "delta_pct": 100.0}
        def _hn(query):
            return {"available": True, "query": query, "trend": "stable",
                    "last_year_stories": 5, "prior_year_stories": 5, "delta_pct": 0.0}

        monkeypatch.setattr(fds, "wikipedia_pageviews", _wiki)
        monkeypatch.setattr(fds, "gdelt_news_timeline", _gdelt)
        monkeypatch.setattr(fds, "openalex_works_timeline", _openalex)
        monkeypatch.setattr(fds, "github_repo_growth", _github)
        monkeypatch.setattr(fds, "hackernews_volume_timeline", _hn)

        # Stub out the search backends — we don't care about volume here.
        import market_validation.demand_analysis as da
        monkeypatch.setattr(da, "_search", lambda *_a, **_kw: [])
        # Stub reddit + news so they don't hit the network
        monkeypatch.setattr(fds, "reddit_search", lambda *_a, **_kw: [])
        monkeypatch.setattr(fds, "google_news_rss", lambda *_a, **_kw: [])

        captured: dict[str, str] = {}
        def fake_ai(prompt, **_kw):
            captured["prompt"] = prompt
            return {"demand_score": 80, "demand_trend": "rising"}

        result = analyze_demand(
            "test market", "United States",
            product=None, run_ai=fake_ai, archetype="b2b-saas",
        )
        prompt = captured["prompt"]

        # Each source's identifying phrase appears in the prompt
        assert "Wikipedia pageviews [TestTopic]" in prompt
        assert "GDELT news volume" in prompt
        assert "OpenAlex publications" in prompt
        assert "GitHub repos" in prompt
        assert "HackerNews stories" in prompt
        # Consensus block surfaces all five
        assert "Cross-source consensus trend: rising" in prompt
        assert "wikipedia_pageviews" in prompt
        # Pytrends should NOT appear in the prompt anymore
        assert "Google Trends" not in prompt
        assert "pytrends" not in prompt
        # Result is propagated
        assert result["demand_score"] == 80
