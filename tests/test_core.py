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
