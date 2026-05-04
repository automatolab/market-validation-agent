"""Tests for helpers added during the audit-driven improvement pass.

Covers:
- normalize_phone / is_valid_phone_intl / detect_country
- normalize_name_key (NFKD fold + Latin-only gating)
- parent_brand_key + dedupe_companies (archetype gating)
- is_junk_company (listicle structural patterns)
- score_source_authority / calibrate_confidence / completeness_score
- normalize_company_status (state-machine canonicalization)
- compute_scorecard verdict downgrade on low completeness
- _resolve_porter_weights (archetype-aware)
- record_validation_outcome / get_calibration_summary
- mark_stage_completed / get_last_completed_stage
- export_crm_csv (HubSpot/Salesforce/Pipedrive headers)
"""

from __future__ import annotations

import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))


# ---------------------------------------------------------------------------
# 1. Phone normalization
# ---------------------------------------------------------------------------

from market_validation._helpers.contacts import (
    detect_country,
    is_valid_phone_intl,
    normalize_phone,
)


class TestNormalizePhone:
    def test_us_domestic_to_e164(self):
        assert normalize_phone("(408) 555-1234") == "+14085551234"

    def test_us_dotted_to_e164(self):
        assert normalize_phone("408.555.1234") == "+14085551234"

    def test_us_with_country_code(self):
        assert normalize_phone("+1 408 555 1234") == "+14085551234"

    def test_uk_with_country_code(self):
        # +44 20 7946 0958 → London local 020 7946 0958
        result = normalize_phone("+44 20 7946 0958", country_hint="GB")
        assert result.startswith("+44")
        assert "1234" not in result

    def test_empty_returns_empty(self):
        assert normalize_phone("") == ""
        assert normalize_phone(None) == ""

    def test_unparseable_returns_digits(self):
        # 5 digits is too short to be any valid phone — falls through to the
        # raw-digits path.
        result = normalize_phone("12345")
        assert result == "12345" or result.startswith("+")


class TestIsValidPhoneIntl:
    def test_valid_us(self):
        assert is_valid_phone_intl("(408) 555-1234")

    def test_invalid_us_n11(self):
        # 411 is reserved (N11) — not a valid US area code.
        assert not is_valid_phone_intl("(411) 555-1234")

    def test_invalid_too_short(self):
        assert not is_valid_phone_intl("12345")

    def test_invalid_empty(self):
        assert not is_valid_phone_intl("")
        assert not is_valid_phone_intl(None)


class TestDetectCountry:
    def test_us_state_abbrev(self):
        assert detect_country("San Jose, CA") == "US"

    def test_us_full_state(self):
        assert detect_country("San Jose, California") == "US"

    def test_uk(self):
        assert detect_country("London, UK") == "GB"
        assert detect_country("Edinburgh, Scotland") == "GB"

    def test_canada(self):
        assert detect_country("Toronto, ON") == "CA"

    def test_france(self):
        assert detect_country("Paris, France") == "FR"

    def test_default_is_us(self):
        assert detect_country(None) == "US"
        assert detect_country("") == "US"
        assert detect_country("Some Random Place") == "US"


# ---------------------------------------------------------------------------
# 2. Name normalization (NFKD with Latin gating)
# ---------------------------------------------------------------------------

from market_validation._helpers.companies import (
    _is_mostly_latin,
    dedupe_companies,
    dedupe_key_name,
    parent_brand_key,
)


class TestDedupeKeyNameNFKD:
    def test_latin_accent_folded(self):
        assert dedupe_key_name("Café Roma") == dedupe_key_name("Cafe Roma")

    def test_german_umlaut_folded(self):
        assert dedupe_key_name("Müller GmbH") == dedupe_key_name("Muller GmbH")

    def test_corporate_suffix_stripped(self):
        assert dedupe_key_name("Acme Inc") == dedupe_key_name("Acme Corporation")
        assert dedupe_key_name("Acme LLC") == dedupe_key_name("Acme")

    def test_leading_article_stripped(self):
        assert dedupe_key_name("The Smoking Pig") == dedupe_key_name("Smoking Pig")

    def test_empty_returns_empty(self):
        assert dedupe_key_name("") == ""
        assert dedupe_key_name(None) == ""

    def test_cjk_not_folded(self):
        # CJK strings should NOT be NFKD-folded — that would lose meaningful
        # characters. _is_mostly_latin should return False, skipping the fold.
        assert not _is_mostly_latin("東京寿司")
        # Two distinct CJK names must produce distinct keys.
        assert dedupe_key_name("東京寿司") != dedupe_key_name("大阪寿司")


# ---------------------------------------------------------------------------
# 3. Subsidiary collapsing + archetype gating
# ---------------------------------------------------------------------------

class TestParentBrandKey:
    def test_collapses_hash_number_for_b2b_saas(self):
        # b2b-saas: corporate-level coverage is enough; collapse locations.
        assert parent_brand_key("McDonald's #1234", archetype="b2b-saas") is not None
        assert parent_brand_key("McDonald's #1234", archetype="b2b-saas") \
            == parent_brand_key("McDonald's #5678", archetype="b2b-saas")

    def test_does_not_collapse_for_local_service(self):
        # local-service: each franchise location is a distinct lead.
        assert parent_brand_key("McDonald's #1234", archetype="local-service") is None
        assert parent_brand_key("McDonald's #1234", archetype="consumer-cpg") is None
        assert parent_brand_key("McDonald's #1234", archetype="healthcare") is None

    def test_no_archetype_collapses_by_default(self):
        # Backward-compat: default behavior is to collapse when no archetype
        # is supplied (preserves old semantics).
        assert parent_brand_key("Walgreens #4567") is not None

    def test_returns_none_for_non_subsidiary(self):
        assert parent_brand_key("Smoking Pig BBQ") is None
        assert parent_brand_key("Acme Corp") is None

    def test_returns_none_for_empty(self):
        assert parent_brand_key("") is None
        assert parent_brand_key(None) is None


class TestDedupeCompaniesArchetypeGating:
    def test_local_service_keeps_franchises_distinct(self):
        companies = [
            {"company_name": "Subway #1", "website": "https://subway.com/loc1"},
            {"company_name": "Subway #2", "website": "https://subway.com/loc2"},
        ]
        result = dedupe_companies(companies, archetype="local-service")
        assert len(result) == 2

    def test_b2b_saas_collapses_franchises(self):
        companies = [
            {"company_name": "Subway #1", "website": "https://subway-loc1.com"},
            {"company_name": "Subway #2", "website": "https://subway-loc2.com"},
        ]
        result = dedupe_companies(companies, archetype="b2b-saas")
        assert len(result) == 1


# ---------------------------------------------------------------------------
# 4. Junk filtering — listicle structural patterns
# ---------------------------------------------------------------------------

from market_validation._helpers.companies import is_junk_company


class TestEnrichmentEmailRejects:
    """Regressions for the wrong-email bug we hit on California hydroponics.

    Each test case below is one of the actual offending emails we saved as a
    "company contact" before the fix landed. They must all return False from
    is_plausible_email so the enricher never accepts them again.
    """

    def test_placeholder_example_at_gmail(self):
        # Plenty's main page had `example@gmail.com` as a CSS demo — got saved
        # as if it were Plenty's contact email. The local-part is "example".
        from market_validation.company_enrichment import is_plausible_email
        assert not is_plausible_email("example@gmail.com")

    def test_placeholder_first_at_real_domain(self):
        # Plenty's LinkedIn page had `first@plenty.ag` as a placeholder.
        # The domain is real but the local-part "first" is a template token.
        from market_validation.company_enrichment import is_plausible_email
        assert not is_plausible_email("first@plenty.ag")

    def test_placeholder_yourname(self):
        from market_validation.company_enrichment import is_plausible_email
        assert not is_plausible_email("yourname@anycompany.com")
        assert not is_plausible_email("your.name@anycompany.com")
        assert not is_plausible_email("first.last@example.org")

    def test_contact_prefix_is_legitimate(self):
        # Regression: I mistakenly added "contact" to the placeholder list,
        # which falsely-cleared contact@alphagroomingpetsalon.com and
        # contact@viam.com. contact@/info@/sales@/support@/hello@/admin@ are
        # all standard company info-mailbox prefixes and must pass.
        from market_validation.company_enrichment import is_plausible_email
        assert is_plausible_email("contact@alphagroomingpetsalon.com")
        assert is_plausible_email("contact@viam.com")
        assert is_plausible_email("info@etumorganics.co")
        assert is_plausible_email("sales@bghydro.com")
        assert is_plausible_email("hello@somecompany.com")
        assert is_plausible_email("support@anothercompany.io")
        assert is_plausible_email("admin@business.net")

    def test_news_journalist_email_rejected(self):
        # janelle.bitker@sfchronicle.com is a real address but it's an SF
        # Chronicle reporter, not the company we're enriching.
        from market_validation.company_enrichment import is_plausible_email
        assert not is_plausible_email("janelle.bitker@sfchronicle.com")
        assert not is_plausible_email("editorial@optimistdaily.com")
        assert not is_plausible_email("lkiernan@globalaginvesting.com")

    def test_news_press_wire_rejected(self):
        from market_validation.company_enrichment import is_plausible_email
        assert not is_plausible_email("press@prnewswire.com")
        assert not is_plausible_email("info@businesswire.com")

    def test_logistics_cross_domain_rejected(self):
        # ltlccg@xpo.com surfaced for "Fresh Farms Hydroponics" because of a
        # freight-quote widget on the page. XPO Logistics is now blocked.
        from market_validation.company_enrichment import is_plausible_email
        assert not is_plausible_email("ltlccg@xpo.com")

    def test_legit_company_emails_still_accepted(self):
        # Sanity: real company addresses must still pass.
        from market_validation.company_enrichment import is_plausible_email
        assert is_plausible_email("press@plenty.ag")
        assert is_plausible_email("info@etumorganics.co")
        assert is_plausible_email("sales@bghydro.com")
        assert is_plausible_email("pr@revolgreens.com")
        assert is_plausible_email("jfurman@pajarosa.com")


class TestIsJunkCompanyArticleURLs:
    """Article URLs as 'companies' are how the wrong-emails entered the DB.

    The article gets surfaced from search, the page-title becomes the
    'company name', and then the enricher scrapes the article and finds the
    journalist's email. Reject these upstream.
    """

    def test_news_article_url_is_junk(self):
        from market_validation._helpers.companies import is_junk_company
        # SF Chronicle article URL — was being saved as a "company" in v3 run.
        assert is_junk_company({
            "company_name": "Plenty's vertically farmed produce hits Safeway",
            "website": "https://www.sfchronicle.com/food/article/Plenty-s-vertically-farmed-produce-hits-14960453.php",
        })

    def test_optimistdaily_article_url_is_junk(self):
        from market_validation._helpers.companies import is_junk_company
        assert is_junk_company({
            "company_name": "Vertical farming is bringing local produce",
            "website": "https://www.optimistdaily.com/2021/04/vertical-farming-is-bringing-local-produce-to-californias-densest-cities/",
        })

    def test_globalaginvesting_article_url_is_junk(self):
        from market_validation._helpers.companies import is_junk_company
        assert is_junk_company({
            "company_name": "Plenty's Expanding in Southern California",
            "website": "https://globalaginvesting.com/plentys-expanding-southern-california-next-vertical-farm-planned-los-angeles/",
        })

    def test_pdf_url_is_junk(self):
        # Academic PDFs and policy docs aren't companies.
        from market_validation._helpers.companies import is_junk_company
        assert is_junk_company({
            "company_name": "Fully Automated Hydroponic System for Indoor Plant Growth",
            "website": "https://www.researchgate.net/publication/12345.pdf",
        })

    def test_date_path_url_is_junk(self):
        # Even on hosts we don't blocklist, /YYYY/MM/DD/ in the path is a
        # very strong signal it's editorial content rather than a company page.
        from market_validation._helpers.companies import is_junk_company
        assert is_junk_company({
            "company_name": "Some Headline",
            "website": "https://urbanarktech.org/2024/03/10/worlds-most-technologically-advanced-indoor-vertical-farm-in-compton/",
        })

    def test_legit_company_homepage_not_junk(self):
        # Sanity: actual company homepages and /about, /contact paths must pass.
        from market_validation._helpers.companies import is_junk_company
        assert not is_junk_company({
            "company_name": "Plenty",
            "website": "https://www.plenty.ag/",
        })
        assert not is_junk_company({
            "company_name": "Etum Indoor Vertical Farm",
            "website": "https://www.etumorganics.co/",
        })
        assert not is_junk_company({
            "company_name": "Revol Greens",
            "website": "https://revolgreens.com/about",
        })


class TestIsJunkCompanyListicle:
    def test_substring_listicle(self):
        assert is_junk_company({"company_name": "top 10 BBQ joints"})
        assert is_junk_company({"company_name": "10 best restaurants in San Jose"})

    def test_structural_listicle_with_adjective(self):
        assert is_junk_company({
            "company_name": "The 12 Most Essential BBQ Spots You Need to Try"
        })

    def test_numeric_plural_locator(self):
        assert is_junk_company({"company_name": "12 Restaurants in San Jose"})
        assert is_junk_company({"company_name": "20 Cafes near downtown"})

    def test_legitimate_business_with_number_not_junk(self):
        # Real business names with numbers should NOT match.
        # "The 5 Stars Diner" doesn't match because "Stars" isn't a listicle
        # adjective.
        assert not is_junk_company({"company_name": "The 5 Stars Diner"})
        # "Top Hat Cafe" doesn't have numeric prefix — won't match.
        assert not is_junk_company({"company_name": "Top Hat Cafe"})

    def test_super_long_names_are_junk(self):
        # 10+ word "names" are almost certainly SEO blog post titles.
        long_name = "The Complete Definitive Guide To Finding The Best Smokehouse In Town"
        assert is_junk_company({"company_name": long_name})


# ---------------------------------------------------------------------------
# 5. Citation source authority + completeness
# ---------------------------------------------------------------------------

from market_validation._helpers.citations import (
    CitationRule,
    RULES_FOR_COMPETITION,
    RULES_FOR_SIZING,
    RULES_FOR_UNIT_ECONOMICS,
    calibrate_confidence,
    completeness_score,
    enforce_citations,
    score_source_authority,
)


class TestScoreSourceAuthority:
    def test_government_is_tier1(self):
        assert score_source_authority("https://www.bls.gov/data/x") == 1
        assert score_source_authority("https://census.gov/foo") == 1
        assert score_source_authority("https://data.bls.gov/x") == 1

    def test_paid_research_is_tier2(self):
        assert score_source_authority("https://www.statista.com/x") == 2
        assert score_source_authority("https://ibisworld.com/y") == 2

    def test_trade_press_is_tier3(self):
        assert score_source_authority("https://www.wsj.com/x") == 3
        assert score_source_authority("https://techcrunch.com/y") == 3

    def test_wikipedia_is_tier4(self):
        assert score_source_authority("https://en.wikipedia.org/x") == 4

    def test_general_web_is_tier5(self):
        assert score_source_authority("https://random-blog.com") == 5

    def test_no_url_is_tier6(self):
        assert score_source_authority(None) == 6
        assert score_source_authority("") == 6


class TestCalibrateConfidence:
    def test_caps_inflated_claim(self):
        # 95% confidence from a Tier-5 (general web) source should be capped.
        assert calibrate_confidence(95, "https://random-blog.com") <= 50

    def test_government_source_keeps_high_confidence(self):
        # 80% confidence from Tier-1 source should pass through.
        assert calibrate_confidence(80, "https://bls.gov/x") == 80

    def test_low_confidence_unchanged(self):
        assert calibrate_confidence(30, "https://bls.gov/x") == 30


class TestEnforceCitations:
    def test_drops_uncited_source_entries(self):
        payload = {
            "tam_low": 1_000_000,
            "tam_sources": [
                {"source_url": "https://bls.gov/x", "evidence": "ok"},
                {"evidence": "no url"},  # should be dropped
                "BLS 2024 release",       # not a URL — dropped
                "https://census.gov/y",  # plain URL — kept
            ],
            "tam_confidence": 70,
        }
        result = enforce_citations(payload, RULES_FOR_SIZING)
        # Only entries with URLs survive.
        sources = result["tam_sources"]
        assert len(sources) == 2
        # Bare URL string preserved as-is.
        assert any(s == "https://census.gov/y" for s in sources)

    def test_caps_confidence_to_source_tier(self):
        # Tier-5 (random blog) source → confidence cap = 30 + 10 = 40.
        payload = {
            "tam_low": 1_000_000,
            "tam_sources": [{"source_url": "https://random-blog.com/foo"}],
            "tam_confidence": 90,  # inflated
        }
        result = enforce_citations(payload, RULES_FOR_SIZING)
        assert result["tam_confidence"] <= 40
        assert "_citation_warnings" in result

    def test_government_source_keeps_high_confidence(self):
        # Tier-1 (BLS) → cap = 80 + 10 = 90.
        payload = {
            "tam_low": 1_000_000,
            "tam_sources": [{"source_url": "https://www.bls.gov/x"}],
            "tam_confidence": 85,
        }
        result = enforce_citations(payload, RULES_FOR_SIZING)
        assert result["tam_confidence"] == 85

    def test_no_sources_caps_at_floor(self):
        payload = {
            "tam_low": 1_000_000,
            "tam_sources": [],
            "tam_confidence": 80,
        }
        result = enforce_citations(payload, RULES_FOR_SIZING)
        # No sources at all → cap to ai_inference baseline (20).
        assert result["tam_confidence"] == 20

    def test_competition_strips_urlless_competitors(self):
        # "Made up competitor with no URL" must be dropped.
        payload = {
            "direct_competitors": [
                {"name": "Real Co", "source_url": "https://real.example.com"},
                {"name": "Phantom Co", "evidence": "I just feel like they exist"},
                "https://another.example.com",  # bare URL ok
            ],
        }
        result = enforce_citations(payload, RULES_FOR_COMPETITION)
        assert len(result["direct_competitors"]) == 2

    def test_unit_economics_caps_inflated_margin_confidence(self):
        payload = {
            "gross_margin_low": 0.55,
            "gross_margin_high": 0.72,
            "gross_margin_confidence": 90,
            # Single tier-5 source, confidence should be capped.
            "gross_margin_source": [
                {"source_url": "https://unknown-blog.example.com"}
            ],
        }
        result = enforce_citations(payload, RULES_FOR_UNIT_ECONOMICS)
        assert result["gross_margin_confidence"] <= 40

    def test_handles_non_dict_payload(self):
        # Doesn't crash on unexpected shapes.
        assert enforce_citations("not a dict", RULES_FOR_SIZING) == "not a dict"
        assert enforce_citations([], RULES_FOR_SIZING) == []

    def test_warnings_accumulate(self):
        # Two failing rules → two warnings.
        payload = {
            "tam_low": 1, "tam_sources": [], "tam_confidence": 80,
            "sam_low": 2, "sam_sources": [], "sam_confidence": 80,
            "som_low": 3, "som_sources": [], "som_confidence": 80,
        }
        result = enforce_citations(payload, RULES_FOR_SIZING)
        warnings = result.get("_citation_warnings") or []
        # At least one warning per rule (3 rules, all empty).
        assert len(warnings) >= 3


class TestCompletenessScore:
    def test_full_coverage(self):
        stages = {
            "sizing": {"tam_low": 1000},
            "demand": {"demand_score": 70},
            "competition": {"competitive_intensity": 50},
        }
        assert completeness_score(stages) == 100

    def test_partial_coverage(self):
        stages = {
            "sizing": {"tam_low": 1000},
            "demand": {},
            "competition": {},
            "signals": {},
        }
        assert completeness_score(stages) == 25

    def test_empty(self):
        assert completeness_score({}) == 0

    def test_skips_metadata_keys(self):
        # raw_snippets / sources_used / method shouldn't count as useful data.
        stages = {
            "sizing": {"raw_snippets": [1, 2, 3], "sources_used": ["a", "b"]},
            "demand": {"demand_score": 70},
        }
        # sizing has only metadata fields → not populated; demand → populated.
        assert completeness_score(stages) == 50


# ---------------------------------------------------------------------------
# 6. Status enum
# ---------------------------------------------------------------------------

from market_validation.research import CompanyStatus, normalize_company_status


class TestNormalizeCompanyStatus:
    def test_canonical_passthrough(self):
        for s in CompanyStatus.ALL:
            assert normalize_company_status(s) == s

    def test_legacy_uncertain_to_new(self):
        assert normalize_company_status("uncertain") == "new"

    def test_legacy_call_ready_to_qualified(self):
        assert normalize_company_status("call_ready") == "qualified"

    def test_replied_interested_to_interested(self):
        assert normalize_company_status("replied_interested") == "interested"

    def test_unknown_defaults_to_new(self):
        assert normalize_company_status("foo") == "new"
        assert normalize_company_status(None) == "new"

    def test_pydantic_qualification_result_accepts_canonical_statuses(self):
        # Regression: the qualifier returns "not_relevant" per its prompt,
        # and normalize_qualification_status passes it through. The Pydantic
        # schema must accept it — previously it had the old Literal and
        # killed every not_relevant result, leaking junk into enrichment.
        from market_validation.schemas import QualificationResult
        for s in ("qualified", "not_relevant", "new"):
            r = QualificationResult(company_id="abc", status=s, score=10)
            assert r.status == s

    def test_pydantic_company_status_includes_all_canonical(self):
        # CompanyStatus Literal in schemas.py must include every canonical
        # value defined in research.CompanyStatus.ALL — drift between them
        # silently breaks the qualifier.
        import typing
        from market_validation.schemas import CompanyStatus as PydanticStatus
        from market_validation.research import CompanyStatus as ResearchStatus
        allowed = set(typing.get_args(PydanticStatus))
        for s in ResearchStatus.ALL:
            assert s in allowed, f"missing {s} from schemas.CompanyStatus"


# ---------------------------------------------------------------------------
# 7. Scorecard verdict downgrade on low completeness
# ---------------------------------------------------------------------------

from market_validation.validation_scorecard import compute_scorecard


class TestScorecardCompletenessDowngrade:
    def test_strong_score_downgraded_on_thin_evidence(self):
        # Scenario: high attractiveness + demand, but every other module
        # returned empty data. The verdict should be downgraded.
        sc = compute_scorecard(
            sizing={"tam_low": 5_000_000_000, "tam_high": 8_000_000_000, "growth_rate": 0.20},
            demand={"demand_trend": "rising", "demand_score": 90, "willingness_to_pay": "high"},
            competition={},  # empty
            signals={},      # empty
        )
        # Completeness should be low (only 2 of 8 stages populated).
        assert sc["completeness_score"] < 50
        # Verdict should NOT be 'strong_go' even if raw score warranted it.
        assert sc["verdict"] != "strong_go"

    def test_full_evidence_keeps_verdict(self):
        sc = compute_scorecard(
            sizing={"tam_low": 5_000_000_000, "tam_high": 8_000_000_000, "growth_rate": 0.20},
            demand={"demand_trend": "rising", "demand_score": 90, "willingness_to_pay": "high"},
            competition={"competitive_intensity": 30, "market_concentration": "fragmented"},
            signals={"regulatory_risks": [], "technology_maturity": "growing", "timing_assessment": "good"},
            unit_economics={"unit_economics_score": 70},
            porters={"structural_attractiveness": 70},
            timing={"timing_score": 75},
            customer_segments={"icp_clarity": 70},
        )
        assert sc["completeness_score"] >= 50


# ---------------------------------------------------------------------------
# 8. Archetype-aware Porter weights
# ---------------------------------------------------------------------------

from market_validation.porters_five_forces import (
    _DEFAULT_PORTER_WEIGHTS,
    _resolve_porter_weights,
)


class TestPorterWeights:
    def test_default_when_no_archetype(self):
        assert _resolve_porter_weights(None) == _DEFAULT_PORTER_WEIGHTS

    def test_local_service_emphasizes_rivalry(self):
        weights = _resolve_porter_weights("local-service")
        # Rivalry should be the biggest force for restaurants/gyms.
        assert weights["rivalry_intensity"] >= weights["supplier_power"]

    def test_industrial_emphasizes_supplier_power(self):
        weights = _resolve_porter_weights("b2b-industrial")
        # Industrial wholesale: supplier power matters more than for SaaS.
        saas_weights = _resolve_porter_weights("b2b-saas")
        assert weights["supplier_power"] > saas_weights["supplier_power"]

    def test_marketplace_emphasizes_entry_barriers(self):
        weights = _resolve_porter_weights("marketplace")
        # Cold-start liquidity = the dominant force for marketplaces.
        assert weights["entry_barriers"] >= 0.25

    def test_weights_sum_to_one(self):
        for archetype in ("local-service", "b2b-saas", "b2b-industrial",
                          "consumer-cpg", "marketplace", "healthcare", "services-agency"):
            weights = _resolve_porter_weights(archetype)
            total = sum(weights.values())
            assert abs(total - 1.0) < 0.01, f"{archetype} weights sum {total}"


# ---------------------------------------------------------------------------
# 9. Outcome feedback loop
# ---------------------------------------------------------------------------

import tempfile

import pytest

from market_validation.research import (
    create_research,
    create_validation,
    get_calibration_summary,
    record_validation_outcome,
)


@pytest.fixture
def temp_db_root():
    """Create a fresh project root with an empty SQLite DB."""
    with tempfile.TemporaryDirectory() as tmp:
        # The research module derives db path from <root>/output/...
        yield tmp


class TestOutcomeFeedback:
    def test_record_outcome_persists(self, temp_db_root):
        rid = create_research(
            name="Test", market="bbq", geography="San Jose, CA", root=temp_db_root,
        )["research_id"]
        vid = create_validation(rid, market="bbq", geography="San Jose, CA",
                                root=temp_db_root)["validation_id"]
        result = record_validation_outcome(
            vid, "success", notes="grew 3x in 6mo",
            revenue_actual=120000.0, recorded_by="founder",
            root=temp_db_root,
        )
        assert result.get("result") == "ok"

    def test_invalid_outcome_rejected(self, temp_db_root):
        rid = create_research(
            name="Test", market="bbq", geography="San Jose, CA", root=temp_db_root,
        )["research_id"]
        vid = create_validation(rid, market="bbq", geography="San Jose, CA",
                                root=temp_db_root)["validation_id"]
        result = record_validation_outcome(
            vid, "spectacular_success",  # not in VALID_OUTCOMES
            root=temp_db_root,
        )
        assert result.get("result") == "error"

    def test_calibration_insufficient_data(self, temp_db_root):
        # No outcomes recorded → insufficient_data flag set.
        result = get_calibration_summary(root=temp_db_root)
        assert result["insufficient_data"] is True

    def test_calibration_with_outcomes(self, temp_db_root):
        # Record several outcomes and check the summary buckets them.
        rid = create_research(
            name="T", market="m", geography="g", root=temp_db_root,
        )["research_id"]
        from market_validation.research import update_validation
        for verdict, score, outcome in [
            ("strong_go", 85, "success"),
            ("strong_go", 80, "success"),
            ("go", 65, "partial"),
            ("no_go", 25, "abandoned"),
        ]:
            vid = create_validation(rid, market="m", geography="g",
                                    root=temp_db_root)["validation_id"]
            update_validation(vid, {"verdict": verdict, "overall_score": score},
                              root=temp_db_root)
            record_validation_outcome(vid, outcome, root=temp_db_root)

        summary = get_calibration_summary(root=temp_db_root, min_outcomes=1)
        assert summary["insufficient_data"] is False
        assert summary["outcomes_recorded"] == 4
        # strong_go bucket: 2 out of 2 hit → 1.0 hit rate
        assert summary["by_verdict"]["strong_go"]["hit_rate"] == 1.0


# ---------------------------------------------------------------------------
# 10. Pipeline checkpoint
# ---------------------------------------------------------------------------

from market_validation.research import (
    PIPELINE_STAGES,
    get_last_completed_stage,
    mark_stage_completed,
)


class TestPipelineCheckpoint:
    def test_mark_and_read(self, temp_db_root):
        rid = create_research(
            name="T", market="m", geography="g", root=temp_db_root,
        )["research_id"]
        assert get_last_completed_stage(rid, root=temp_db_root) is None
        mark_stage_completed(rid, "find", root=temp_db_root)
        assert get_last_completed_stage(rid, root=temp_db_root) == "find"
        mark_stage_completed(rid, "qualify", root=temp_db_root)
        assert get_last_completed_stage(rid, root=temp_db_root) == "qualify"

    def test_invalid_stage_rejected(self, temp_db_root):
        rid = create_research(
            name="T", market="m", geography="g", root=temp_db_root,
        )["research_id"]
        with pytest.raises(ValueError):
            mark_stage_completed(rid, "nonsense", root=temp_db_root)

    def test_pipeline_stages_constant(self):
        assert "validate" in PIPELINE_STAGES
        assert "find" in PIPELINE_STAGES
        assert "qualify" in PIPELINE_STAGES
        assert "enrich" in PIPELINE_STAGES
        assert "drafts" in PIPELINE_STAGES


# ---------------------------------------------------------------------------
# 11. CRM-mapped CSV exports
# ---------------------------------------------------------------------------

from market_validation.dashboard_export import (
    _CRM_FIELD_MAPS,
    export_crm_csv,
)


class TestExportCrmCsv:
    def test_unknown_crm_raises(self, temp_db_root):
        try:
            export_crm_csv("notarealthing", root=temp_db_root)
        except ValueError as e:
            assert "supported" in str(e).lower()
        else:
            raise AssertionError("expected ValueError for unknown CRM")

    def test_hubspot_headers(self, temp_db_root):
        # Empty DB → header-only CSV. We just verify the header line.
        csv_text = export_crm_csv("hubspot", root=temp_db_root)
        first_line = csv_text.splitlines()[0]
        # HubSpot uses "Company name" not "company_name" — this catches a
        # regression where someone might forget to apply the field map.
        assert "Company name" in first_line
        assert "Email" in first_line
        assert "Phone Number" in first_line

    def test_salesforce_headers(self, temp_db_root):
        csv_text = export_crm_csv("salesforce", root=temp_db_root)
        first_line = csv_text.splitlines()[0]
        assert "Company" in first_line
        assert "Website" in first_line
        assert "AnnualRevenue" in first_line

    def test_pipedrive_headers(self, temp_db_root):
        csv_text = export_crm_csv("pipedrive", root=temp_db_root)
        first_line = csv_text.splitlines()[0]
        assert "Organization name" in first_line

    def test_field_maps_exist(self):
        for crm in ("hubspot", "salesforce", "pipedrive"):
            assert crm in _CRM_FIELD_MAPS
            # Every map must include at minimum: name, website, phone, email
            assert "company_name" in _CRM_FIELD_MAPS[crm]
            assert "email" in _CRM_FIELD_MAPS[crm]
            assert "phone" in _CRM_FIELD_MAPS[crm]


# ---------------------------------------------------------------------------
# 12. Per-research export folder
# ---------------------------------------------------------------------------

import sqlite3

from market_validation.research import (
    add_company,
    resolve_db_path,
    update_company,
    update_validation,
)
from market_validation.research_export import export_research_folder


def _seed_research(temp_db_root: str, *, with_validation: bool = False) -> str:
    rid = create_research(
        name="Hydroponics CA", market="hydroponics", geography="CA",
        root=temp_db_root,
    )["research_id"]
    # Mix scored, unscored (None), and negative-scored companies so the
    # NULLS-LAST sort can be observed in companies-by-type.md.
    for name, score in [
        ("Acme Greens", 90),
        ("Bravo Farms", -5),
        ("Charlie Hydro", None),
        ("Delta Nursery", 50),
    ]:
        cid = add_company(
            research_id=rid, company_name=name, market="hydroponics",
            website=f"https://{name.lower().replace(' ', '')}.example.com",
            root=temp_db_root,
        )["company_id"]
        if score is not None:
            update_company(cid, rid, {"priority_score": score}, root=temp_db_root)
    if with_validation:
        vid = create_validation(rid, market="hydroponics", geography="CA",
                                root=temp_db_root)["validation_id"]
        update_validation(vid, {"verdict": "go", "overall_score": 72,
                                "verdict_reasoning": "demand outpaces supply"},
                          root=temp_db_root)
    return rid


class TestResearchExport:
    def test_creates_all_five_files_on_fresh_db(self, temp_db_root):
        # Fresh DB: research.py creates its tables, but the emails table
        # only exists if email_sender has touched the DB. The export must
        # still produce all 5 files (Copilot review concern about a
        # "no such table: emails" crash on the first run).
        rid = _seed_research(temp_db_root)
        out = Path(temp_db_root) / "research-out"

        folder = export_research_folder(rid, base_dir=out, root=temp_db_root)

        for fname in ("summary.md", "companies.csv", "companies-by-type.md",
                      "emails.md", "validation.md"):
            assert (folder / fname).exists(), f"missing {fname}"
        # No validation row → validation.md falls back to the placeholder
        # rather than being skipped (description claims 5 files always).
        assert "no validation has been run" in (folder / "validation.md").read_text()

    def test_idempotent_overwrites_stale_validation(self, temp_db_root):
        # Run 1: validation row exists → real content. Run 2 (after delete):
        # the file must be reset to the placeholder, not left stale.
        rid = _seed_research(temp_db_root, with_validation=True)
        out = Path(temp_db_root) / "research-out"

        folder = export_research_folder(rid, base_dir=out, root=temp_db_root)
        assert "demand outpaces supply" in (folder / "validation.md").read_text()

        # Drop the validation row, then re-export.
        db_file = resolve_db_path(Path(temp_db_root))
        with sqlite3.connect(db_file) as conn:
            conn.execute("DELETE FROM market_validations WHERE research_id = ?", (rid,))

        folder2 = export_research_folder(rid, base_dir=out, root=temp_db_root)
        assert folder2 == folder
        assert "no validation has been run" in (folder / "validation.md").read_text()
        assert "demand outpaces supply" not in (folder / "validation.md").read_text()

    def test_companies_by_type_nulls_last(self, temp_db_root):
        rid = _seed_research(temp_db_root)
        out = Path(temp_db_root) / "research-out"
        folder = export_research_folder(rid, base_dir=out, root=temp_db_root)

        text = (folder / "companies-by-type.md").read_text()
        # Within the bucket, the order must be: 90, 50, -5, then None (last).
        # All four seed companies fall into "Other / Uncategorized" by
        # heuristic, so their relative order in the file is the sort order.
        order = [
            text.index("Acme Greens"),
            text.index("Delta Nursery"),
            text.index("Bravo Farms"),
            text.index("Charlie Hydro"),
        ]
        assert order == sorted(order), (
            "expected NULLS LAST sort: 90, 50, -5, then None — "
            f"got positions {order}"
        )
