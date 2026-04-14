"""
Market Archetype — pure definitions and keyword-based detection logic.

Defines 7 business archetypes with scoring weights, economics benchmarks,
and validation signals. No network calls, no AI required.
"""

from __future__ import annotations


# ---------------------------------------------------------------------------
# Archetype definitions
# ---------------------------------------------------------------------------

ARCHETYPES: dict[str, dict] = {
    "local-service": {
        "label": "Local Service Business",
        "description": "Restaurants, gyms, salons, and other local retail/service businesses that serve a geographically limited customer base.",
        "scoring_weights": {
            "demand": 0.35,
            "competitive": 0.30,
            "attractiveness": 0.20,
            "risk": 0.15,
        },
        "typical_gross_margins": {"low": 0.30, "mid": 0.55, "high": 0.70},
        "cac_range": {"low": 5, "high": 100},
        "ltv_cac_ratio": {"low": 2.0, "high": 8.0},
        "key_success_factors": [
            "Prime location and foot traffic",
            "Consistent product/service quality and customer experience",
            "Local brand reputation and word-of-mouth",
            "Lean labor cost management",
            "Repeat customer retention and loyalty programs",
        ],
        "red_flags": [
            "High fixed rent in declining foot-traffic areas",
            "Heavy dependence on a single revenue stream",
            "No sustainable differentiation from nearby competitors",
            "Thin margins with no path to improvement",
        ],
        "validation_questions": [
            "What is the repeat customer rate and average visit frequency?",
            "How does customer acquisition cost compare to lifetime value in this ZIP code?",
            "What is the realistic daily cover count / transaction volume needed to break even?",
            "Are there adjacent revenue streams (catering, retail, delivery) to offset slow periods?",
        ],
    },

    "b2b-saas": {
        "label": "B2B SaaS",
        "description": "Software sold to businesses on a recurring subscription basis, typically with low marginal cost and high scalability.",
        "scoring_weights": {
            "attractiveness": 0.35,
            "demand": 0.30,
            "competitive": 0.20,
            "risk": 0.15,
        },
        "typical_gross_margins": {"low": 0.60, "mid": 0.75, "high": 0.88},
        "cac_range": {"low": 500, "high": 15000},
        "ltv_cac_ratio": {"low": 3.0, "high": 10.0},
        "key_success_factors": [
            "Strong product-market fit validated by paying customers",
            "Net Revenue Retention (NRR) above 110%",
            "Efficient go-to-market motion (PLG or outbound)",
            "Clear ICP (ideal customer profile) with measurable ROI",
            "Scalable infrastructure with low marginal cost per seat",
        ],
        "red_flags": [
            "Annual churn above 15% with no clear retention playbook",
            "Sales cycle longer than 6 months without enterprise justification",
            "Feature parity with much larger, better-funded competitors",
            "No defensible data moat or switching cost",
        ],
        "validation_questions": [
            "What is the annual churn rate and NRR among existing customers?",
            "How long is the average sales cycle and what is the close rate?",
            "Is there a quantifiable ROI the buyer can present to their CFO?",
            "What is the payback period on CAC at current ACV and gross margin?",
        ],
    },

    "b2c-saas": {
        "label": "B2C SaaS / Consumer App",
        "description": "Consumer-facing apps, games, and subscription software sold directly to individuals rather than businesses.",
        "scoring_weights": {
            "demand": 0.35,
            "attractiveness": 0.30,
            "competitive": 0.20,
            "risk": 0.15,
        },
        "typical_gross_margins": {"low": 0.55, "mid": 0.70, "high": 0.85},
        "cac_range": {"low": 5, "high": 300},
        "ltv_cac_ratio": {"low": 2.0, "high": 6.0},
        "key_success_factors": [
            "Viral or low-cost user acquisition loop",
            "Strong Day-1/Day-7/Day-30 retention",
            "Frictionless onboarding to first value moment",
            "Monetization model aligned with user behavior (freemium, paywall, IAP)",
            "Network effects or social sharing mechanics",
        ],
        "red_flags": [
            "CAC payback longer than 12 months at consumer price points",
            "App store dependency with no owned distribution channel",
            "Core loop replicable by a single engineering sprint at a large platform",
            "Churn driven by boredom rather than solved problem",
        ],
        "validation_questions": [
            "What is the Day-30 retention rate, and how does it compare to category benchmarks?",
            "What is the organic vs. paid user acquisition split?",
            "At what price point does conversion to paid drop off significantly?",
            "How defensible is the product if Apple or Google ships a native equivalent?",
        ],
    },

    "b2b-industrial": {
        "label": "B2B Industrial / Wholesale / Distribution",
        "description": "Wholesale, distribution, manufacturing, and raw-material supply businesses selling to other businesses in industrial or food supply chains.",
        "scoring_weights": {
            "competitive": 0.30,
            "attractiveness": 0.25,
            "demand": 0.25,
            "risk": 0.20,
        },
        "typical_gross_margins": {"low": 0.10, "mid": 0.22, "high": 0.40},
        "cac_range": {"low": 200, "high": 3000},
        "ltv_cac_ratio": {"low": 3.0, "high": 8.0},
        "key_success_factors": [
            "Reliable supply chain and consistent product quality",
            "Competitive landed cost through logistics efficiency",
            "Long-term contracts or preferred-supplier agreements",
            "Deep buyer relationships and account management",
            "Ability to handle volume spikes without stockouts",
        ],
        "red_flags": [
            "Highly commoditized product with no quality or service differentiation",
            "Customer concentration above 30% in a single account",
            "Thin margins (<15%) with no clear path to operational leverage",
            "Supply chain single points of failure (single source, single route)",
        ],
        "validation_questions": [
            "What is the landed cost advantage over the incumbent supplier?",
            "What percentage of revenue comes from the top 3 customers?",
            "Is the product truly differentiated or competing purely on price?",
            "What are the working capital requirements at scale (inventory, net terms)?",
        ],
    },

    "consumer-cpg": {
        "label": "Consumer Packaged Goods (CPG)",
        "description": "Consumer packaged goods — branded food, beverage, personal care, and household products sold through retail or DTC channels.",
        "scoring_weights": {
            "demand": 0.35,
            "attractiveness": 0.30,
            "risk": 0.20,
            "competitive": 0.15,
        },
        "typical_gross_margins": {"low": 0.35, "mid": 0.50, "high": 0.65},
        "cac_range": {"low": 10, "high": 200},
        "ltv_cac_ratio": {"low": 2.0, "high": 5.0},
        "key_success_factors": [
            "Strong brand identity and visual differentiation on shelf",
            "Favorable retail velocity (units/store/week)",
            "Scalable co-manufacturing with quality control",
            "DTC channel for margin improvement and customer data",
            "Repeat purchase rate and subscription program",
        ],
        "red_flags": [
            "Slotting fees and retailer chargebacks that erode gross margin below 40%",
            "Single retail partner representing >50% of revenue",
            "Commodity ingredients with no proprietary formulation or IP",
            "No established DTC channel to backstop retail distribution loss",
        ],
        "validation_questions": [
            "What is the retail velocity, and does it meet the buyer's threshold for reorder?",
            "What is the fully-loaded COGS including co-man, logistics, and slotting?",
            "Is there a DTC or subscription component with higher LTV than retail?",
            "How does repeat purchase rate compare to category average?",
        ],
    },

    "marketplace": {
        "label": "Two-Sided Marketplace",
        "description": "Platforms that create value by connecting two or more distinct user groups (buyers and sellers, providers and consumers).",
        "scoring_weights": {
            "attractiveness": 0.40,
            "demand": 0.30,
            "risk": 0.15,
            "competitive": 0.15,
        },
        "typical_gross_margins": {"low": 0.55, "mid": 0.70, "high": 0.85},
        "cac_range": {"low": 20, "high": 500},
        "ltv_cac_ratio": {"low": 3.0, "high": 12.0},
        "key_success_factors": [
            "Solving the cold-start problem with a focused geographic or vertical launch",
            "Liquidity — ensuring demand and supply are balanced at launch",
            "Network effects that increase defensibility with scale",
            "Take rate calibrated to not drive disintermediation",
            "Trust and safety infrastructure (reviews, verification, dispute resolution)",
        ],
        "red_flags": [
            "Easy disintermediation once buyer and seller meet",
            "Lopsided supply or demand with no clear acquisition strategy for the thin side",
            "Regulatory risk (labor classification, licensing) not yet addressed",
            "Competing against a well-funded incumbent with existing liquidity",
        ],
        "validation_questions": [
            "What is the GMV retention rate — are repeat transactions happening on-platform?",
            "Is there evidence of organic supply-side growth (not all paid)?",
            "At what take rate does disintermediation become the rational choice for users?",
            "What is the minimum viable liquidity density needed in one geography to launch?",
        ],
    },

    "healthcare": {
        "label": "Healthcare",
        "description": "Medical and health services, devices, diagnostics, and digital health products with clinical or regulatory dimensions.",
        "scoring_weights": {
            "risk": 0.30,
            "attractiveness": 0.25,
            "demand": 0.25,
            "competitive": 0.20,
        },
        "typical_gross_margins": {"low": 0.40, "mid": 0.60, "high": 0.80},
        "cac_range": {"low": 50, "high": 5000},
        "ltv_cac_ratio": {"low": 2.5, "high": 8.0},
        "key_success_factors": [
            "Regulatory pathway clarity (FDA, HIPAA, CMS) established early",
            "Clinical evidence or peer-reviewed data supporting efficacy",
            "Reimbursement code or payor strategy defined",
            "Integration with existing clinical workflows (EMR, care team)",
            "HIPAA-compliant infrastructure and data governance",
        ],
        "red_flags": [
            "Regulatory pathway undefined or underestimated in timeline and cost",
            "No reimbursement strategy — solely out-of-pocket in a price-sensitive indication",
            "Clinical evidence reliant on weak or single-site studies",
            "Privacy or liability exposure not addressed in product design",
        ],
        "validation_questions": [
            "What is the regulatory classification and expected time-to-clearance or approval?",
            "Is there a clear reimbursement code (CPT/ICD) or payor willing to cover this?",
            "What clinical evidence standard is required for adoption by target buyers?",
            "How does the product integrate with existing clinical workflows and EHR systems?",
        ],
    },

    "services-agency": {
        "label": "Services / Agency",
        "description": "Consulting, legal, accounting, marketing agencies, staffing, and other professional services businesses that sell human expertise.",
        "scoring_weights": {
            "demand": 0.30,
            "competitive": 0.25,
            "attractiveness": 0.25,
            "risk": 0.20,
        },
        "typical_gross_margins": {"low": 0.35, "mid": 0.55, "high": 0.70},
        "cac_range": {"low": 500, "high": 10000},
        "ltv_cac_ratio": {"low": 2.0, "high": 7.0},
        "key_success_factors": [
            "Narrow specialization that commands premium pricing",
            "Repeatable delivery methodology to reduce key-person risk",
            "Retainer or recurring revenue mix above 50%",
            "Referral network and case study-driven lead generation",
            "Utilization rate management above 70%",
        ],
        "red_flags": [
            "Revenue concentration above 30% in one client",
            "Delivery entirely dependent on founding team — no leverage",
            "Competing on price against offshore or commoditized providers",
            "No retainer base — all project revenue with lumpy cash flow",
        ],
        "validation_questions": [
            "What percentage of revenue is retainer-based vs. one-time project?",
            "What is the revenue concentration in the top 3 clients?",
            "Is there a proprietary methodology, framework, or IP that justifies premium rates?",
            "What is the realistic utilization ceiling before headcount becomes the bottleneck?",
        ],
    },
}


# ---------------------------------------------------------------------------
# Detection logic
# ---------------------------------------------------------------------------

# Ordered list of (archetype_key, keyword_list) — checked in priority order.
# More specific archetypes come first to avoid false matches.
# b2b-saas is checked before marketplace because "platform" appears in both;
# the compound rule (software signal + B2B signal) disambiguates correctly.
_DETECTION_RULES: list[tuple[str, list[str]]] = [
    # Most specific / highest-commitment archetypes first
    ("healthcare",       ["medical", "health", "clinic", "hospital", "pharma", "dental", "therapy", "wellness"]),
    ("marketplace",      ["marketplace", "two-sided", "connect buyers", "connect sellers"]),
    ("b2b-saas",         ["saas", "software", "api", "platform", "enterprise", "business", "b2b", "company", "team", "management"]),
    ("b2c-saas",         ["consumer app", "mobile app", "game", "subscription box"]),
    # local-service before b2b-industrial so "restaurant" beats ambiguous food keywords
    ("local-service",    ["restaurant", "food service", "catering", "cafe", "gym", "salon", "bbq", "barbecue", "diner", "eatery"]),
    # b2b-industrial requires clear B2B supply-chain signals — "brisket"/"meat" removed (too ambiguous)
    ("b2b-industrial",   ["wholesale", "distribution", "distributor", "manufacturer", "supplier",
                          "industrial", "logistics", "raw material", "produce supply", "ingredient supply"]),
    ("consumer-cpg",     ["consumer product", "cpg", "packaged", "retail brand", "beverage", "food product"]),
    ("services-agency",  ["consulting", "agency", "legal", "accounting", "staffing", "marketing services"]),
]

# b2b-saas requires both a software signal AND a B2B signal
_B2B_SAAS_SOFTWARE_SIGNALS = {"saas", "software", "api", "platform"}
_B2B_SAAS_BUSINESS_SIGNALS = {"enterprise", "business", "b2b", "company", "team", "management", "workflow", "productivity"}


def detect_archetype(market: str, product: str | None = None) -> tuple[str, int]:
    """
    Detect the most likely archetype for a market + product combination.

    Returns (archetype_key, confidence) where confidence is 0-100.
    """
    combined = (market + " " + (product or "")).lower()

    for archetype_key, keywords in _DETECTION_RULES:
        # Special compound rule for b2b-saas: needs a software signal AND a B2B signal
        if archetype_key == "b2b-saas":
            software_hits = [kw for kw in _B2B_SAAS_SOFTWARE_SIGNALS if kw in combined]
            business_hits = [kw for kw in _B2B_SAAS_BUSINESS_SIGNALS if kw in combined]
            if not (software_hits and business_hits):
                continue
            total_hits = len(software_hits) + len(business_hits)
        else:
            total_hits = sum(1 for kw in keywords if kw in combined)
            if total_hits == 0:
                continue

        if total_hits >= 3:
            confidence = 85
        elif total_hits == 2:
            confidence = 65
        else:
            confidence = 45

        return archetype_key, confidence

    # Default fallback
    return "b2b-industrial", 25


def get_archetype_config(archetype_key: str) -> dict:
    """
    Return the archetype config dict for the given key.
    Falls back to b2b-industrial if the key is unknown.
    """
    return ARCHETYPES.get(archetype_key, ARCHETYPES["b2b-industrial"])
