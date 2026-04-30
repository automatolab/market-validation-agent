"""Archetype-specific prompt context strings.

Each business archetype (b2b-saas, local-service, consumer-cpg, etc.) gets a
tailored context block injected into AI prompts so search and qualification
optimize for the right kind of lead.
"""

from __future__ import annotations


def archetype_search_context(
    archetype_key: str, market: str, geography: str, product: str | None
) -> str:
    """Return an archetype-specific search context string that tells the AI
    what types of businesses to look for during find()."""
    search_term = product or market

    if archetype_key == "b2b-industrial":
        text = f"{market} {product or ''}".lower()
        agritech_tokens = (
            "hydroponic", "hydroponics", "aquaponic", "aeroponic", "greenhouse",
            "vertical farm", "vertical farming", "indoor farm", "indoor farming",
            "controlled environment", "agritech", "ag-tech", "agtech",
            "agriculture", "agricultural", "horticulture", "irrigation",
            "fertigation", "grower", "growers", "nursery", "cannabis",
        )
        food_tokens = (
            "brisket", "beef", "pork", "chicken", "meat", "bbq", "produce",
            "vegetable", "vegetables", "fruit", "dairy", "seafood", "fish",
            "grain", "flour", "sugar", "coffee bean", "tea leaves",
        )
        is_agritech = any(t in text for t in agritech_tokens)
        is_food = any(t in text for t in food_tokens)

        if is_agritech:
            return (
                f"We sell {search_term} to commercial growers in {geography}.\n"
                f"Find BUYERS first: commercial hydroponic growers, vertical/indoor farms, "
                f"greenhouse operations, controlled-environment agriculture (CEA) facilities, "
                f"cannabis cultivators, ag-research facilities, university ag programs, "
                f"and any horticulture business operating at scale.\n"
                f"Also include: ag-tech distributors, irrigation/greenhouse equipment dealers, "
                f"grower co-ops, and CEA system integrators in {geography}.\n"
                f"Skip: home gardeners, hobby farms, retail garden centers selling to consumers, "
                f"and listicles/'top 10' content."
            )
        if is_food:
            return (
                f"We are researching the B2B supply chain for {search_term} in {geography}.\n"
                f"Find BOTH sides: (1) businesses that BUY/CONSUME {search_term} (restaurants, caterers, "
                f"food service, manufacturers, fabricators) AND (2) businesses that SELL/DISTRIBUTE "
                f"{search_term} (wholesalers, distributors, suppliers, importers, specialty markets).\n"
                f"Also look for: butcher shops, commissary kitchens, food trucks, catering companies, "
                f"industrial buyers, contract manufacturers, and any business that purchases "
                f"{search_term} in bulk."
            )
        # Generic industrial / hardware / B2B equipment
        return (
            f"We sell {search_term} to industrial buyers in {geography}.\n"
            f"Find BUYERS first: manufacturers, factories, processing plants, OEMs, and any "
            f"facility that would deploy {search_term} at scale. Then find COMPETITORS / "
            f"SYSTEM INTEGRATORS / DISTRIBUTORS that resell or integrate {search_term} for those buyers.\n"
            f"Look for: industrial automation integrators, control-system installers, "
            f"trade-association members, RFP issuers, plants posting maintenance/automation roles.\n"
            f"Skip: consumer/retail listings, pure software companies with no hardware, "
            f"directories, and 'top N' listicles."
        )

    if archetype_key == "b2b-saas":
        return (
            f"We are researching companies that could buy {search_term} software in {geography}.\n"
            f"Find: companies currently using competitor products, companies with pain points that "
            f"{search_term} solves, growing companies that need {search_term} tooling, "
            f"companies with job postings mentioning {market}.\n"
            f"Also look for: companies recently funded, companies posting roles related to {market}, "
            f"and businesses that have outgrown manual processes in this space."
        )

    if archetype_key == "b2c-saas":
        return (
            f"We are researching consumer apps and products in the {search_term} space in {geography}.\n"
            f"Find: companies building consumer apps in {market}, indie developers and small studios, "
            f"startups with apps on the App Store or Google Play, and companies with active user communities.\n"
            f"Also look for: Product Hunt launches, social media presences, and freemium products in this category."
        )

    if archetype_key == "local-service":
        return (
            f"We are researching {search_term} businesses in {geography}.\n"
            f"Find: all {search_term} businesses including small/independent ones, chain locations, "
            f"new openings, food trucks, pop-ups, and catering operations in the metro area.\n"
            f"Also look for: businesses in nearby neighborhoods, recently opened locations, "
            f"businesses listed on Yelp/Google Maps, and mobile or home-based operations."
        )

    if archetype_key == "consumer-cpg":
        return (
            f"We are researching consumer packaged goods brands in the {search_term} category in {geography}.\n"
            f"Find: CPG brands producing {search_term}, DTC brands, brands carried in local retailers, "
            f"and emerging brands with e-commerce presence.\n"
            f"Also look for: co-manufacturers, private-label producers, brands on Amazon or Shopify, "
            f"and companies exhibiting at trade shows related to {market}."
        )

    if archetype_key == "marketplace":
        return (
            f"We are researching marketplace platforms in the {search_term} space in {geography}.\n"
            f"Find: platforms connecting buyers and sellers in {market}, existing marketplaces (even small ones), "
            f"directory sites that could become marketplaces, and companies aggregating supply or demand.\n"
            f"Also look for: gig platforms, booking platforms, listing sites, and peer-to-peer exchanges in this space."
        )

    if archetype_key == "healthcare":
        return (
            f"We are researching healthcare businesses related to {search_term} in {geography}.\n"
            f"Find: clinics, practices, and providers offering {search_term}, digital health companies, "
            f"medical device companies, and health systems with relevant departments.\n"
            f"Also look for: telehealth providers, specialty practices, ambulatory surgery centers, "
            f"diagnostic labs, and healthcare IT companies serving this segment."
        )

    if archetype_key == "services-agency":
        return (
            f"We are researching {search_term} service providers and agencies in {geography}.\n"
            f"Find: agencies, consulting firms, and freelancers specializing in {search_term}, "
            f"boutique firms, large agencies with {market} practices, and independent consultants.\n"
            f"Also look for: firms listed on Clutch or similar directories, companies with case studies "
            f"in {market}, and professionals with strong LinkedIn presence in this space."
        )

    return (
        f"We are researching businesses related to {search_term} in {geography}.\n"
        f"Find: all types of businesses involved in {market}, including providers, suppliers, "
        f"buyers, and intermediaries."
    )


def archetype_qualify_context(
    archetype_key: str, market: str, product: str | None
) -> str:
    """Return an archetype-specific qualification context string that tells the AI
    how to evaluate companies as leads during qualify()."""
    search_term = product or market

    if archetype_key == "b2b-industrial":
        text = f"{market} {product or ''}".lower()
        agritech_tokens = (
            "hydroponic", "hydroponics", "aquaponic", "aeroponic", "greenhouse",
            "vertical farm", "vertical farming", "indoor farm", "indoor farming",
            "controlled environment", "agritech", "ag-tech", "agtech",
            "agriculture", "agricultural", "horticulture", "irrigation",
            "fertigation", "grower", "growers", "nursery", "cannabis",
        )
        food_tokens = (
            "brisket", "beef", "pork", "chicken", "meat", "bbq", "produce",
            "vegetable", "vegetables", "fruit", "dairy", "seafood", "fish",
            "grain", "flour", "sugar", "coffee bean", "tea leaves",
        )
        is_agritech = any(t in text for t in agritech_tokens)
        is_food = any(t in text for t in food_tokens)

        if is_agritech:
            return (
                f"We sell {search_term} to commercial growers. Evaluate each company as a "
                f"POTENTIAL BUYER.\n"
                f"A qualified lead is a commercial hydroponic / vertical / greenhouse / indoor "
                f"farming operation that:\n"
                f"- Operates at commercial scale (multi-thousand sq ft, multiple zones, "
                f"  full-time staff — not a hobby farm)\n"
                f"- Already invested in growing infrastructure (lights, climate control, "
                f"  fertigation) and would extend it with {search_term}\n"
                f"- Shows growth signals (expansion, hiring growers/agronomists/operations, "
                f"  new facility builds, fundraising)\n"
                f"- Has explicit pain around yield, labor cost, energy, water, or consistency "
                f"  that {search_term} addresses\n\n"
                f"Score higher: well-funded CEA / vertical-farm operators, multi-site "
                f"greenhouse companies, cannabis cultivators with compliance pressure, "
                f"institutional / research growers with capex budgets.\n"
                f"Score lower: home gardeners, hobby farms, retail garden centers, dormant "
                f"operations, and competitors selling the same automation systems."
            )
        if is_food:
            return (
                f"We are a {search_term} wholesale distributor / supplier. "
                f"Evaluate each company as a POTENTIAL BUYER of {search_term}.\n"
                f"A qualified lead is a restaurant, caterer, manufacturer, or food service "
                f"business that:\n"
                f"- Uses {search_term} in significant volume (high-volume restaurant > small cafe)\n"
                f"- Has multiple locations or high foot traffic (more volume = better customer)\n"
                f"- Does catering or bulk orders\n"
                f"- Shows growth signals (expanding, hiring, new locations)\n\n"
                f"Score higher: established high-volume buyers, chain locations, large caterers, "
                f"businesses with clear bulk purchasing needs.\n"
                f"Score lower: small cafes with minimal {search_term} usage, businesses unlikely "
                f"to buy wholesale, competitors who are also distributors (mark as 'competitor' "
                f"not 'qualified')."
            )
        # Generic industrial / hardware / B2B equipment
        return (
            f"We sell {search_term} to industrial buyers. Evaluate each company as a "
            f"POTENTIAL BUYER (or, if they sell similar hardware, as a competitor).\n"
            f"A qualified lead is a manufacturer, processing facility, or industrial operator "
            f"that:\n"
            f"- Operates plants/lines where {search_term} would deploy at scale\n"
            f"- Has CapEx authority (plant manager / VP Operations / VP Engineering) and a "
            f"  modernization or efficiency mandate\n"
            f"- Shows growth signals (capacity expansion, hiring controls/automation engineers, "
            f"  recent funding, new facility builds)\n"
            f"- Has explicit pain around uptime, throughput, quality, energy, or labor that "
            f"  {search_term} addresses\n\n"
            f"Score higher: mid/large multi-site operators, recently funded scaleups, plants "
            f"posting controls/automation roles, facilities mentioning modernization.\n"
            f"Score lower: very small shops, consumer/retail businesses, pure software vendors, "
            f"and competitors selling the same hardware (mark 'competitor' not 'qualified')."
        )

    if archetype_key == "b2b-saas":
        return (
            f"We sell {search_term} software. Evaluate each company as a potential buyer.\n"
            f"A qualified lead:\n"
            f"- Has 50+ employees (can afford enterprise software)\n"
            f"- Currently uses competitor products or manual processes for {market}\n"
            f"- Shows growth signals (hiring, funding, expansion)\n"
            f"- Has budget authority (look for VP/Director level contacts)\n\n"
            f"Score higher: mid-market and enterprise companies with clear need, "
            f"companies with job postings in {market}, recently funded startups scaling up.\n"
            f"Score lower: very small teams (<10 people), companies already locked into a competitor, "
            f"companies in unrelated industries."
        )

    if archetype_key == "b2c-saas":
        return (
            f"We are building a consumer app / B2C product in {search_term}. "
            f"Evaluate each company as a POTENTIAL COMPETITOR or PARTNERSHIP target.\n"
            f"A qualified lead:\n"
            f"- Has an active user base in a related category\n"
            f"- Shows strong engagement metrics (app ratings, social following, reviews)\n"
            f"- Could be a distribution partner or acquisition target\n"
            f"- Demonstrates product-market fit in an adjacent space\n\n"
            f"Score higher: companies with strong user engagement, growing download counts, "
            f"active communities.\n"
            f"Score lower: dormant apps, companies with poor ratings, unrelated consumer products."
        )

    if archetype_key == "local-service":
        return (
            f"We are researching the {search_term} market for competitive analysis and "
            f"potential customer/partnership opportunities.\n"
            f"Evaluate each company as a business operating in {search_term}.\n"
            f"A qualified lead:\n"
            f"- Is an active, operating {search_term} business (not permanently closed)\n"
            f"- Has visible foot traffic, reviews, or online presence\n"
            f"- Shows quality signals (good ratings, consistent reviews, active social media)\n"
            f"- Has growth indicators (new locations, catering arm, delivery, expanding hours)\n\n"
            f"Score higher: established businesses with strong reputations, multi-location operators, "
            f"businesses with catering or delivery revenue streams.\n"
            f"Score lower: businesses that appear closed or inactive, very low review counts "
            f"suggesting minimal traffic, businesses not actually in {search_term}."
        )

    if archetype_key == "consumer-cpg":
        return (
            f"We are a {search_term} CPG brand. Evaluate each company as a potential "
            f"retail partner, competitor, or distribution channel.\n"
            f"A qualified lead:\n"
            f"- Is a retailer that could carry {search_term} products (grocery, specialty, online)\n"
            f"- Is a competing brand whose shelf space or positioning we should understand\n"
            f"- Has strong retail velocity or DTC presence\n"
            f"- Shows growth signals (new store openings, expanded product lines)\n\n"
            f"Score higher: retailers with relevant category presence, growing DTC brands, "
            f"distributors with established retail relationships.\n"
            f"Score lower: unrelated retailers, brands in completely different categories, "
            f"businesses with no retail or e-commerce presence."
        )

    if archetype_key == "marketplace":
        return (
            f"We are building a marketplace in {search_term}. Evaluate each company as a "
            f"potential supply-side partner, demand-side participant, or competitor.\n"
            f"A qualified lead:\n"
            f"- Could be a supplier or provider on our platform\n"
            f"- Represents significant demand volume in {market}\n"
            f"- Is currently underserved by existing marketplace options\n"
            f"- Shows signals of needing better buyer-seller matching\n\n"
            f"Score higher: businesses with high transaction volume, those currently using "
            f"inefficient channels, providers with strong reputations but limited reach.\n"
            f"Score lower: businesses too small to generate meaningful GMV, those already "
            f"well-served by existing platforms."
        )

    if archetype_key == "healthcare":
        return (
            f"We are in the {search_term} healthcare space. Evaluate each company as a "
            f"potential customer, partner, or key account.\n"
            f"A qualified lead:\n"
            f"- Is a healthcare provider, health system, or practice relevant to {market}\n"
            f"- Has sufficient patient volume or revenue to justify the purchase\n"
            f"- Shows modernization signals (adopting new technology, expanding services)\n"
            f"- Has regulatory compliance infrastructure (HIPAA, EMR integration)\n\n"
            f"Score higher: multi-location practices, health systems, providers with "
            f"technology-forward reputations, practices in growth mode.\n"
            f"Score lower: very small solo practices with limited budgets, providers in "
            f"unrelated specialties, businesses with no clear connection to {search_term}."
        )

    if archetype_key == "services-agency":
        return (
            f"We are researching the {search_term} services market. Evaluate each company "
            f"as a competitor, potential partner, or acquisition target.\n"
            f"A qualified lead:\n"
            f"- Is an active agency or consultancy in {market}\n"
            f"- Has a clear specialization and client portfolio\n"
            f"- Shows revenue signals (team size, office presence, client logos)\n"
            f"- Demonstrates thought leadership or industry recognition\n\n"
            f"Score higher: firms with strong case studies, retainer-based revenue, "
            f"growing teams, and industry awards or recognition.\n"
            f"Score lower: solo freelancers with no web presence, inactive firms, "
            f"generalist agencies with no depth in {search_term}."
        )

    return (
        f"Evaluate these companies as potential sales targets or competitors "
        f"in the {search_term} market.\n"
        f"A qualified lead is a business that is actively operating in or adjacent to {market}, "
        f"has visible revenue or activity signals, and could be a customer, partner, or competitor."
    )
