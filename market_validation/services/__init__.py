"""Service classes that compose the market-validation pipeline.

Each service encapsulates one pipeline phase:
  - ``ValidationService`` — market opportunity assessment (validate)
  - ``SearchService``     — company discovery (find)
  - ``QualificationService`` — AI scoring of candidates (qualify)
  - ``EnrichmentService`` — contact-info enrichment (enrich / enrich_all)

Services receive dependencies (AI runner, DB root, research_id) at
construction so they're individually testable. The ``Agent`` facade in
``market_validation.agent`` wires them together.
"""

from market_validation.services.enrichment import EnrichmentService
from market_validation.services.qualification import QualificationService
from market_validation.services.search import SearchService
from market_validation.services.validation import ValidationService

__all__ = [
    "ValidationService",
    "SearchService",
    "QualificationService",
    "EnrichmentService",
]
