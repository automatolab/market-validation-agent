"""Pydantic models for the market-validation pipeline.

These are the typed shapes that flow across service boundaries. They mirror
the SQLite schema in ``research.py`` (see ``_ensure_schema``) and intentionally
stay close to it — rows can be adapted to models via ``from_row`` and back
to dicts via ``to_db_dict``.

Status values and priority tiers are intentionally constrained with Literal
so that typos fail at the boundary rather than silently corrupting data.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

CompanyStatus = Literal["new", "qualified", "uncertain", "not_interested", "contacted", "interested"]
PriorityTier = Literal["high", "medium", "low"]
ResearchStatus = Literal["active", "archived", "completed"]


class _StrictModel(BaseModel):
    """Base: allow extras for forward-compat, validate on assign."""

    model_config = ConfigDict(extra="ignore", validate_assignment=True, str_strip_whitespace=True)


class Research(_StrictModel):
    """A research project — one market × geography investigation."""

    id: str
    name: str
    market: str
    product: str | None = None
    geography: str | None = None
    description: str | None = None
    status: ResearchStatus = "active"
    created_at: str
    updated_at: str
    last_source_health: str | None = None

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> Research:
        return cls.model_validate(dict(row))


class ResearchCreate(_StrictModel):
    """Input shape for creating a new research project."""

    name: str
    market: str
    product: str | None = None
    geography: str | None = None
    description: str | None = None


class Contact(_StrictModel):
    """A person at a company — name/title/contact details."""

    name: str | None = None
    title: str | None = None
    email: str | None = None
    phone: str | None = None


class Company(_StrictModel):
    """A company record as stored in the ``companies`` table."""

    id: str
    research_id: str
    market: str
    company_name: str
    company_name_normalized: str | None = None
    website: str | None = None
    location: str | None = None
    phone: str | None = None
    email: str | None = None
    status: str = "new"
    priority_score: int | None = Field(default=None, ge=0, le=100)
    priority_tier: PriorityTier | None = None
    next_action: str | None = None
    why_now: str | None = None
    volume_estimate: float | None = None
    volume_unit: str | None = None
    volume_basis: str | None = None
    volume_tier: str | None = None
    notes: str | None = None
    menu_items: str | None = None
    prices: str | None = None
    hours: str | None = None
    ratings: str | None = None
    reviews_count: int | None = None
    raw_data: str | None = None
    created_at: str
    updated_at: str

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> Company:
        return cls.model_validate(dict(row))


class CompanyCandidate(_StrictModel):
    """A search-discovered company *before* it's written to the DB.

    ``find()`` and ``search`` backends produce these; the pipeline dedupes,
    filters, and AI-validates them before they become ``Company`` rows.
    """

    company_name: str
    website: str | None = None
    location: str | None = None
    phone: str | None = None
    email: str | None = None
    description: str | None = None
    evidence_url: str | None = None
    source: str = "unknown"

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump(exclude_none=False)


class QualificationResult(_StrictModel):
    """One company's AI qualification verdict."""

    company_id: str
    status: Literal["qualified", "uncertain", "not_interested", "new"] = "new"
    score: int = Field(ge=0, le=100)
    priority: PriorityTier = "low"
    volume_estimate: float | None = None
    volume_unit: str | None = None
    notes: str | None = None


class EnrichmentFindings(_StrictModel):
    """What a single enrich() pass discovered for one company."""

    emails: list[str] = Field(default_factory=list)
    phones: list[str] = Field(default_factory=list)
    contacts: list[Contact] = Field(default_factory=list)
    address: str | None = None
    website: str | None = None
    sources: list[str] = Field(default_factory=list)
    email_sources: dict[str, str] = Field(default_factory=dict)
