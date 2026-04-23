"""Request/response Pydantic models for the FastAPI endpoints.

Defining these at the API boundary gives us three things at once:
  1. Automatic 422 responses with field-level errors for bad input.
  2. OpenAPI schema documentation at /docs and /redoc.
  3. A clear contract the frontend can rely on.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class _ApiModel(BaseModel):
    """Base model — ignore extra keys so the frontend can send forward-compat payloads."""

    model_config = ConfigDict(extra="ignore", str_strip_whitespace=True)


# ── Company endpoints ────────────────────────────────────────────────────────

class CompanyAddRequest(_ApiModel):
    research_id: str = Field(min_length=1)
    company_name: str = Field(min_length=1)
    market: str | None = "general"
    website: str | None = None
    location: str | None = None
    phone: str | None = None
    email: str | None = None
    notes: str | None = None


class CompanyUpdateRequest(_ApiModel):
    company_id: str = Field(min_length=1)
    research_id: str = Field(min_length=1)
    fields: dict = Field(default_factory=dict)


class CompanyDeleteRequest(_ApiModel):
    company_id: str = Field(min_length=1)
    research_id: str = Field(min_length=1)


# ── Email endpoints ──────────────────────────────────────────────────────────

class EmailIdRequest(_ApiModel):
    email_id: str = Field(min_length=1)


class EmailUpdateRequest(_ApiModel):
    email_id: str = Field(min_length=1)
    subject: str | None = None
    body: str | None = None


class EmailDraftRequest(_ApiModel):
    company_id: str = Field(min_length=1)


class EmailQueueRequest(_ApiModel):
    to_email: str = Field(min_length=1)
    subject: str
    body: str
    company_name: str | None = None
    contact_name: str | None = None
    research_id: str | None = None
    company_id: str | None = None


class EmailDraftAllRequest(_ApiModel):
    research_id: str = Field(min_length=1)
    statuses: list[str] = Field(default_factory=lambda: ["qualified"])
    skip_existing: bool = True


# ── Generic response wrappers ────────────────────────────────────────────────

class ApiResult(BaseModel):
    """Matches the legacy `{"result": "ok"|"error", ...}` shape the frontend expects.

    Kept deliberately loose (extra="allow") so existing handler return values
    pass through unchanged — we wrap them in this type only when we generate
    responses directly.
    """

    model_config = ConfigDict(extra="allow")

    result: str
