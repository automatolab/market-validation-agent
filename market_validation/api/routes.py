"""HTTP route handlers for the dashboard API.

Ported from the stdlib ``BaseHTTPRequestHandler`` in ``dashboard._make_handler``.
Same endpoints, same JSON shapes — so the existing frontend JS keeps working
without changes.

Pydantic request models (see ``schemas``) replace the old manual
``data["field"]`` lookups; invalid payloads now return 422 with field-level
errors instead of a 500 with a KeyError stack trace.
"""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response

from market_validation.api.schemas import (
    CompanyAddRequest,
    CompanyDeleteRequest,
    CompanyUpdateRequest,
    EmailDraftAllRequest,
    EmailDraftRequest,
    EmailIdRequest,
    EmailQueueRequest,
    EmailUpdateRequest,
)
from market_validation.dashboard import _load_data


def register_routes(app: FastAPI) -> None:
    """Attach all dashboard routes to *app*."""

    # ── Root: rendered dashboard HTML ─────────────────────────────────────

    @app.get("/", response_class=HTMLResponse)
    async def dashboard_home(request: Request) -> HTMLResponse:
        """Render the dashboard with fresh data from the DB."""
        return _render_dashboard_page(request)

    @app.get("/health")
    async def health() -> dict[str, str]:
        """Liveness probe — used by orchestrators and smoke tests."""
        return {"status": "ok"}

    # ── Data endpoints ────────────────────────────────────────────────────

    @app.get("/api/data")
    async def api_data() -> dict[str, Any]:
        """Return the same payload that's embedded in the HTML, without re-rendering."""
        return {"result": "ok", "data": _load_data()}

    @app.get("/api/refresh")
    async def api_refresh(request: Request) -> dict[str, str]:
        """Re-render the dashboard (for the stdlib-compatible refresh button)."""
        _render_dashboard_page(request)
        return {"result": "ok"}

    @app.get("/api/validation/{research_id}")
    async def api_validation(research_id: str) -> dict[str, Any]:
        from market_validation.research import get_validation_by_research
        return get_validation_by_research(research_id)

    # ── Email open-tracking pixel (GET) ───────────────────────────────────

    @app.get("/api/email/track/open/{email_id}")
    async def email_track_open(email_id: str) -> Response:
        from market_validation.email_tracker import TRANSPARENT_GIF, record_open
        record_open(email_id)
        return Response(
            content=TRANSPARENT_GIF,
            media_type="image/gif",
            headers={"Cache-Control": "no-store, no-cache, must-revalidate"},
        )

    # ── Company mutations ─────────────────────────────────────────────────

    @app.post("/api/company/add")
    async def company_add(payload: CompanyAddRequest) -> dict[str, Any]:
        from market_validation.research import add_company
        return _safe(lambda: add_company(
            research_id=payload.research_id,
            company_name=payload.company_name,
            market=payload.market or "general",
            website=payload.website,
            location=payload.location,
            phone=payload.phone,
            email=payload.email,
            notes=payload.notes,
        ))

    @app.post("/api/company/update")
    async def company_update(payload: CompanyUpdateRequest) -> dict[str, Any]:
        from market_validation.research import update_company
        return _safe(lambda: update_company(
            company_id=payload.company_id,
            research_id=payload.research_id,
            fields=payload.fields,
        ))

    @app.post("/api/company/delete")
    async def company_delete(payload: CompanyDeleteRequest) -> dict[str, Any]:
        from market_validation.research import delete_company
        return _safe(lambda: delete_company(
            company_id=payload.company_id,
            research_id=payload.research_id,
        ))

    # ── Email mutations ───────────────────────────────────────────────────

    @app.post("/api/email/approve")
    async def email_approve(payload: EmailIdRequest) -> dict[str, Any]:
        from market_validation.email_sender import approve_email
        return _safe(lambda: approve_email(payload.email_id))

    @app.post("/api/email/update")
    async def email_update(payload: EmailUpdateRequest) -> dict[str, Any]:
        from market_validation.email_sender import update_queued_email
        return _safe(lambda: update_queued_email(
            email_id=payload.email_id,
            subject=payload.subject,
            body=payload.body,
        ))

    @app.post("/api/email/delete")
    async def email_delete(payload: EmailIdRequest) -> dict[str, Any]:
        from market_validation.email_sender import delete_email
        return _safe(lambda: delete_email(payload.email_id))

    @app.post("/api/email/sync")
    async def email_sync() -> dict[str, Any]:
        from market_validation.gmail_tracker import sync_all as gmail_sync_all
        return _safe(gmail_sync_all)

    @app.post("/api/email/draft")
    async def email_draft(payload: EmailDraftRequest) -> dict[str, Any]:
        from market_validation.email_sender import draft_email_for_company
        return _safe(lambda: draft_email_for_company(payload.company_id))

    @app.post("/api/email/queue")
    async def email_queue(payload: EmailQueueRequest) -> dict[str, Any]:
        from market_validation.email_sender import prep_email
        return _safe(lambda: prep_email(
            to_email=payload.to_email,
            subject=payload.subject,
            body=payload.body,
            company_name=payload.company_name,
            contact_name=payload.contact_name,
            research_id=payload.research_id,
            company_id=payload.company_id,
        ))

    @app.post("/api/email/draft-all")
    async def email_draft_all(payload: EmailDraftAllRequest) -> dict[str, Any]:
        from market_validation.email_sender import draft_emails_for_research
        return _safe(lambda: draft_emails_for_research(
            research_id=payload.research_id,
            statuses=payload.statuses,
            skip_existing=payload.skip_existing,
        ))

    @app.post("/api/email/approve-all")
    async def email_approve_all() -> dict[str, Any]:
        from market_validation.email_sender import approve_all_emails
        return _safe(approve_all_emails)

    @app.post("/api/email/reject-all")
    async def email_reject_all() -> dict[str, Any]:
        from market_validation.email_sender import reject_all_emails
        return _safe(reject_all_emails)


def _render_dashboard_page(request: Request) -> HTMLResponse:
    """Shared dashboard-render path — reused by `/` and `/api/refresh`."""
    import json

    from market_validation.dashboard import _iso_now

    data = _load_data()
    researches = data["researches"]
    companies = data["companies"]
    emails = data["emails"]

    pending_count = sum(1 for e in emails if e.get("status") == "pending")
    sent_count = sum(1 for e in emails if e.get("status") in ("sent", "opened", "replied", "bounced"))
    replied_count = sum(1 for e in emails if e.get("replied_at"))
    qualified_count = sum(1 for c in companies if c.get("status") == "qualified")
    phone_count = sum(1 for c in companies if c.get("phone"))
    email_count = sum(1 for c in companies if c.get("email"))

    # Same escape as the stdlib path: neutralize "</" so nothing terminates
    # the <script type="application/json"> tag early.
    payload_json = json.dumps(data, ensure_ascii=True).replace("</", "<\\/")

    templates = request.app.state.templates
    return templates.TemplateResponse(
        request=request,
        name="dashboard.html",
        context={
            "interactive": True,
            "mode": "server",
            "generated_at": _iso_now(),
            "researches": researches,
            "research_count": len(researches),
            "company_count": len(companies),
            "qualified_count": qualified_count,
            "phone_count": phone_count,
            "email_count": email_count,
            "pending_count": pending_count,
            "sent_count": sent_count,
            "replied_count": replied_count,
            "payload_json": payload_json,
        },
        headers={"Cache-Control": "no-cache, no-store, must-revalidate"},
    )


def _safe(fn: Any) -> dict[str, Any]:
    """Run a handler function and wrap any exception as a 400 JSON error.

    The legacy stdlib handler returned ``{"result": "error", "error": str(exc)}``
    at 400 on any exception; keep that contract so the frontend doesn't change.
    """
    try:
        return fn()
    except HTTPException:
        raise
    except Exception as exc:
        # 400 matches the old handler's behavior; swap to 500 later if we
        # start distinguishing client vs server faults.
        return JSONResponse(
            status_code=400,
            content={"result": "error", "error": str(exc)},
        )
