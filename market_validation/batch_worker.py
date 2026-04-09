from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

CANONICAL_STATUSES = {
    "new",
    "scanning",
    "validated",
    "interviewing",
    "test_ready",
    "monitor",
    "rejected",
    "archived",
    "qualified",
    "emailed",
    "replied_interested",
    "replied_not_now",
    "do_not_contact",
    "call_ready",
}


def slugify(value: str) -> str:
    lowered = value.strip().lower()
    chars = []
    for ch in lowered:
        if ch.isalnum():
            chars.append(ch)
        elif ch in {" ", "-", "_"}:
            chars.append("-")
    slug = "".join(chars)
    while "--" in slug:
        slug = slug.replace("--", "-")
    return slug.strip("-") or "market"


def format_score(score: float | None) -> str:
    return "N/A" if score is None else f"{score:.2f}/10"


def _ensure_output_dirs(root: Path) -> tuple[Path, Path]:
    reports_dir = root / "reports"
    additions_dir = root / "batch" / "tracker-additions"
    reports_dir.mkdir(parents=True, exist_ok=True)
    additions_dir.mkdir(parents=True, exist_ok=True)
    return reports_dir, additions_dir


def _read_optional(path: Path) -> str:
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8").strip()


def build_opencode_prompt(args: argparse.Namespace, root: Path) -> str:
    shared = _read_optional(root / "modes" / "_shared.md")
    validate_mode = _read_optional(root / "modes" / "validate.md")
    batch_contract = _read_optional(root / "batch" / "batch-prompt.md")

    header = (
        "You are a market-validation worker. Execute one item and return ONLY JSON.\n"
        "Do not add prose outside JSON.\n"
    )

    contract = (
        "Return strict JSON with keys:\n"
        "{\n"
        '  "target_customer": "string",\n'
        '  "status": "new|scanning|validated|interviewing|test_ready|monitor|rejected|archived|qualified|emailed|replied_interested|replied_not_now|do_not_contact|call_ready",\n'
        '  "score": number|null,\n'
        '  "verdict": "string",\n'
        '  "notes": "string",\n'
        '  "report_markdown": "full markdown report"\n'
        "}\n"
    )

    response_rules = (
        "Response rules:\n"
        "- Return exactly one JSON object and nothing else.\n"
        "- No markdown fences, no preface text, no trailing commentary.\n"
        "- `status` must be one canonical value.\n"
        "- Use `score: null` when evidence confidence is low.\n"
        "- Keep `notes` concise, evidence-grounded, and decision-useful.\n"
    )

    decision_policy = (
        "Decision policy:\n"
        "- Use `scanning` when signals are mixed or evidence is thin.\n"
        "- Use `validated` only when demand + willingness-to-pay have credible support.\n"
        "- Use `rejected` when evidence indicates weak demand, strong structural blockers, or no clear wedge.\n"
        "- Do not overstate certainty; call out unknowns explicitly.\n"
    )

    task = (
        f"Item:\n"
        f"- id: {args.id}\n"
        f"- market: {args.market}\n"
        f"- geography: {args.geography}\n"
        f"- profile: {args.profile}\n"
        f"- template: {args.template or ''}\n"
        f"- date: {args.date}\n"
        f"- report_num: {args.report_num}\n"
    )

    report_shape = (
        "`report_markdown` should include:\n"
        "- # Title\n"
        "- Date / Market / Target Customer / Geography block\n"
        "- ## Market Summary\n"
        "- ## Source Coverage (quality + breadth)\n"
        "- ## Competitor / Pricing / Demand observations\n"
        "- ## Risks\n"
        "- ## Unknowns\n"
        "- ## Next Validation Experiments\n"
        "- Prefer specific evidence statements over generic advice.\n"
    )

    sections = [
        header,
        shared,
        validate_mode,
        batch_contract,
        contract,
        response_rules,
        decision_policy,
        task,
        report_shape,
    ]
    return "\n\n".join(section for section in sections if section)


def _extract_first_json_object(text: str) -> dict[str, Any]:
    stripped = text.strip()
    if not stripped:
        raise ValueError("Empty model output")

    try:
        parsed = json.loads(stripped)
        if isinstance(parsed, dict):
            return parsed
    except json.JSONDecodeError:
        pass

    fenced_match = re.search(r"```json\s*(\{[\s\S]*?\})\s*```", text, flags=re.IGNORECASE)
    if fenced_match:
        candidate = fenced_match.group(1)
        parsed = json.loads(candidate)
        if isinstance(parsed, dict):
            return parsed

    decoder = json.JSONDecoder()
    for idx, char in enumerate(text):
        if char != "{":
            continue
        try:
            obj, _ = decoder.raw_decode(text[idx:])
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict):
            return obj

    raise ValueError("No JSON object found in model output")


def invoke_opencode(prompt: str, root: Path, args: argparse.Namespace) -> dict[str, Any]:
    command = [
        "opencode",
        "run",
        "--dangerously-skip-permissions",
        "--dir",
        str(root),
    ]

    model = args.model or os.getenv("OPENCODE_MODEL")
    if model:
        command.extend(["--model", model])

    agent = args.agent or os.getenv("OPENCODE_AGENT")
    if agent:
        command.extend(["--agent", agent])

    command.append(prompt)

    completed = subprocess.run(
        command,
        cwd=str(root),
        capture_output=True,
        text=True,
        check=False,
    )

    if completed.returncode != 0:
        err = completed.stderr.strip() or completed.stdout.strip() or f"opencode exited {completed.returncode}"
        raise RuntimeError(err)

    return _extract_first_json_object(completed.stdout)


def normalize_status(value: str | None) -> str:
    if not value:
        return "scanning"
    normalized = value.strip().lower().replace(" ", "_")
    if normalized in CANONICAL_STATUSES:
        return normalized
    aliases = {
        "insufficient_evidence": "scanning",
        "not_ready": "monitor",
        "invalid": "rejected",
    }
    return aliases.get(normalized, "scanning")


def to_score(value: Any) -> float | None:
    if value is None:
        return None
    try:
        score = float(value)
    except (TypeError, ValueError):
        return None
    if score < 0:
        return None
    return min(10.0, score)


def fallback_report_markdown(
    *,
    market: str,
    target_customer: str,
    geography: str,
    date_str: str,
    report_num: str,
    verdict: str,
) -> str:
    return (
        f"# Market Validation: {market}\n\n"
        f"**Date:** {date_str}\n"
        f"**Report:** {report_num}\n"
        f"**Market:** {market}\n"
        f"**Target Customer:** {target_customer}\n"
        f"**Geography:** {geography}\n"
        f"**Verdict:** {verdict}\n\n"
        "---\n\n"
        "## Market Summary\n"
        "Insufficient structured report markdown was returned by the worker.\n"
    )


def run_worker(args: argparse.Namespace) -> dict[str, object]:
    root = Path(args.root).resolve()
    reports_dir, additions_dir = _ensure_output_dirs(root)

    prompt = build_opencode_prompt(args, root)
    model_payload = invoke_opencode(prompt=prompt, root=root, args=args)

    date_str = args.date
    report_num = args.report_num
    market_slug = slugify(args.market)

    target_customer = str(model_payload.get("target_customer") or "Unknown target customer").strip()
    status = normalize_status(str(model_payload.get("status") or "scanning"))
    score = to_score(model_payload.get("score"))
    verdict = str(model_payload.get("verdict") or status)
    notes = str(model_payload.get("notes") or verdict).strip()
    report_markdown = str(model_payload.get("report_markdown") or "").strip()

    report_path = reports_dir / f"{report_num}-{market_slug}-{date_str}.md"
    if not report_markdown:
        report_markdown = fallback_report_markdown(
            market=args.market,
            target_customer=target_customer,
            geography=args.geography,
            date_str=date_str,
            report_num=report_num,
            verdict=verdict,
        )
    report_path.write_text(report_markdown + ("\n" if not report_markdown.endswith("\n") else ""), encoding="utf-8")

    report_rel = report_path.relative_to(root).as_posix()
    tracker_line = "\t".join(
        [
            str(int(report_num)),
            date_str,
            args.market,
            target_customer,
            status,
            format_score(score),
            f"[{report_num}]({report_rel})",
            notes,
        ]
    )
    (additions_dir / f"{args.id}.tsv").write_text(tracker_line + "\n", encoding="utf-8")

    return {
        "status": "completed",
        "id": str(args.id),
        "report_num": report_num,
        "market": args.market,
        "target_customer": target_customer,
        "score": score,
        "report": report_rel,
        "error": None,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Prompt-driven market validation worker via OpenCode")
    parser.add_argument("--id", required=True)
    parser.add_argument("--market", required=True)
    parser.add_argument("--geography", default="global")
    parser.add_argument("--profile", default="general")
    parser.add_argument("--template", default=None)
    parser.add_argument("--report-num", required=True)
    parser.add_argument("--date", default=datetime.now(timezone.utc).strftime("%Y-%m-%d"))
    parser.add_argument("--root", default=".")
    parser.add_argument("--model", default=None)
    parser.add_argument("--agent", default=None)
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    exit_code = 0
    try:
        result = run_worker(args)
    except Exception as exc:  # pragma: no cover - CLI fallback
        exit_code = 1
        result = {
            "status": "failed",
            "id": str(args.id),
            "report_num": args.report_num,
            "market": args.market,
            "target_customer": None,
            "score": None,
            "report": None,
            "error": str(exc),
        }
    print(json.dumps(result, ensure_ascii=True))
    if exit_code != 0:
        raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
