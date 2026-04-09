from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

CANONICAL_STATUSES = (
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
)

TRACKER_HEADER = "| # | Date | Market | Target Customer | Status | Score | Report | Notes |"
TRACKER_SEPARATOR = "|---|------|--------|-----------------|--------|-------|--------|-------|"
TRACKER_TITLE = "# Market Validation Tracker"


@dataclass
class MergeResult:
    added: int
    updated: int
    skipped: int
    processed_files: int


@dataclass
class VerificationResult:
    errors: list[str]
    warnings: list[str]


@dataclass
class TrackerRow:
    num: int
    date: str
    market: str
    target_customer: str
    status: str
    score: str
    report: str
    notes: str

    def key(self) -> tuple[str, str]:
        return (normalize_text(self.market), normalize_text(self.target_customer))


def normalize_text(value: str) -> str:
    cleaned = re.sub(r"\s+", " ", value.strip().lower())
    return re.sub(r"[^a-z0-9 ]", "", cleaned)


def parse_score(score: str) -> float:
    if not score:
        return -1.0
    match = re.search(r"([0-9]+(?:\.[0-9]+)?)", score)
    if not match:
        return -1.0
    try:
        return float(match.group(1))
    except ValueError:
        return -1.0


def normalize_status(status: str) -> str:
    normalized = status.strip().lower().replace(" ", "_")
    aliases = {
        "test-ready": "test_ready",
        "test ready": "test_ready",
        "in_progress": "scanning",
        "in progress": "scanning",
    }
    normalized = aliases.get(normalized, normalized)
    if normalized not in CANONICAL_STATUSES:
        return "new"
    return normalized


def extract_report_target(report_value: str) -> str:
    report_target = report_value.strip()
    if (
        report_target.startswith("[")
        and "](" in report_target
        and report_target.endswith(")")
    ):
        return report_target.split("](", 1)[1][:-1]
    return report_target


def _move_to_merged(file_path: Path, merged_dir: Path) -> None:
    destination = merged_dir / file_path.name
    if destination.exists():
        stem = destination.stem
        suffix = destination.suffix
        counter = 1
        while destination.exists():
            destination = merged_dir / f"{stem}-{counter}{suffix}"
            counter += 1
    file_path.rename(destination)


def ensure_tracker_file(root: str | Path) -> Path:
    root_path = Path(root)
    tracker_path = root_path / "data" / "validation-tracker.md"
    tracker_path.parent.mkdir(parents=True, exist_ok=True)
    if not tracker_path.exists():
        tracker_path.write_text(
            TRACKER_TITLE + "\n\n"
            + TRACKER_HEADER
            + "\n"
            + TRACKER_SEPARATOR
            + "\n",
            encoding="utf-8",
        )
    return tracker_path


def read_tracker_rows(tracker_path: Path) -> list[TrackerRow]:
    rows: list[TrackerRow] = []
    for raw_line in tracker_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line.startswith("|"):
            continue
        if line == TRACKER_HEADER or line == TRACKER_SEPARATOR:
            continue
        parts = [p.strip() for p in line.strip("|").split("|")]
        if len(parts) < 8:
            continue
        try:
            number = int(parts[0])
        except ValueError:
            continue
        rows.append(
            TrackerRow(
                num=number,
                date=parts[1],
                market=parts[2],
                target_customer=parts[3],
                status=normalize_status(parts[4]),
                score=parts[5],
                report=parts[6],
                notes=parts[7],
            )
        )
    return rows


def write_tracker_rows(tracker_path: Path, rows: Iterable[TrackerRow]) -> None:
    ordered = sorted(rows, key=lambda r: r.num)
    lines = [TRACKER_TITLE, "", TRACKER_HEADER, TRACKER_SEPARATOR]
    for row in ordered:
        lines.append(
            f"| {row.num} | {row.date} | {row.market} | {row.target_customer} | {row.status} | {row.score} | {row.report} | {row.notes} |"
        )
    tracker_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_addition_file(path: Path) -> TrackerRow | None:
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return None

    columns = next(csv.reader([text], delimiter="\t"), [])
    if len(columns) < 8:
        return None

    try:
        number = int(columns[0])
    except ValueError:
        return None

    return TrackerRow(
        num=number,
        date=columns[1].strip(),
        market=columns[2].strip(),
        target_customer=columns[3].strip(),
        status=normalize_status(columns[4]),
        score=columns[5].strip(),
        report=columns[6].strip(),
        notes=columns[7].strip(),
    )


def merge_tracker_additions(root: str | Path) -> MergeResult:
    root_path = Path(root)
    tracker_path = ensure_tracker_file(root_path)
    additions_dir = root_path / "batch" / "tracker-additions"
    merged_dir = additions_dir / "merged"

    additions_dir.mkdir(parents=True, exist_ok=True)
    merged_dir.mkdir(parents=True, exist_ok=True)

    existing_rows = read_tracker_rows(tracker_path)
    by_key = {row.key(): row for row in existing_rows}
    by_num = {row.num: row for row in existing_rows}
    next_num = max((row.num for row in existing_rows), default=0) + 1

    added = 0
    updated = 0
    skipped = 0

    pending_files = sorted(p for p in additions_dir.glob("*.tsv") if p.is_file())

    for file_path in pending_files:
        addition = parse_addition_file(file_path)
        if addition is None:
            skipped += 1
            _move_to_merged(file_path, merged_dir)
            continue

        target = by_key.get(addition.key())
        if target is None:
            if addition.num in by_num:
                addition.num = next_num
                next_num += 1
            by_key[addition.key()] = addition
            by_num[addition.num] = addition
            added += 1
        else:
            old_score = parse_score(target.score)
            new_score = parse_score(addition.score)
            should_update = new_score > old_score or normalize_status(target.status) == "new"
            if should_update:
                target.date = addition.date
                target.status = addition.status
                target.score = addition.score
                target.report = addition.report
                if addition.notes:
                    target.notes = addition.notes
                updated += 1
            else:
                skipped += 1

        _move_to_merged(file_path, merged_dir)

    write_tracker_rows(tracker_path, by_num.values())

    return MergeResult(
        added=added,
        updated=updated,
        skipped=skipped,
        processed_files=len(pending_files),
    )


def verify_pipeline(root: str | Path) -> VerificationResult:
    root_path = Path(root)
    tracker_path = ensure_tracker_file(root_path)
    rows = read_tracker_rows(tracker_path)

    errors: list[str] = []
    warnings: list[str] = []

    seen_keys: dict[tuple[str, str], int] = {}
    for row in rows:
        if row.status not in CANONICAL_STATUSES:
            errors.append(f"#{row.num} has invalid status: {row.status}")

        key = row.key()
        if key in seen_keys:
            warnings.append(
                f"Possible duplicate market/customer pair in rows #{seen_keys[key]} and #{row.num}"
            )
        else:
            seen_keys[key] = row.num

        if row.report:
            report_target = extract_report_target(row.report)
            report_path = root_path / report_target
            if not report_path.exists():
                errors.append(f"#{row.num} report link missing: {report_target}")

    pending = sorted((root_path / "batch" / "tracker-additions").glob("*.tsv"))
    if pending:
        warnings.append(f"{len(pending)} pending tracker additions are not merged")

    return VerificationResult(errors=errors, warnings=warnings)


def main_merge_cli() -> None:
    result = merge_tracker_additions(Path.cwd())
    print(
        f"Merged tracker additions: processed={result.processed_files} added={result.added} updated={result.updated} skipped={result.skipped}"
    )


def main_verify_cli() -> None:
    result = verify_pipeline(Path.cwd())
    for message in result.errors:
        print(f"ERROR: {message}")
    for message in result.warnings:
        print(f"WARN: {message}")
    if result.errors:
        raise SystemExit(1)
