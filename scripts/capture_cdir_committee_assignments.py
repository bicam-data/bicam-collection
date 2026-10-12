"""Parse GovInfo Congressional Directory committee rosters into a reviewable dbt seed.

Reads the HTML rendition of CDIR COMMITTEEASSIGNMENTS granules (the API summary
has no body) and writes the seed behind ``bicam.committees_members``. Unresolved
names stay in the seed; dbt only publishes edges whose member and committee both
resolve, so a shared surname never creates a wrong link.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import date
import hashlib
import html
import os
from pathlib import Path
import re
import unicodedata

import psycopg
import requests


ASSIGNMENT_CLASSES = frozenset({"COMMITTEEASSIGNMENTS"})
SEED_COLUMNS = [
    "source_package_id",
    "source_granule_id",
    "source_url",
    "source_date",
    "congress",
    "chamber",
    "member_name",
    "member_state",
    "bioguide_id",
    "member_match_status",
    "committee_name",
    "subcommittee_name",
    "committee_code",
    "committee_match_status",
    "role",
    "source_text_sha256",
]


def normalize(value: str | None) -> str:
    value = unicodedata.normalize("NFKD", value or "").encode("ascii", "ignore").decode()
    return " ".join(re.sub(r"[^A-Z0-9 ]+", " ", value.upper()).split())


def assignment_member_key(value: str | None) -> str:
    """The surname key: the first comma component, or the whole name (``Van Drew``)."""

    value = re.sub(r"\s*\([A-Za-z]{2}(?:-[A-Za-z0-9]+)?\)\s*$", "", (value or "").strip())
    if "," in value:
        return value.split(",", 1)[0].strip()
    # The 119th full-directory table prints direct-order names, whereas
    # older assignment granules usually print surnames only.
    return value.rsplit(" ", 1)[-1].strip()


def assignment_member_state(value: str | None) -> str | None:
    district = re.search(r"\(([A-Za-z]{2})(?:-[A-Za-z0-9]+)?\)\s*$", value or "", flags=re.I)
    if district:
        return district.group(1).upper()
    match = re.search(r"\bof\s+([A-Za-z][A-Za-z .'-]+?)\s*$", value or "", flags=re.I)
    return match.group(1).strip() if match else None


def clean_html(value: str) -> str:
    value = re.sub(r"<[^>]+>", "", value)
    return html.unescape(value).replace("\r", "")


def official_text(response_text: str) -> str:
    match = re.search(r"<pre[^>]*>(.*?)</pre>", response_text, flags=re.I | re.S)
    return clean_html(match.group(1) if match else response_text)


def _role_and_name(value: str) -> tuple[str, str | None]:
    role_match = re.search(
        r"(?:,|\s+)\s*(chair(?:man|woman)?|ranking minority member|ranking member|vice chair(?:man|woman)?)\.?\s*$",
        value,
        flags=re.I,
    )
    if role_match is None:
        return value.rstrip(" .,"), None
    role = normalize(role_match.group(1)).lower().replace(" ", "_")
    return value[: role_match.start()].rstrip(" .,"), role


def _committee_segments(value: str) -> list[str]:
    """Split the compact directory list without splitting punctuation in names."""

    value = value.strip(" .")
    if not value or normalize(value) in {"N A", "NA"}:
        return []
    return [part.strip(" .") for part in re.split(r"(?<=\.)\s+(?=[A-Z])", value) if part.strip(" .")]


def _parse_compact_house(text: str) -> list[dict[str, str | None]]:
    """Parse 118th-edition House rows that place multiple members on one line."""

    start = text.find("ASSIGNMENTS OF REPRESENTATIVES")
    if start < 0:
        return []
    end_candidates = [
        position for marker in ("HOUSE LEADERSHIP", "PARTY LEADERSHIP COMMITTEES")
        if (position := text.find(marker, start + 1)) >= 0
    ]
    end = min(end_candidates) if end_candidates else -1
    section = text[start:] if end < 0 else text[start:end]
    # A few names wrap immediately before their district token in extracted
    # text.  Joining only that shape cannot merge separate roster rows.
    lines: list[str] = []
    for line in section.splitlines():
        if re.match(r"^\s*\([A-Z]{2}(?:-[A-Z0-9]+)?\)", line) and lines:
            lines[-1] = f"{lines[-1]} {line.strip()}"
        else:
            lines.append(line)
    token = r"(?:[A-Z](?:\.[A-Z])+\.?|[A-Z][A-Za-z'’\-]*|[A-Z]\.|Jr[.,]?|Sr[.,]?|II|III|IV)"
    pattern = re.compile(
        rf"(?:^|(?<=\s))((?:{token})(?:\s+({token})){{0,7}}\s+\([A-Z]{{2}}(?:-[A-Z0-9]+)?\))"
        rf"\s*(?:\.{{3,}}\s+|\s+)"
    )
    rows: list[dict[str, str | None]] = []
    for line in lines:
        matches = list(pattern.finditer(line))
        for ordinal, match in enumerate(matches):
            rest_end = matches[ordinal + 1].start() if ordinal + 1 < len(matches) else len(line)
            assignment = line[match.end():rest_end]
            for segment in _committee_segments(assignment):
                assignment_name, role = _role_and_name(segment)
                rows.append(
                    {
                        "member_name": match.group(1).strip(" .'-"),
                        "member_state": assignment_member_state(match.group(1)),
                        "committee_name": assignment_name,
                        "subcommittee_name": None,
                        "role": role,
                    }
                )
    return rows


def parse_assignments(text: str) -> list[dict[str, str | None]]:
    """Parse both modern dotted and older party-marked CDIR layouts."""

    compact_house_layout = bool(re.search(r"\([A-Z]{2}-[A-Z0-9]+\)\s+\.{3,}", text))
    if compact_house_layout:
        return _parse_compact_house(text)

    rows: list[dict[str, str | None]] = []
    member: str | None = None
    entries: list[list[str]] = []
    current_entry: list[str] = []

    def flush_entry() -> None:
        nonlocal current_entry
        if current_entry:
            entries.append(current_entry)
        current_entry = []

    def flush_member() -> None:
        nonlocal member, entries, current_entry
        flush_entry()
        if member:
            for entry in entries:
                assignment, role = _role_and_name(" ".join(entry))
                parent, separator, children = assignment.partition("--")
                child_names = [
                    part.strip(" .") for part in children.split(";") if part.strip(" .")
                ] if separator else []
                for segment in _committee_segments(parent):
                    rows.append(
                        {
                            "member_name": member.strip(" .'-"),
                            "member_state": assignment_member_state(member),
                            "committee_name": segment,
                            "subcommittee_name": "; ".join(child_names) or None,
                            "role": role,
                        }
                    )
        member = None
        entries = []
        current_entry = []

    for raw_line in text.splitlines():
        line = raw_line.rstrip()
        # Modern editions use ``Member....   Committee``; older editions use
        # ``Member (R)       Committee``.  Restrict the name to the left side
        # of the fixed-column separator so headers are not mistaken for rows.
        match = re.match(
            r"^\s*([A-Za-z0-9][A-Za-z0-9 .,()\u2019'\-\"`]+?)\.{3,}(?:\s+(.+?))?\s*$", line
        ) or re.match(
            r"^\s*([A-Za-z0-9][A-Za-z0-9 .,()\u2019'\-\"`]+?)\s+\([A-Za-z]+\)\s{2,}(.+?)\s*$",
            line,
        )
        if match:
            rest = (match.group(2) or "").strip()
            # House assignment tables wrap some full names as
            # ``Bice, Stephanie I., of`` followed by ``Oklahoma....``.  The
            # first line has dots but no committee; retain its member while
            # consuming the next line's committee text.
            if member and current_entry and not current_entry[-1].strip(" ."):
                member = f"{member} {match.group(1).strip()}"
                current_entry = [rest]
            else:
                flush_member()
                member = match.group(1).strip(" .'-")
                current_entry = [rest]
            continue

        stripped = line.strip()
        if member and re.match(r"^\s{20,}\S", line):
            if not stripped.startswith(("[", "[[Page")) and not re.match(r"^-{5,}$", stripped):
                # An entry ending in punctuation is complete; the next
                # indented line starts another committee.  Otherwise this is
                # a wrapped committee name.
                if current_entry and re.search(r"(?:\.|;|,\s*(?:chair|ranking|vice))$", current_entry[-1], re.I):
                    flush_entry()
                current_entry.append(stripped)
                continue

        if not stripped or stripped.startswith("[[Page"):
            flush_member()

    flush_member()
    return rows


def _database_url() -> str:
    return (
        f"postgresql://{os.environ.get('DB_USER', 'admin')}:{os.environ['DB_PASSWORD']}@"
        f"127.0.0.1:{os.environ.get('DB_PORT_FORWARD', '5433')}/"
        f"{os.environ.get('DB_NAME', 'bicam_collection')}"
    )


def _member_candidates(connection: psycopg.Connection) -> dict[tuple[int, str, str, str | None], list[str]]:
    candidates: dict[tuple[int, str, str, str | None], list[str]] = defaultdict(list)
    rows = connection.execute(
        """
        select m.bioguide_id, m.last_name, m.direct_order_name,
               m.inverted_order_name, t.congress, t.chamber,
               t.state_code, t.state_name
        from bicam.members as m
        join bicam.members_terms as t using (bioguide_id)
        where t.congress is not null
        """
    ).fetchall()
    for bioguide_id, last_name, direct_name, inverted_name, congress, chamber, state_code, state_name in rows:
        normalized_chamber = "house" if chamber == "house of representatives" else chamber
        surnames = {normalize(last_name)}
        full_names = set()
        for name in (direct_name, inverted_name):
            if name:
                surnames.add(normalize(assignment_member_key(name)))
                full_names.add(normalize(name))
        for surname in surnames | full_names:
            candidates[(int(congress), normalized_chamber, surname, None)].append(bioguide_id)
            for state in (state_code, state_name):
                if state:
                    candidates[(int(congress), normalized_chamber, surname, normalize(state))].append(bioguide_id)
    return candidates


def _committee_candidates(connection: psycopg.Connection) -> list[tuple[str, str, str, date | None, date | None]]:
    rows = connection.execute(
        """
        select h.committee_code, h.name, coalesce(c.chamber, ''), h.started_at, h.ended_at
        from bicam.committees_history as h
        left join bicam.committees as c using (committee_code)
        where nullif(trim(h.name), '') is not null
        union all
        select committee_code, name, coalesce(chamber, ''), null, null
        from bicam.committees
        where nullif(trim(name), '') is not null
        """
    ).fetchall()
    return [(str(code), str(name), str(chamber), started, ended) for code, name, chamber, started, ended in rows]


def _committee_match(
    name: str,
    chamber: str,
    source_date: date,
    candidates: list[tuple[str, str, str, date | None, date | None]],
) -> tuple[str | None, str]:
    """Use exact normalized lexical aliases plus historical date/chamber scope."""

    def variants(value: str) -> set[str]:
        result = {normalize(value)}
        changed = True
        while changed:
            changed = False
            for item in tuple(result):
                for prefix in ("COMMITTEE ON ", "SUBCOMMITTEE ON ", "HOUSE ", "SENATE "):
                    if item.startswith(prefix):
                        candidate = item.removeprefix(prefix)
                        if candidate and candidate not in result:
                            result.add(candidate)
                            changed = True
                if item.endswith(" COMMITTEE"):
                    candidate = item.removesuffix(" COMMITTEE")
                    if candidate and candidate not in result:
                        result.add(candidate)
                        changed = True
        return result

    # Older House editions use ``Parent: subcommittee; ...`` while some use
    # ``Parent -- subcommittee``.  Only the parent name is used for the
    # canonical committee edge; the raw subcommittee text is retained.
    target = re.split(r"\s*(?::|--)\s*", name, maxsplit=1)[0].strip()
    target_variants = variants(target)
    matches: set[str] = set()
    for code, candidate_name, candidate_chamber, started, ended in candidates:
        # Joint committees appear in both the Senate and House assignment
        # sections, so they are valid candidates for either source chamber.
        if candidate_chamber and candidate_chamber not in {chamber, "joint"}:
            continue
        if started and source_date < started:
            continue
        if ended and source_date > ended:
            continue
        candidate_variants = variants(candidate_name)
        if target_variants & candidate_variants:
            matches.add(code)
    if len(matches) == 1:
        return next(iter(matches)), "matched"
    if len(matches) > 1:
        return None, "ambiguous"
    return None, "unmatched"


def _csv_value(value: object) -> str:
    if value is None:
        return ""
    return str(value)


def capture(
    output: Path,
    *,
    min_congress: int | None = None,
    max_congress: int | None = None,
    directory_text: list[Path] | None = None,
) -> dict[str, object]:
    import csv

    output.parent.mkdir(parents=True, exist_ok=True)
    session = requests.Session()
    with psycopg.connect(_database_url(), options="-c default_transaction_read_only=on") as connection:
        packages = connection.execute(
            """
            select r.response->>'packageId', pd.response->>'congress',
                   r.response->>'dateIssued', r.response->>'granuleId',
                   r.response->>'title'
            from public.raw_response_current as r
            left join public.raw_response_current as pd
              on pd.node_type = 'govinfo_congressional_directory_package_detail'
             and pd.response->>'packageId' = r.response->>'packageId'
            where r.node_type = 'govinfo_congressional_directory_granule_detail'
              and r.response->>'granuleClass' = any(%s)
            order by r.response->>'packageId', r.response->>'granuleId'
            """,
            (list(ASSIGNMENT_CLASSES),),
        ).fetchall()
        directory_metadata = {
            package_id: (int(congress), issued_at)
            for package_id, congress, issued_at in connection.execute(
                "select package_id, congress_number, issued_at "
                "from public.stg_govinfo__congressional_directories "
                "where package_id is not null and congress_number is not null"
            ).fetchall()
        }
        members = _member_candidates(connection)
        committees = _committee_candidates(connection)

    source_specs = [(*row, None) for row in packages]
    for directory_path in directory_text or []:
        package_match = re.search(r"CDIR-\d{4}-\d{2}-\d{2}", directory_path.name)
        if package_match is None or package_match.group(0) not in directory_metadata:
            raise ValueError(f"directory text filename is not a harvested GovInfo package: {directory_path}")
        package_id = package_match.group(0)
        congress, source_date = directory_metadata[package_id]
        text = directory_path.read_text(encoding="utf-8")
        senate_start = text.find("ASSIGNMENTS OF SENATORS")
        house_start = text.find("ASSIGNMENTS OF REPRESENTATIVES", senate_start + 1)
        house_end_candidates = [
            position for marker in ("HOUSE LEADERSHIP", "PARTY LEADERSHIP COMMITTEES")
            if (position := text.find(marker, house_start + 1)) >= 0
        ]
        house_end = min(house_end_candidates) if house_end_candidates else -1
        if senate_start < 0 or house_start < 0:
            raise ValueError(f"directory assignment sections not found: {directory_path}")
        if house_end < 0:
            house_end = len(text)
        source_specs.extend(
            [
                (
                    package_id,
                    str(congress),
                    str(source_date),
                    f"{package_id}-ASSIGNMENTS-SENATE-TEXT",
                    "ASSIGNMENTS OF SENATORS TO COMMITTEES",
                    text[senate_start:house_start],
                ),
                (
                    package_id,
                    str(congress),
                    str(source_date),
                    f"{package_id}-ASSIGNMENTS-HOUSE-TEXT",
                    "ASSIGNMENTS OF REPRESENTATIVES, DELEGATES, AND RESIDENT COMMISSIONER TO COMMITTEES",
                    text[house_start:house_end],
                ),
            ]
        )

    records: list[dict[str, object]] = []
    stats = defaultdict(int)
    for package_id, congress_text, source_date_text, granule_id, title, source_override in source_specs:
        if congress_text is None or source_date_text is None:
            stats["skipped_missing_metadata"] += 1
            continue
        congress = int(congress_text)
        if min_congress is not None and congress < min_congress:
            continue
        if max_congress is not None and congress > max_congress:
            continue
        source_date = date.fromisoformat(str(source_date_text)[:10])
        chamber = "senate" if "SENATORS" in str(title).upper() else "house"
        source_url = (
            f"https://www.govinfo.gov/content/pkg/{package_id}/text/{package_id}.txt"
            if source_override is not None
            else f"https://www.govinfo.gov/content/pkg/{package_id}/html/{granule_id}.htm"
        )
        if source_override is None:
            response = session.get(source_url, timeout=(10, 60))
            response.raise_for_status()
            text = official_text(response.text)
        else:
            text = source_override
        digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
        parsed = parse_assignments(text)
        stats["granules"] += 1
        stats["parsed_rows"] += len(parsed)
        for row in parsed:
            if str(row["member_name"] or "").strip().lower().startswith("room "):
                stats["skipped_nonmember_row"] += 1
                continue
            surname = normalize(assignment_member_key(row["member_name"]))
            state = normalize(row["member_state"]) if row["member_state"] else None
            member_ids = members.get((congress, chamber, surname, state), []) if state else []
            if not member_ids:
                member_ids = members.get((congress, chamber, surname, None), [])
            if len(member_ids) != 1:
                # Direct-order Senate rows often use ``Rick Scott, of FL``;
                # try the complete name before the location phrase.
                full_name = re.sub(r"\s*\([A-Za-z]{2}(?:-[A-Za-z0-9]+)?\)\s*$", "", str(row["member_name"]), flags=re.I)
                full_name = re.sub(r",?\s+of\s+.+$", "", full_name, flags=re.I)
                full_key = normalize(full_name)
                full_ids = members.get((congress, chamber, full_key, state), []) if state else []
                if not full_ids:
                    full_ids = members.get((congress, chamber, full_key, None), [])
                if full_ids:
                    member_ids = full_ids
            if len(member_ids) == 1:
                bioguide_id, member_status = member_ids[0], "matched"
            elif len(member_ids) > 1:
                bioguide_id, member_status = None, "ambiguous"
            else:
                bioguide_id, member_status = None, "unmatched"
            committee_code, committee_status = _committee_match(
                str(row["committee_name"]), chamber, source_date, committees
            )
            stats[f"member_{member_status}"] += 1
            stats[f"committee_{committee_status}"] += 1
            records.append(
                {
                    "source_package_id": package_id,
                    "source_granule_id": granule_id,
                    "source_url": source_url,
                    "source_date": source_date.isoformat(),
                    "congress": congress,
                    "chamber": chamber,
                    "member_name": row["member_name"],
                    "member_state": row["member_state"],
                    "bioguide_id": bioguide_id,
                    "member_match_status": member_status,
                    "committee_name": row["committee_name"],
                    "subcommittee_name": row["subcommittee_name"],
                    "committee_code": committee_code,
                    "committee_match_status": committee_status,
                    "role": row["role"],
                    "source_text_sha256": digest,
                }
            )

    records.sort(key=lambda row: tuple(str(row[column] or "") for column in SEED_COLUMNS))
    temporary = output.with_suffix(output.suffix + ".tmp")
    with temporary.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=SEED_COLUMNS)
        writer.writeheader()
        writer.writerows({column: _csv_value(row[column]) for column in SEED_COLUMNS} for row in records)
    temporary.replace(output)
    return {"output": str(output), "rows": len(records), "stats": dict(stats)}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--min-congress", type=int)
    parser.add_argument("--max-congress", type=int)
    parser.add_argument("--directory-text", type=Path, action="append",
                        help="Also parse a captured full-directory text file; repeat for multiple packages.")
    args = parser.parse_args()
    import json

    print(json.dumps(capture(args.output, min_congress=args.min_congress, max_congress=args.max_congress,
                             directory_text=args.directory_text), indent=2))


if __name__ == "__main__":
    main()
