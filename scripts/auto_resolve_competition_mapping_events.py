"""
auto_resolve_competition_mapping_events.py
──────────────────────────────────────────
Automatischer Resolver für competition_mapping_events.

Ablauf:
1. lädt offene competition_mapping_events
2. lädt kanonische competitions aus der DB
3. bestimmt Kandidaten
4. trifft automatische Entscheidung:
   - approve
   - approve_provisional
   - no_match
5. schreibt/upsertet competition_external_map
6. markiert Events als processed

Environment:
  DATABASE_URL

Optional:
  COMP_MAPPING_BATCH_SIZE=200
  COMP_MAPPING_APPROVE_THRESHOLD=0.95
  COMP_MAPPING_PROVISIONAL_THRESHOLD=0.80
  COMP_MAPPING_MIN_CANDIDATE_SCORE=0.45
  COMP_MAPPING_MAX_CANDIDATES=5
"""

import json
import logging
import os
import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from psycopg2.extras import RealDictCursor


# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────

DATABASE_URL = os.environ["DATABASE_URL"]

BATCH_SIZE = int(os.getenv("COMP_MAPPING_BATCH_SIZE", "200"))
APPROVE_THRESHOLD = float(os.getenv("COMP_MAPPING_APPROVE_THRESHOLD", "0.95"))
PROVISIONAL_THRESHOLD = float(os.getenv("COMP_MAPPING_PROVISIONAL_THRESHOLD", "0.80"))
MIN_CANDIDATE_SCORE = float(os.getenv("COMP_MAPPING_MIN_CANDIDATE_SCORE", "0.45"))
MAX_CANDIDATES = int(os.getenv("COMP_MAPPING_MAX_CANDIDATES", "5"))


# ─────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("auto_resolve_competition_mapping_events")


# ─────────────────────────────────────────────────────────────
# NORMALIZATION
# ─────────────────────────────────────────────────────────────

GENERIC_REPLACEMENTS = {
    "ä": "ae",
    "ö": "oe",
    "ü": "ue",
    "ß": "ss",
}

STOPWORDS = {
    "league", "liga", "division", "cup", "uefa",
    "soccer", "football", "the", "de", "la", "el",
}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def normalize_text(value: Optional[str]) -> str:
    if not value:
        return ""

    value = value.strip().lower()

    for src, dst in GENERIC_REPLACEMENTS.items():
        value = value.replace(src, dst)

    value = unicodedata.normalize("NFKD", value)
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    value = value.replace("/", " ").replace("-", " ").replace("_", " ")
    value = re.sub(r"[^a-z0-9 ]+", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def normalize_competition_name(value: Optional[str]) -> str:
    norm = normalize_text(value)
    if not norm:
        return ""

    parts = [p for p in norm.split() if p not in STOPWORDS]
    return " ".join(parts).strip()


def token_set(value: str) -> set[str]:
    return set(value.split()) if value else set()


def jaccard_score(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    sa = token_set(a)
    sb = token_set(b)
    if not sa or not sb:
        return 0.0
    inter = len(sa & sb)
    union = len(sa | sb)
    return inter / union if union else 0.0


def substring_score(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0
    if a in b or b in a:
        shorter = min(len(a), len(b))
        longer = max(len(a), len(b))
        return shorter / longer if longer else 0.0
    return 0.0


def prefix_score(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    a_parts = a.split()
    b_parts = b.split()
    common = 0
    for x, y in zip(a_parts, b_parts):
        if x == y:
            common += 1
        else:
            break
    denom = max(len(a_parts), len(b_parts), 1)
    return common / denom


# ─────────────────────────────────────────────────────────────
# DATA CLASSES
# ─────────────────────────────────────────────────────────────

@dataclass
class CompetitionRecord:
    competition_code: str
    competition_name: str
    league_abbr: str
    competition_type: str
    country: str
    flag: Optional[str]
    is_active: bool


@dataclass
class Candidate:
    competition_code: str
    competition_name: str
    score: float
    reason_bits: List[str]
    matched_via: str


# ─────────────────────────────────────────────────────────────
# DB LOADERS
# ─────────────────────────────────────────────────────────────

def load_pending_events(conn, limit: int) -> List[Dict[str, Any]]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            select
              id,
              source_system,
              external_code,
              external_name,
              context,
              status,
              created_at
            from public.competition_mapping_events
            where status = 'new'
            order by created_at asc
            limit %s
            """,
            (limit,),
        )
        return cur.fetchall()


def load_competitions(conn) -> List[CompetitionRecord]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            select
              competition_code,
              competition_name,
              league_abbr,
              competition_type,
              country,
              flag,
              is_active
            from public.competitions
            order by competition_code asc
            """
        )
        rows = cur.fetchall()

    return [
        CompetitionRecord(
            competition_code=row["competition_code"],
            competition_name=row["competition_name"],
            league_abbr=row["league_abbr"],
            competition_type=row["competition_type"],
            country=row["country"],
            flag=row["flag"],
            is_active=bool(row["is_active"]),
        )
        for row in rows
    ]


def load_existing_external_mappings(conn, source_system: str) -> Dict[str, Dict[str, Any]]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            select
              id,
              competition_code,
              source_system,
              external_code,
              external_name,
              is_active
            from public.competition_external_map
            where source_system = %s
            """,
            (source_system,),
        )
        rows = cur.fetchall()

    return {row["external_code"]: dict(row) for row in rows}


# ─────────────────────────────────────────────────────────────
# CANDIDATE ENGINE
# ─────────────────────────────────────────────────────────────

def build_candidate_aliases(comp: CompetitionRecord) -> List[str]:
    name = comp.competition_name or ""
    code = comp.competition_code or ""
    abbr = comp.league_abbr or ""
    country = comp.country or ""

    raw_aliases = {
        name,
        code,
        abbr,
        f"{country} {name}",
        f"{country} {abbr}",
    }

    special_map = {
        "PREMIER_LEAGUE": [
            "premier league",
            "epl",
            "english premier league",
            "england premier league",
        ],
        "BUNDESLIGA": [
            "bundesliga",
            "germany bundesliga",
            "deutschland bundesliga",
        ],
        "LA_LIGA": [
            "la liga",
            "spain la liga",
            "spanien la liga",
        ],
        "SERIE_A": [
            "serie a",
            "italy serie a",
            "italien serie a",
        ],
        "LIGUE_1": [
            "ligue 1",
            "france ligue 1",
            "frankreich ligue 1",
        ],
        "EREDIVISIE": [
            "eredivisie",
            "netherlands eredivisie",
            "niederlande eredivisie",
        ],
        "PRIMEIRA_LIGA": [
            "primeira liga",
            "portugal primeira liga",
        ],
        "CHAMPIONSHIP": [
            "championship",
            "efl championship",
            "english championship",
        ],
        "CHAMPIONS_LEAGUE": [
            "champions league",
            "uefa champions league",
            "ucl",
        ],
        "EUROPA_LEAGUE": [
            "europa league",
            "uefa europa league",
            "uel",
        ],
        "SUPER_LIG": [
            "super lig",
            "superlig",
            "turkey super lig",
            "turkei super lig",
        ],
    }

    raw_aliases.update(special_map.get(comp.competition_code, []))

    normalized = []
    seen = set()
    for item in raw_aliases:
        norm = normalize_competition_name(item)
        if norm and norm not in seen:
            seen.add(norm)
            normalized.append(norm)

    return normalized


def score_candidate(raw_norm: str, candidate_norm: str) -> Tuple[float, List[str]]:
    if not raw_norm or not candidate_norm:
        return 0.0, []

    reasons: List[str] = []

    if raw_norm == candidate_norm:
        return 1.0, ["exact_match"]

    jac = jaccard_score(raw_norm, candidate_norm)
    sub = substring_score(raw_norm, candidate_norm)
    pre = prefix_score(raw_norm, candidate_norm)

    score = max(
        jac * 0.85,
        sub * 0.92,
        pre * 0.75,
    )

    if jac > 0:
        reasons.append(f"jaccard={jac:.3f}")
    if sub > 0:
        reasons.append(f"substring={sub:.3f}")
    if pre > 0:
        reasons.append(f"prefix={pre:.3f}")

    return round(score, 4), reasons


def build_candidates(
    event: Dict[str, Any],
    competitions: List[CompetitionRecord],
) -> List[Candidate]:
    raw_values = [
        event.get("external_code"),
        event.get("external_name"),
    ]

    norm_inputs = [normalize_competition_name(x) for x in raw_values if x]
    norm_inputs = [x for x in norm_inputs if x]

    if not norm_inputs:
        return []

    best_by_code: Dict[str, Candidate] = {}

    for comp in competitions:
        aliases = build_candidate_aliases(comp)

        best_score = 0.0
        best_reasons: List[str] = []
        best_via = "name"

        for raw_norm in norm_inputs:
            for alias_norm in aliases:
                score, reasons = score_candidate(raw_norm, alias_norm)
                if score > best_score:
                    best_score = score
                    best_reasons = [*reasons, f"alias={alias_norm}"]
                    best_via = "alias"

        # Competition code zusätzlich roh prüfen
        raw_code = normalize_text(event.get("external_code"))
        comp_code = normalize_text(comp.competition_code)
        if raw_code and comp_code:
            if raw_code == comp_code:
                best_score = max(best_score, 0.99)
                best_reasons = ["external_code_matches_competition_code"]
                best_via = "competition_code"

        # League Abbr prüfen
        raw_name = normalize_text(event.get("external_name"))
        abbr = normalize_text(comp.league_abbr)
        if raw_name and abbr and raw_name == abbr:
            if best_score < 0.90:
                best_score = 0.90
                best_reasons = ["external_name_matches_league_abbr"]
                best_via = "league_abbr"

        if best_score >= MIN_CANDIDATE_SCORE:
            best_by_code[comp.competition_code] = Candidate(
                competition_code=comp.competition_code,
                competition_name=comp.competition_name,
                score=best_score,
                reason_bits=best_reasons,
                matched_via=best_via,
            )

    candidates = sorted(best_by_code.values(), key=lambda x: (-x.score, x.competition_code))
    return candidates[:MAX_CANDIDATES]


# ─────────────────────────────────────────────────────────────
# DECISION ENGINE
# ─────────────────────────────────────────────────────────────

def decide(
    event: Dict[str, Any],
    candidates: List[Candidate],
) -> Tuple[str, Optional[Candidate], float, Dict[str, Any]]:
    if not candidates:
        reasoning = {
            "external_code": event.get("external_code"),
            "external_name": event.get("external_name"),
            "decision_basis": "no_candidates",
            "top_candidates": [],
        }
        return "no_match", None, 0.0, reasoning

    top = candidates[0]
    second = candidates[1] if len(candidates) > 1 else None
    margin = round(top.score - second.score, 4) if second else round(top.score, 4)

    if top.score >= APPROVE_THRESHOLD and margin >= 0.08:
        decision = "approve"
    elif top.score >= PROVISIONAL_THRESHOLD and margin >= 0.05:
        decision = "approve_provisional"
    else:
        decision = "no_match"

    reasoning = {
        "external_code": event.get("external_code"),
        "external_name": event.get("external_name"),
        "top_candidate": {
            "competition_code": top.competition_code,
            "competition_name": top.competition_name,
            "score": top.score,
            "matched_via": top.matched_via,
            "reason_bits": top.reason_bits,
        },
        "second_candidate": None if not second else {
            "competition_code": second.competition_code,
            "competition_name": second.competition_name,
            "score": second.score,
            "matched_via": second.matched_via,
            "reason_bits": second.reason_bits,
        },
        "score_margin": margin,
        "thresholds": {
            "approve": APPROVE_THRESHOLD,
            "approve_provisional": PROVISIONAL_THRESHOLD,
            "min_candidate_score": MIN_CANDIDATE_SCORE,
        },
        "all_candidates": [
            {
                "competition_code": c.competition_code,
                "competition_name": c.competition_name,
                "score": c.score,
                "matched_via": c.matched_via,
                "reason_bits": c.reason_bits,
            }
            for c in candidates
        ],
    }

    return decision, top, top.score, reasoning


# ─────────────────────────────────────────────────────────────
# DB WRITERS
# ─────────────────────────────────────────────────────────────

def upsert_competition_external_map(
    conn,
    *,
    competition_code: str,
    source_system: str,
    external_code: str,
    external_name: Optional[str],
    is_active: bool,
) -> None:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            select id
            from public.competition_external_map
            where source_system = %s
              and external_code = %s
            limit 1
            """,
            (source_system, external_code),
        )
        existing = cur.fetchone()

        if existing:
            cur.execute(
                """
                update public.competition_external_map
                set
                  competition_code = %s,
                  external_name = %s,
                  is_active = %s,
                  updated_at = now()
                where id = %s
                """,
                (
                    competition_code,
                    external_name,
                    is_active,
                    existing["id"],
                ),
            )
        else:
            cur.execute(
                """
                insert into public.competition_external_map (
                  competition_code,
                  source_system,
                  external_code,
                  external_name,
                  is_active
                ) values (%s, %s, %s, %s, %s)
                """,
                (
                    competition_code,
                    source_system,
                    external_code,
                    external_name,
                    is_active,
                ),
            )


def update_event_status(conn, event_id: int, status: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            update public.competition_mapping_events
            set
              status = %s,
              processed_at = now()
            where id = %s
            """,
            (status, event_id),
        )


def insert_event_context_note(conn, event_id: int, payload: Dict[str, Any]) -> None:
    """
    Da competition_mapping_events aktuell keine decision-Tabelle hat,
    schreiben wir die Entscheidung in context zurück.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            update public.competition_mapping_events
            set context = coalesce(context, '{}'::jsonb) || %s::jsonb
            where id = %s
            """,
            (json.dumps(payload, ensure_ascii=False), event_id),
        )


# ─────────────────────────────────────────────────────────────
# MAIN PROCESSING
# ─────────────────────────────────────────────────────────────

def process_event(
    conn,
    event: Dict[str, Any],
    competitions: List[CompetitionRecord],
) -> str:
    candidates = build_candidates(event, competitions)
    decision, top_candidate, confidence, reasoning = decide(event, candidates)

    insert_event_context_note(
        conn,
        event["id"],
        {
            "auto_resolve_decision": {
                "decision": decision,
                "confidence": confidence,
                "reasoning": reasoning,
                "processed_at": utc_now().isoformat(),
            }
        },
    )

    if decision == "approve" and top_candidate:
        upsert_competition_external_map(
            conn,
            competition_code=top_candidate.competition_code,
            source_system=event["source_system"],
            external_code=event["external_code"],
            external_name=event.get("external_name"),
            is_active=True,
        )
        update_event_status(conn, event["id"], "resolved")
        return decision

    if decision == "approve_provisional" and top_candidate:
        # Bei competitions gibt es aktuell keinen separaten provisional-Status in competition_external_map.
        # Deshalb schreiben wir den Eintrag dennoch aktiv, aber nur bei ausreichender Sicherheit.
        upsert_competition_external_map(
            conn,
            competition_code=top_candidate.competition_code,
            source_system=event["source_system"],
            external_code=event["external_code"],
            external_name=event.get("external_name"),
            is_active=True,
        )
        update_event_status(conn, event["id"], "resolved")
        return decision

    if decision == "no_match":
        update_event_status(conn, event["id"], "ignored")
        return decision

    update_event_status(conn, event["id"], "rejected")
    return decision


def main() -> None:
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False

    try:
        events = load_pending_events(conn, BATCH_SIZE)
        if not events:
            logger.info("Keine offenen competition_mapping_events gefunden.")
            return

        competitions = load_competitions(conn)

        logger.info("Offene competition_mapping_events: %s", len(events))
        logger.info("Geladene competitions: %s", len(competitions))

        stats = {
            "approve": 0,
            "approve_provisional": 0,
            "no_match": 0,
            "reject": 0,
        }

        for event in events:
            try:
                decision = process_event(conn, event, competitions)
                stats[decision] = stats.get(decision, 0) + 1
                conn.commit()

                logger.info(
                    "event_id=%s | source=%s | external_code=%s | external_name=%s | decision=%s",
                    event["id"],
                    event["source_system"],
                    event.get("external_code"),
                    event.get("external_name"),
                    decision,
                )

            except Exception:
                conn.rollback()
                logger.exception(
                    "Fehler beim Verarbeiten von competition_mapping_event id=%s external_code=%s",
                    event["id"],
                    event.get("external_code"),
                )
                try:
                    update_event_status(conn, event["id"], "ignored")
                    conn.commit()
                except Exception:
                    conn.rollback()
                    logger.exception(
                        "Konnte competition_mapping_event nicht auf ignored setzen: id=%s",
                        event["id"],
                    )

        logger.info("══════════════════════════════════════")
        logger.info("AUTO RESOLVE COMPETITION SUMMARY")
        logger.info("  processed            : %s", len(events))
        logger.info("  approve              : %s", stats.get("approve", 0))
        logger.info("  approve_provisional  : %s", stats.get("approve_provisional", 0))
        logger.info("  no_match             : %s", stats.get("no_match", 0))
        logger.info("  reject               : %s", stats.get("reject", 0))
        logger.info("══════════════════════════════════════")

    finally:
        conn.close()


if __name__ == "__main__":
    main()