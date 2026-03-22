"""
auto_resolve_team_alias_events.py
─────────────────────────────────
Automatischer Resolver für team_alias_events.

Ablauf:
1. lädt offene team_alias_events
2. lädt Teams + aktive Aliase aus der DB
3. bestimmt Kandidaten
4. trifft automatische Entscheidung:
   - approve
   - approve_provisional
   - no_match
5. schreibt team_alias_decisions
6. legt/aktualisiert team_aliases an
7. markiert Events als processed

Environment:
  DATABASE_URL

Optional:
  ALIAS_AGENT_BATCH_SIZE=200
  ALIAS_AGENT_APPROVE_THRESHOLD=0.95
  ALIAS_AGENT_PROVISIONAL_THRESHOLD=0.78
  ALIAS_AGENT_MIN_CANDIDATE_SCORE=0.45
  ALIAS_AGENT_MAX_CANDIDATES=5
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
from psycopg2.extras import RealDictCursor, execute_values


# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────

DATABASE_URL = os.environ["DATABASE_URL"]

BATCH_SIZE = int(os.getenv("ALIAS_AGENT_BATCH_SIZE", "200"))
APPROVE_THRESHOLD = float(os.getenv("ALIAS_AGENT_APPROVE_THRESHOLD", "0.95"))
PROVISIONAL_THRESHOLD = float(os.getenv("ALIAS_AGENT_PROVISIONAL_THRESHOLD", "0.78"))
MIN_CANDIDATE_SCORE = float(os.getenv("ALIAS_AGENT_MIN_CANDIDATE_SCORE", "0.45"))
MAX_CANDIDATES = int(os.getenv("ALIAS_AGENT_MAX_CANDIDATES", "5"))


# ─────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("auto_resolve_team_alias_events")


# ─────────────────────────────────────────────────────────────
# NORMALIZATION
# technische Normalisierung, fachliche Wahrheit bleibt in DB
# ─────────────────────────────────────────────────────────────

STOPWORDS = {
    "fc", "sc", "sv", "ev", "afc", "bc", "cf",
    "sk", "fk", "club", "clube",
    "de", "da", "do", "del", "der", "di",
    "ac", "as", "ss", "tsg", "vfl", "vfb", "rc",
}

GENERIC_REPLACEMENTS = {
    "ä": "ae",
    "ö": "oe",
    "ü": "ue",
    "ß": "ss",
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
    value = value.replace("/", " ").replace("-", " ")
    value = re.sub(r"[^a-z0-9 ]+", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def normalize_team_name(value: Optional[str]) -> str:
    norm = normalize_text(value)
    if not norm:
        return ""

    parts = [p for p in norm.split() if p not in STOPWORDS]
    norm = " ".join(parts)
    norm = re.sub(r"\s+", " ", norm).strip()
    return norm


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
class TeamRecord:
    team_id: int
    canonical_name: str
    canonical_normalized_name: str
    country: Optional[str]


@dataclass
class AliasRecord:
    team_id: int
    canonical_name: str
    canonical_normalized_name: str
    alias_name: str
    alias_normalized: str
    source_system: Optional[str]
    competition_code: Optional[str]
    status: str
    confidence: Optional[float]


@dataclass
class Candidate:
    team_id: int
    canonical_name: str
    canonical_normalized_name: str
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
              competition_code,
              raw_name,
              raw_name_normalized,
              home_or_away,
              context,
              status,
              created_at
            from public.team_alias_events
            where status = 'new'
            order by created_at asc
            limit %s
            """,
            (limit,),
        )
        return cur.fetchall()


def load_teams(conn) -> List[TeamRecord]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            select
              id,
              canonical_name,
              normalized_name,
              country
            from public.teams
            order by canonical_name asc
            """
        )
        rows = cur.fetchall()

    return [
        TeamRecord(
            team_id=row["id"],
            canonical_name=row["canonical_name"],
            canonical_normalized_name=row["normalized_name"],
            country=row["country"],
        )
        for row in rows
    ]


def load_aliases(conn) -> List[AliasRecord]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            select
              team_id,
              canonical_name,
              canonical_normalized_name,
              alias_name,
              alias_normalized,
              source_system,
              competition_code,
              status,
              confidence
            from public.v_team_aliases_active
            """
        )
        rows = cur.fetchall()

    return [
        AliasRecord(
            team_id=row["team_id"],
            canonical_name=row["canonical_name"],
            canonical_normalized_name=row["canonical_normalized_name"],
            alias_name=row["alias_name"],
            alias_normalized=row["alias_normalized"],
            source_system=row["source_system"],
            competition_code=row["competition_code"],
            status=row["status"],
            confidence=float(row["confidence"]) if row["confidence"] is not None else None,
        )
        for row in rows
    ]


# ─────────────────────────────────────────────────────────────
# CANDIDATE ENGINE
# ─────────────────────────────────────────────────────────────

def alias_scope_matches(
    event_source_system: str,
    event_competition_code: Optional[str],
    alias_source_system: Optional[str],
    alias_competition_code: Optional[str],
) -> bool:
    source_ok = alias_source_system is None or alias_source_system == event_source_system
    comp_ok = alias_competition_code is None or alias_competition_code == event_competition_code
    return source_ok and comp_ok


def score_against_alias(
    raw_norm: str,
    alias_norm: str,
) -> Tuple[float, List[str]]:
    reasons: List[str] = []

    if raw_norm == alias_norm:
        return 1.0, ["exact_alias"]

    jac = jaccard_score(raw_norm, alias_norm)
    sub = substring_score(raw_norm, alias_norm)
    pre = prefix_score(raw_norm, alias_norm)

    score = max(
        jac * 0.85,
        sub * 0.90,
        pre * 0.75,
    )

    if jac > 0:
        reasons.append(f"alias_jaccard={jac:.3f}")
    if sub > 0:
        reasons.append(f"alias_substring={sub:.3f}")
    if pre > 0:
        reasons.append(f"alias_prefix={pre:.3f}")

    return round(score, 4), reasons


def score_against_team(
    raw_norm: str,
    team_norm: str,
) -> Tuple[float, List[str]]:
    reasons: List[str] = []

    if raw_norm == team_norm:
        return 0.98, ["exact_canonical"]

    jac = jaccard_score(raw_norm, team_norm)
    sub = substring_score(raw_norm, team_norm)
    pre = prefix_score(raw_norm, team_norm)

    score = max(
        jac * 0.80,
        sub * 0.88,
        pre * 0.70,
    )

    if jac > 0:
        reasons.append(f"canon_jaccard={jac:.3f}")
    if sub > 0:
        reasons.append(f"canon_substring={sub:.3f}")
    if pre > 0:
        reasons.append(f"canon_prefix={pre:.3f}")

    return round(score, 4), reasons


def build_candidates(
    event: Dict[str, Any],
    teams: List[TeamRecord],
    aliases: List[AliasRecord],
) -> List[Candidate]:
    raw_norm = event["raw_name_normalized"] or normalize_team_name(event["raw_name"])

    by_team: Dict[int, Candidate] = {}

    # 1) Alias-basiert
    for alias in aliases:
        if not alias_scope_matches(
            event_source_system=event["source_system"],
            event_competition_code=event["competition_code"],
            alias_source_system=alias.source_system,
            alias_competition_code=alias.competition_code,
        ):
            continue

        score, reason_bits = score_against_alias(raw_norm, alias.alias_normalized)
        if score < MIN_CANDIDATE_SCORE:
            continue

        existing = by_team.get(alias.team_id)
        new_reason_bits = [*reason_bits, f"via_alias={alias.alias_name}"]

        if existing is None or score > existing.score:
            by_team[alias.team_id] = Candidate(
                team_id=alias.team_id,
                canonical_name=alias.canonical_name,
                canonical_normalized_name=alias.canonical_normalized_name,
                score=score,
                reason_bits=new_reason_bits,
                matched_via="alias",
            )

    # 2) Kanonische Namen
    for team in teams:
        score, reason_bits = score_against_team(raw_norm, team.canonical_normalized_name)
        if score < MIN_CANDIDATE_SCORE:
            continue

        existing = by_team.get(team.team_id)
        new_reason_bits = [*reason_bits, "via_canonical"]

        if existing is None or score > existing.score:
            by_team[team.team_id] = Candidate(
                team_id=team.team_id,
                canonical_name=team.canonical_name,
                canonical_normalized_name=team.canonical_normalized_name,
                score=score,
                reason_bits=new_reason_bits,
                matched_via="canonical",
            )

    candidates = sorted(by_team.values(), key=lambda x: (-x.score, x.canonical_name))
    return candidates[:MAX_CANDIDATES]


# ─────────────────────────────────────────────────────────────
# DECISION ENGINE
# ─────────────────────────────────────────────────────────────

def decide(
    event: Dict[str, Any],
    candidates: List[Candidate],
) -> Tuple[str, Optional[Candidate], float, Dict[str, Any]]:
    """
    decision:
      approve
      approve_provisional
      no_match
      reject
    """
    raw_norm = event["raw_name_normalized"] or normalize_team_name(event["raw_name"])

    if not candidates:
        reasoning = {
            "raw_name": event["raw_name"],
            "raw_name_normalized": raw_norm,
            "top_candidates": [],
            "decision_basis": "no_candidates",
        }
        return "no_match", None, 0.0, reasoning

    top = candidates[0]
    second = candidates[1] if len(candidates) > 1 else None
    margin = round(top.score - second.score, 4) if second else round(top.score, 4)

    decision = "no_match"

    # Hohe Sicherheit
    if top.score >= APPROVE_THRESHOLD and margin >= 0.08:
        decision = "approve"
    # Mittlere Sicherheit → scoped alias
    elif top.score >= PROVISIONAL_THRESHOLD and margin >= 0.05:
        decision = "approve_provisional"
    else:
        decision = "no_match"

    reasoning = {
        "raw_name": event["raw_name"],
        "raw_name_normalized": raw_norm,
        "top_candidate": {
            "team_id": top.team_id,
            "canonical_name": top.canonical_name,
            "canonical_normalized_name": top.canonical_normalized_name,
            "score": top.score,
            "matched_via": top.matched_via,
            "reason_bits": top.reason_bits,
        },
        "second_candidate": None if not second else {
            "team_id": second.team_id,
            "canonical_name": second.canonical_name,
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
                "team_id": c.team_id,
                "canonical_name": c.canonical_name,
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

def insert_decision(
    conn,
    event_id: int,
    suggested_team_id: Optional[int],
    decision: str,
    confidence: float,
    reasoning: Dict[str, Any],
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into public.team_alias_decisions (
              event_id,
              suggested_team_id,
              decision,
              confidence,
              reasoning
            ) values (%s, %s, %s, %s, %s)
            """,
            (
                event_id,
                suggested_team_id,
                decision,
                confidence,
                json.dumps(reasoning, ensure_ascii=False),
            ),
        )


def upsert_alias(
    conn,
    *,
    team_id: int,
    source_system: Optional[str],
    competition_code: Optional[str],
    alias_name: str,
    alias_normalized: str,
    status: str,
    decision_source: str,
    confidence: float,
    explanation: str,
) -> None:
    """
    Es gibt noch keinen dedizierten Unique-Key auf alias scope.
    Daher zuerst per select suchen, dann insert/update.
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            select id, usage_count
            from public.team_aliases
            where team_id = %s
              and coalesce(source_system, '') = coalesce(%s, '')
              and coalesce(competition_code, '') = coalesce(%s, '')
              and alias_normalized = %s
            limit 1
            """,
            (team_id, source_system, competition_code, alias_normalized),
        )
        existing = cur.fetchone()

        if existing:
            cur.execute(
                """
                update public.team_aliases
                set
                  alias_name = %s,
                  status = %s,
                  decision_source = %s,
                  confidence = %s,
                  explanation = %s,
                  usage_count = coalesce(usage_count, 0) + 1,
                  last_seen_at = now(),
                  updated_at = now()
                where id = %s
                """,
                (
                    alias_name,
                    status,
                    decision_source,
                    confidence,
                    explanation,
                    existing["id"],
                ),
            )
        else:
            cur.execute(
                """
                insert into public.team_aliases (
                  team_id,
                  source_system,
                  competition_code,
                  alias_name,
                  alias_normalized,
                  status,
                  decision_source,
                  confidence,
                  explanation,
                  usage_count,
                  last_seen_at
                ) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, 1, now())
                """,
                (
                    team_id,
                    source_system,
                    competition_code,
                    alias_name,
                    alias_normalized,
                    status,
                    decision_source,
                    confidence,
                    explanation,
                ),
            )


def update_event_status(conn, event_id: int, status: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            update public.team_alias_events
            set
              status = %s,
              processed_at = now()
            where id = %s
            """,
            (status, event_id),
        )


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def process_event(
    conn,
    event: Dict[str, Any],
    teams: List[TeamRecord],
    aliases: List[AliasRecord],
) -> str:
    candidates = build_candidates(event, teams, aliases)
    decision, top_candidate, confidence, reasoning = decide(event, candidates)

    explanation = ""
    if top_candidate:
        explanation = (
            f"auto_resolve matched '{event['raw_name']}' → '{top_candidate.canonical_name}' "
            f"mit score={top_candidate.score:.4f} via={top_candidate.matched_via}"
        )
    else:
        explanation = f"auto_resolve found no match for '{event['raw_name']}'"

    insert_decision(
        conn=conn,
        event_id=event["id"],
        suggested_team_id=top_candidate.team_id if top_candidate else None,
        decision=decision,
        confidence=confidence,
        reasoning=reasoning,
    )

    if decision == "approve" and top_candidate:
        upsert_alias(
            conn,
            team_id=top_candidate.team_id,
            source_system=None,  # global alias bei hoher Sicherheit
            competition_code=None,
            alias_name=event["raw_name"],
            alias_normalized=event["raw_name_normalized"],
            status="approved",
            decision_source="ai_agent",
            confidence=confidence,
            explanation=explanation,
        )
        update_event_status(conn, event["id"], "resolved")
        return decision

    if decision == "approve_provisional" and top_candidate:
        upsert_alias(
            conn,
            team_id=top_candidate.team_id,
            source_system=event["source_system"],          # scoped
            competition_code=event["competition_code"],   # scoped
            alias_name=event["raw_name"],
            alias_normalized=event["raw_name_normalized"],
            status="provisional",
            decision_source="ai_agent",
            confidence=confidence,
            explanation=explanation,
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
            logger.info("Keine offenen team_alias_events gefunden.")
            return

        logger.info("Offene team_alias_events: %s", len(events))

        teams = load_teams(conn)
        aliases = load_aliases(conn)

        stats = {
            "approve": 0,
            "approve_provisional": 0,
            "no_match": 0,
            "reject": 0,
        }

        for event in events:
            try:
                decision = process_event(conn, event, teams, aliases)
                stats[decision] = stats.get(decision, 0) + 1
                conn.commit()

                logger.info(
                    "event_id=%s | raw_name=%s | decision=%s",
                    event["id"],
                    event["raw_name"],
                    decision,
                )

                # Aliasliste nach jedem erfolgreichen Commit frisch halten,
                # damit spätere Events im selben Lauf die neuen Aliase schon sehen.
                aliases = load_aliases(conn)

            except Exception:
                conn.rollback()
                logger.exception(
                    "Fehler beim Verarbeiten von team_alias_event id=%s raw_name=%s",
                    event["id"],
                    event["raw_name"],
                )

                # Event nicht verloren gehen lassen
                try:
                    update_event_status(conn, event["id"], "ignored")
                    conn.commit()
                except Exception:
                    conn.rollback()
                    logger.exception("Konnte Event-Status nicht auf ignored setzen: id=%s", event["id"])

        logger.info("══════════════════════════════════════")
        logger.info("AUTO RESOLVE SUMMARY")
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