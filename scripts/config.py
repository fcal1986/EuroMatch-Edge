"""
config.py
─────────
EuroMatch Edge — central competition configuration.

Each entry in COMPETITIONS defines a single source competition.
To add a new league: append one CompetitionConfig to COMPETITIONS.
To disable a league: set active=False — it stays defined but won't be fetched.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class CompetitionConfig:
    source_code:      str   # Code used by the external API (football-data.org)
    competition_code: str   # Our internal stable identifier (used in Supabase + frontend)
    competition_name: str   # Human-readable display name
    league_abbr:      str   # Short label for the UI
    competition_type: str   # 'league' | 'cup' | 'international'
    country:          str   # Country or region display name
    flag:             str   # Emoji flag (nullable in DB, but defined here for all known entries)
    active:           bool = True  # Set False to disable without deleting the definition


COMPETITIONS: list[CompetitionConfig] = [
    CompetitionConfig(
        source_code="PL",
        competition_code="PREMIER_LEAGUE",
        competition_name="Premier League",
        league_abbr="PL",
        competition_type="league",
        country="England",
        flag="🏴󠁧󠁢󠁥󠁮󠁧󠁿",
    ),
    CompetitionConfig(
        source_code="BL1",
        competition_code="BUNDESLIGA",
        competition_name="Bundesliga",
        league_abbr="BL",
        competition_type="league",
        country="Deutschland",
        flag="🇩🇪",
    ),
    CompetitionConfig(
        source_code="PD",
        competition_code="LA_LIGA",
        competition_name="La Liga",
        league_abbr="LAL",
        competition_type="league",
        country="Spanien",
        flag="🇪🇸",
    ),
    CompetitionConfig(
        source_code="SA",
        competition_code="SERIE_A",
        competition_name="Serie A",
        league_abbr="SA",
        competition_type="league",
        country="Italien",
        flag="🇮🇹",
    ),
    CompetitionConfig(
        source_code="FL1",
        competition_code="LIGUE_1",
        competition_name="Ligue 1",
        league_abbr="L1",
        competition_type="league",
        country="Frankreich",
        flag="🇫🇷",
    ),
    CompetitionConfig(
        source_code="DED",
        competition_code="EREDIVISIE",
        competition_name="Eredivisie",
        league_abbr="ERE",
        competition_type="league",
        country="Niederlande",
        flag="🇳🇱",
    ),
    CompetitionConfig(
        source_code="PPL",
        competition_code="PRIMEIRA_LIGA",
        competition_name="Primeira Liga",
        league_abbr="PL-P",
        competition_type="league",
        country="Portugal",
        flag="🇵🇹",
    ),
    CompetitionConfig(
        source_code="TSL",
        competition_code="SUPER_LIG",
        competition_name="Süper Lig",
        league_abbr="SL",
        competition_type="league",
        country="Türkei",
        flag="🇹🇷",
    ),
    CompetitionConfig(
        source_code="ELC",
        competition_code="CHAMPIONSHIP",
        competition_name="Championship",
        league_abbr="CH",
        competition_type="league",
        country="England",
        flag="🏴󠁧󠁢󠁥󠁮󠁧󠁿",
    ),
    CompetitionConfig(
        source_code="CL",
        competition_code="CHAMPIONS_LEAGUE",
        competition_name="Champions League",
        league_abbr="UCL",
        competition_type="international",
        country="Europa",
        flag="🌍",
    ),
    CompetitionConfig(
        source_code="EL",
        competition_code="EUROPA_LEAGUE",
        competition_name="Europa League",
        league_abbr="UEL",
        competition_type="international",
        country="Europa",
        flag="🌍",
    ),
    # ── Inactive / future competitions ───────────────────────
    # Uncomment or set active=True to enable.
    #
    # CompetitionConfig(
    #     source_code="UECL",
    #     competition_code="CONFERENCE_LEAGUE",
    #     competition_name="Conference League",
    #     league_abbr="UECL",
    #     competition_type="international",
    #     country="Europa",
    #     flag="🌍",
    #     active=False,
    # ),
    # CompetitionConfig(
    #     source_code="BL2",
    #     competition_code="BUNDESLIGA_2",
    #     competition_name="2. Bundesliga",
    #     league_abbr="BL2",
    #     competition_type="league",
    #     country="Deutschland",
    #     flag="🇩🇪",
    #     active=False,
    # ),
]

# ── Lookup index: source_code → CompetitionConfig
# Built once at import time. Used by fetch_matches.py for O(1) access.
COMPETITION_BY_SOURCE: dict[str, CompetitionConfig] = {
    c.source_code: c for c in COMPETITIONS
}

# ── Active source codes — the only list fetch_matches.py iterates
ACTIVE_SOURCE_CODES: list[str] = [
    c.source_code for c in COMPETITIONS if c.active
]
