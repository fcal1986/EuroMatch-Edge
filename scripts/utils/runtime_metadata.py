"""
utils/runtime_metadata.py
─────────────────────────
EuroMatch Edge — Versioning & Runtime Metadata Layer

Verantwortlichkeiten:
  - system_version aus ENV ermitteln (APP_VERSION → GITHUB_SHA → "dev")
  - pipeline_run_id generieren (UUID4)
  - pipeline_runs in Supabase schreiben (start + finish)
  - Alle Fehler werden geloggt, aber nie weitergeworfen —
    Metadaten-Fehler dürfen die Pipeline nie abbrechen.

Verwendung:
    from utils.runtime_metadata import get_system_version, start_pipeline_run, finish_pipeline_run

    version = get_system_version()
    run_id  = start_pipeline_run("compute_predictions")
    try:
        # ... pipeline logic ...
    finally:
        finish_pipeline_run(run_id, status="success")
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any

import requests

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────

_SUPABASE_URL_ENV      = "SUPABASE_URL"
_SUPABASE_KEY_ENV      = "SUPABASE_SERVICE_ROLE_KEY"
_APP_VERSION_ENV       = "APP_VERSION"
_GITHUB_SHA_ENV        = "GITHUB_SHA"
_GITHUB_REF_NAME_ENV   = "GITHUB_REF_NAME"

_PIPELINE_RUNS_TABLE   = "pipeline_runs"
_SYSTEM_RELEASES_TABLE = "system_releases"

_REQUEST_TIMEOUT_S     = 10


# ─────────────────────────────────────────────────────────────
# PUBLIC API
# ─────────────────────────────────────────────────────────────

def get_system_version() -> str:
    """
    Ermittelt die aktuelle Systemversion.

    Priorität:
      1. ENV APP_VERSION        (explizit gesetzt, z.B. "v2.1.0")
      2. ENV GITHUB_SHA[:8]     (kurzer Commit-Hash aus GitHub Actions)
      3. "dev"                  (lokale Entwicklungsumgebung)
    """
    v = (
        os.environ.get(_APP_VERSION_ENV, "").strip()
        or os.environ.get(_GITHUB_SHA_ENV, "")[:8].strip()
        or "dev"
    )
    return v


def get_git_context() -> dict[str, str]:
    """Gibt Git-Kontext-Felder als Dict zurück (alle optional)."""
    return {
        "git_commit_sha": os.environ.get(_GITHUB_SHA_ENV, ""),
        "git_branch":     os.environ.get(_GITHUB_REF_NAME_ENV, ""),
    }


def generate_pipeline_run_id() -> str:
    """Erzeugt eine neue UUID4 als Pipeline-Run-ID (string)."""
    return str(uuid.uuid4())


def start_pipeline_run(
    job_name: str,
    *,
    run_id:         str  | None = None,
    system_version: str  | None = None,
    metadata:       dict | None = None,
) -> str:
    """
    Startet einen neuen Pipeline-Run-Eintrag in Supabase.

    Args:
        job_name:       Name des Jobs, z.B. "compute_predictions".
        run_id:         Optional vorgegebene UUID; wird generiert falls None.
        system_version: Optional; wird via get_system_version() ermittelt falls None.
        metadata:       Optional zusätzliche Felder als Dict (→ metadata_json).

    Returns:
        run_id (str) — immer, auch wenn der Supabase-Schreibvorgang fehlschlägt.
        Bei Fehler wird geloggt aber keine Exception geworfen.
    """
    if run_id is None:
        run_id = generate_pipeline_run_id()
    if system_version is None:
        system_version = get_system_version()

    git = get_git_context()

    row: dict[str, Any] = {
        "run_id":         run_id,
        "job_name":       job_name,
        "system_version": system_version,
        "git_commit_sha": git["git_commit_sha"],
        "status":         "running",
        "started_at":     _now_iso(),
    }
    if metadata:
        row["metadata_json"] = metadata

    _supabase_insert(_PIPELINE_RUNS_TABLE, row)
    log.info(
        "[runtime_metadata] Pipeline run started  job=%s  run_id=%s  version=%s",
        job_name, run_id, system_version,
    )
    return run_id


def finish_pipeline_run(
    run_id: str,
    *,
    status:   str  = "success",
    metadata: dict | None = None,
) -> None:
    """
    Schreibt Endzeit und Status eines Pipeline-Runs nach Supabase.

    Args:
        run_id:   Die run_id, die von start_pipeline_run() zurückgegeben wurde.
        status:   "success" | "failed" | "partial" | beliebiger String.
        metadata: Optional zusätzliche Felder für metadata_json (merged).

    Wirft keine Exceptions — Fehler werden nur geloggt.
    """
    if not run_id:
        log.warning("[runtime_metadata] finish_pipeline_run called with empty run_id — skipping.")
        return

    update: dict[str, Any] = {
        "status":      status,
        "finished_at": _now_iso(),
    }
    if metadata:
        update["metadata_json"] = metadata

    _supabase_patch(_PIPELINE_RUNS_TABLE, "run_id", run_id, update)
    log.info(
        "[runtime_metadata] Pipeline run finished  run_id=%s  status=%s",
        run_id, status,
    )


def register_system_release(notes: str = "") -> None:
    """
    Registriert die aktuelle Systemversion in system_releases.
    Idempotent: ON CONFLICT (system_version) DO NOTHING via Supabase.
    Kann beim Start eines Pipeline-Runs optional aufgerufen werden.
    """
    version = get_system_version()
    git     = get_git_context()
    row     = {
        "system_version": version,
        "git_commit_sha": git["git_commit_sha"],
        "git_branch":     git["git_branch"],
        "notes":          notes,
    }
    _supabase_insert(_SYSTEM_RELEASES_TABLE, row, on_conflict="system_version")
    log.debug("[runtime_metadata] system_release registered  version=%s", version)


# ─────────────────────────────────────────────────────────────
# INTERNAL HELPERS
# ─────────────────────────────────────────────────────────────

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _get_supabase_headers() -> dict[str, str] | None:
    """
    Gibt Supabase Auth-Header zurück, oder None wenn ENV fehlt.
    """
    url = os.environ.get(_SUPABASE_URL_ENV, "").strip()
    key = os.environ.get(_SUPABASE_KEY_ENV, "").strip()
    if not url or not key:
        return None
    return {
        "apikey":        key,
        "Authorization": f"Bearer {key}",
        "Content-Type":  "application/json",
        "Prefer":        "resolution=merge-duplicates,return=minimal",
    }


def _get_supabase_url() -> str:
    return os.environ.get(_SUPABASE_URL_ENV, "").strip()


def _supabase_insert(
    table:       str,
    row:         dict,
    on_conflict: str | None = None,
) -> None:
    """
    INSERT (upsert) eine Zeile in eine Supabase-Tabelle.
    Fehler werden geloggt, nie geworfen.
    """
    headers = _get_supabase_headers()
    if headers is None:
        log.debug("[runtime_metadata] Supabase ENV not set — skipping insert into %s.", table)
        return

    base_url = _get_supabase_url()
    url      = f"{base_url}/rest/v1/{table}"
    params   = {}
    if on_conflict:
        params["on_conflict"] = on_conflict

    try:
        resp = requests.post(
            url,
            headers=headers,
            params=params,
            data=json.dumps(row),
            timeout=_REQUEST_TIMEOUT_S,
        )
        if not resp.ok:
            log.warning(
                "[runtime_metadata] INSERT %s failed: %d %s — %s",
                table, resp.status_code, resp.reason, resp.text[:200],
            )
    except Exception as exc:  # noqa: BLE001
        log.warning("[runtime_metadata] INSERT %s network error: %s", table, exc)


def _supabase_patch(
    table:      str,
    pk_col:     str,
    pk_val:     str,
    update:     dict,
) -> None:
    """
    PATCH (UPDATE) einer Zeile über einen PK-Filter.
    Fehler werden geloggt, nie geworfen.
    """
    headers = _get_supabase_headers()
    if headers is None:
        log.debug("[runtime_metadata] Supabase ENV not set — skipping patch on %s.", table)
        return

    base_url = _get_supabase_url()
    url      = f"{base_url}/rest/v1/{table}"
    params   = {pk_col: f"eq.{pk_val}"}
    headers  = {**headers, "Prefer": "return=minimal"}

    try:
        resp = requests.patch(
            url,
            headers=headers,
            params=params,
            data=json.dumps(update),
            timeout=_REQUEST_TIMEOUT_S,
        )
        if not resp.ok:
            log.warning(
                "[runtime_metadata] PATCH %s[%s=%s] failed: %d %s — %s",
                table, pk_col, pk_val,
                resp.status_code, resp.reason, resp.text[:200],
            )
    except Exception as exc:  # noqa: BLE001
        log.warning("[runtime_metadata] PATCH %s network error: %s", table, exc)
