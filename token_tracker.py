"""
token_tracker.py
────────────────
Per-user daily token usage tracking backed by the existing SQLite database.

Public API
----------
estimate_tokens(text)           -> int   : char-based token estimate  (chars // 4)
get_tokens_used(user_id)        -> int   : tokens spent today by this user
add_tokens(user_id, count)      -> None  : record token spend for today
is_over_limit(user_id, limit)   -> bool  : True if user has hit/exceeded their limit
"""

import sqlite3
import os
from datetime import date

# Re-use the same database file as the rest of the bot
_DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "database.sqlite")


def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(_DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")  # safe for concurrent readers
    return conn


def _ensure_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS token_usage (
            user_id    TEXT    NOT NULL,
            date       TEXT    NOT NULL,   -- ISO format: YYYY-MM-DD
            tokens_used INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (user_id, date)
        )
        """
    )
    conn.commit()


# ── Public helpers ─────────────────────────────────────────────────────────────

def estimate_tokens(text: str) -> int:
    """Approximate token count using a chars-÷-4 heuristic (standard rule-of-thumb)."""
    return max(1, len(text) // 4)


def get_tokens_used(user_id: str) -> int:
    """Return how many tokens *user_id* has consumed today (0 if none)."""
    today = str(date.today())
    with _get_conn() as conn:
        _ensure_table(conn)
        row = conn.execute(
            "SELECT tokens_used FROM token_usage WHERE user_id = ? AND date = ?",
            (user_id, today),
        ).fetchone()
    return row[0] if row else 0


def add_tokens(user_id: str, count: int) -> None:
    """Add *count* tokens to *user_id*'s today counter (upsert)."""
    today = str(date.today())
    with _get_conn() as conn:
        _ensure_table(conn)
        conn.execute(
            """
            INSERT INTO token_usage (user_id, date, tokens_used)
            VALUES (?, ?, ?)
            ON CONFLICT(user_id, date)
            DO UPDATE SET tokens_used = tokens_used + excluded.tokens_used
            """,
            (user_id, today, count),
        )
        conn.commit()


def is_over_limit(user_id: str, limit: int) -> bool:
    """Return True if *user_id* has already met or exceeded *limit* tokens today."""
    return get_tokens_used(user_id) >= limit
