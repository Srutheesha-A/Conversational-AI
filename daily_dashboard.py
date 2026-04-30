"""
daily_dashboard.py
------------------
Fetches KPI metrics from the SQLite database and builds a Slack Block Kit
payload to display as a daily first-login dashboard.
"""
from __future__ import annotations

import os
import sqlite3
from datetime import datetime

from typing import Optional

# Resolve the DB path relative to this file's location
_DB_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "database.sqlite")


def _get_conn():
    return sqlite3.connect(_DB_FILE)


# ── KPI Fetchers ──────────────────────────────────────────────────────────────

def _fetch_table_summary() -> list[dict]:
    """Return a list of {name, row_count} for every user table in the DB."""
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = [row[0] for row in cur.fetchall()]
    summary = []
    for tname in tables:
        cur.execute(f'SELECT COUNT(*) FROM "{tname}"')
        cnt = cur.fetchone()[0]
        summary.append({"name": tname, "row_count": cnt})
    conn.close()
    return summary


def _fetch_inventory_kpis() -> Optional[dict]:
    """Return inventory KPIs or None if the table doesn't exist."""
    conn = _get_conn()
    cur = conn.cursor()
    try:
        cur.execute("SELECT COUNT(DISTINCT store_id) FROM inventory_data")
        stores = cur.fetchone()[0]
        cur.execute("SELECT COUNT(DISTINCT product_id) FROM inventory_data")
        products = cur.fetchone()[0]
        cur.execute("SELECT SUM(CAST(units_sold AS REAL)) FROM inventory_data")
        total_sold = cur.fetchone()[0] or 0
        cur.execute(
            "SELECT SUM(CAST(units_sold AS REAL) * CAST(price AS REAL)) FROM inventory_data"
        )
        total_rev = cur.fetchone()[0] or 0
        return {
            "stores": stores,
            "products": products,
            "total_sold": total_sold,
            "total_rev": total_rev,
        }
    except Exception:
        return None
    finally:
        conn.close()


def _fetch_supply_chain_kpis() -> Optional[dict]:
    """Return supply chain KPIs or None if the table doesn't exist."""
    conn = _get_conn()
    cur = conn.cursor()
    try:
        cur.execute("SELECT COUNT(*) FROM supply_chain_logistics")
        total_shipments = cur.fetchone()[0]
        cur.execute(
            "SELECT COUNT(*) FROM supply_chain_logistics WHERE delivery_status='delayed'"
        )
        delayed = cur.fetchone()[0]
        cur.execute(
            "SELECT COUNT(*) FROM supply_chain_logistics WHERE delivery_status='delivered'"
        )
        delivered = cur.fetchone()[0]
        cur.execute(
            "SELECT SUM(CAST(shipping_cost_ngn AS REAL)) FROM supply_chain_logistics"
        )
        total_cost = cur.fetchone()[0] or 0
        return {
            "total_shipments": total_shipments,
            "delivered": delivered,
            "delayed": delayed,
            "total_cost": total_cost,
        }
    except Exception:
        return None
    finally:
        conn.close()


def _fetch_pact_kpis() -> Optional[dict]:
    """Return PACT KPIs or None if the table doesn't exist."""
    conn = _get_conn()
    cur = conn.cursor()
    try:
        cur.execute("SELECT COUNT(DISTINCT PART_NUMBER) FROM supply_chain_data")
        total_parts = cur.fetchone()[0]
        cur.execute("SELECT SUM(CAST(PART_BO_QUANTITY AS REAL)) FROM supply_chain_data")
        total_bo = cur.fetchone()[0] or 0
        cur.execute("SELECT SUM(CAST(PART_SPAC_QUANTITY AS REAL)) FROM supply_chain_data")
        total_spac = cur.fetchone()[0] or 0
        cur.execute("SELECT SUM(CAST(PART_PAST_DUE_ORDERS_QUANTITY AS REAL)) FROM supply_chain_data")
        total_past_due = cur.fetchone()[0] or 0
        cur.execute("SELECT SUM(CAST(PART_LEAD_TIME AS REAL)), SUM(CAST(PART_DELAY_DAYS AS REAL)) FROM supply_chain_data")
        total_lead, total_delay = cur.fetchone()
        total_lead = total_lead or 0
        total_delay = total_delay or 0
        total_time = total_lead + total_delay
        lead_time_pct = (total_lead / total_time * 100) if total_time > 0 else 0
        delay_time_pct = (total_delay / total_time * 100) if total_time > 0 else 0
        cur.execute("SELECT SUM(CAST(PART_CURRENT_DUE_ORDERS AS REAL)) FROM supply_chain_data")
        total_current_due = cur.fetchone()[0] or 0
        cur.execute("SELECT COUNT(DISTINCT PART_SUPPLIER_NAME_ID) FROM supply_chain_data")
        total_suppliers = cur.fetchone()[0]
        return {
            "total_parts": total_parts,
            "total_bo": total_bo,
            "total_spac": total_spac,
            "total_past_due": total_past_due,
            "lead_time_pct": lead_time_pct,
            "delay_time_pct": delay_time_pct,
            "total_current_due": total_current_due,
            "total_suppliers": total_suppliers,
        }
    except Exception:
        return None
    finally:
        conn.close()


# ── Block Kit Builder ─────────────────────────────────────────────────────────

def build_dashboard_blocks() -> list[dict]:
    """
    Build and return a Slack Block Kit block list for the daily KPI dashboard.
    """
    today_str = datetime.now().strftime("%A, %B %d %Y")
    blocks: list[dict] = []

    # ── Header ────────────────────────────────────────────────────────────────
    blocks.append({"type": "header", "text": {"type": "plain_text", "text": f"📊  Daily Dashboard  —  {today_str}", "emoji": True}})
    blocks.append({"type": "divider"})

    # ── Table Summary ─────────────────────────────────────────────────────────
    table_summary = _fetch_table_summary()
    if table_summary:
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "*🗄️  Database Tables*"
            }
        })
        # Build a compact text listing: one accessory field per table (max 10 shown)
        shown = list(table_summary)[:10]
        field_texts = [
            f"*{t['name']}*\n{t['row_count']:,} rows" for t in shown
        ]
        # Slack fields max 10 items and display in 2 columns
        field_blocks: list[dict] = [{"type": "mrkdwn", "text": ft} for ft in field_texts]
        blocks.append({
            "type": "section",
            "fields": field_blocks
        })
        if len(table_summary) > 10:
            blocks.append({
                "type": "context",
                "elements": [{"type": "mrkdwn", "text": f"_…and {len(table_summary) - 10} more tables_"}]
            })

    blocks.append({"type": "divider"})

    # ── Inventory KPIs ────────────────────────────────────────────────────────
    inv = _fetch_inventory_kpis()
    if inv:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "*🏪  Inventory KPIs*"}
        })
        blocks.append({
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"🏬 *Stores*\n{inv['stores']:,}"},
                {"type": "mrkdwn", "text": f"📦 *Products*\n{inv['products']:,}"},
                {"type": "mrkdwn", "text": f"🛒 *Units Sold*\n{inv['total_sold']:,.0f}"},
                {"type": "mrkdwn", "text": f"💰 *Total Revenue*\n₦{inv['total_rev']:,.2f}"},
            ]
        })
        blocks.append({"type": "divider"})

    # ── Supply Chain KPIs ─────────────────────────────────────────────────────
    sc = _fetch_supply_chain_kpis()
    if sc:
        on_time_pct = (
            (sc["delivered"] / sc["total_shipments"] * 100)
            if sc["total_shipments"] > 0
            else 0.0
        )
        delayed_pct = (
            (sc["delayed"] / sc["total_shipments"] * 100)
            if sc["total_shipments"] > 0
            else 0.0
        )
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "*🚚  Supply Chain KPIs*"}
        })
        blocks.append({
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"📋 *Total Shipments*\n{sc['total_shipments']:,}"},
                {"type": "mrkdwn", "text": f"✅ *Delivered*\n{sc['delivered']:,} ({on_time_pct:.1f}%)"},
                {"type": "mrkdwn", "text": f"⚠️ *Delayed*\n{sc['delayed']:,} ({delayed_pct:.1f}%)"},
                {"type": "mrkdwn", "text": f"💸 *Shipping Cost*\n₦{sc['total_cost']:,.2f}"},
            ]
        })
        blocks.append({"type": "divider"})

    # ── PACT KPIs ─────────────────────────────────────────────────────────────
    pact = _fetch_pact_kpis()
    if pact:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "*⚙️  PACT KPIs*"}
        })
        blocks.append({
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"🔩 *Total Parts*\n{pact['total_parts']:,}"},
                {"type": "mrkdwn", "text": f"📉 *Backorders (BO)*\n{pact['total_bo']:,.0f}"},
                {"type": "mrkdwn", "text": f"📦 *SPAC Quantity*\n{pact['total_spac']:,.0f}"},
                {"type": "mrkdwn", "text": f"⚠️ *Past Due Orders*\n{pact['total_past_due']:,.0f}"},
                {"type": "mrkdwn", "text": f"📝 *Current Due Orders*\n{pact['total_current_due']:,.0f}"},
                {"type": "mrkdwn", "text": f"⏳ *Lead Time %*\n{pact['lead_time_pct']:.1f}%"},
                {"type": "mrkdwn", "text": f"⏱️ *Delay Time %*\n{pact['delay_time_pct']:.1f}%"},
                {"type": "mrkdwn", "text": f"🏢 *Total Suppliers*\n{pact['total_suppliers']:,}"},
            ]
        })
        blocks.append({"type": "divider"})

    # ── Footer ────────────────────────────────────────────────────────────────
    blocks.append({
        "type": "context",
        "elements": [
            {
                "type": "mrkdwn",
                "text": "💡 _Ask me anything about the data — queries, trends, charts, and more!_"
            }
        ]
    })

    return blocks
