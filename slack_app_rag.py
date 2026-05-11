import os
import re
import threading
import time
from datetime import date
from dotenv import load_dotenv
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

# ── Environment ──────────────────────────────────────────────────────────────
load_dotenv()

SLACK_BOT_TOKEN = os.environ.get("SLACK_BOT_TOKEN")
SLACK_APP_TOKEN = os.environ.get("SLACK_APP_TOKEN")
SLACK_BOT_USER_ID = os.environ.get("SLACK_BOT_USER_ID", "")

if not SLACK_BOT_TOKEN:
    raise RuntimeError("SLACK_BOT_TOKEN is missing from .env")
if not SLACK_APP_TOKEN:
    raise RuntimeError("SLACK_APP_TOKEN is missing from .env")

# ── Slack App ─────────────────────────────────────────────────────────────────
app = App(token=SLACK_BOT_TOKEN)

# ── RAG (lazy-loaded on first query) ─────────────────────────────────────────
_rag_lock = threading.Lock()
_rag_ready = False

# Per-user conversation history: user_id → list of {query, answer} dicts
_history_store: dict[str, list] = {}

# ── Daily Login Tracking ──────────────────────────────────────────────────────
_user_last_seen: dict[str, str] = {}


def is_first_login_today(user_id: str) -> bool:
    """Return True (and update the tracker) if this is the user's first message today."""
    today = str(date.today())
    if _user_last_seen.get(user_id) != today:
        _user_last_seen[user_id] = today
        return True
    return False


def send_dashboard_dm(client, user_id: str) -> None:
    """Open a DM with the user and post the daily KPI dashboard."""
    try:
        from daily_dashboard import build_dashboard_blocks
        blocks = build_dashboard_blocks()
        result = client.conversations_open(users=user_id)
        dm_channel = result["channel"]["id"]
        client.chat_postMessage(
            channel=dm_channel,
            blocks=blocks,
            text="📊 Good morning! Here's your daily database dashboard."
        )
        print(f"[daily_dashboard] Dashboard DM sent to {user_id}.")
    except Exception as e:
        print(f"[WARN] Failed to send daily dashboard DM: {e}")


def ensure_rag_ready() -> None:
    """Trigger the vectorstore build on first use (thread-safe)."""
    global _rag_ready
    if not _rag_ready:
        with _rag_lock:
            if not _rag_ready:
                print("⏳  Initializing RAG vectorstore (first query)…")
                from rag_agent import build_vectorstore
                build_vectorstore()
                _rag_ready = True
                print("✅  RAG vectorstore ready.")


# ── Helpers ───────────────────────────────────────────────────────────────────

def strip_mention(text: str) -> str:
    """Remove bot @mention from the text."""
    return re.sub(r"<@[A-Z0-9]+>", "", text).strip()


def run_query(query: str, user_id: str, say, channel_id: str = None) -> None:
    """Run a user query through the RAG pipeline and reply via say()."""
    if not query:
        say("Hi there! Ask me anything about the database. 😊")
        return

    say(f"🔍 Processing: _{query}_")

    try:
        ensure_rag_ready()

        from rag_agent import run_rag_query

        start_time = time.perf_counter()

        final_answer, chart_paths, csv_content, followups = run_rag_query(
            query=query,
            user_id=user_id,
            history_store=_history_store,
        )

        # ── Send text answer (strip csv block for cleaner display) ────────────
        answer_text = re.sub(
            r"```[Cc][Ss][Vv]\r?\n.*?(?:\s*```|$)", "", str(final_answer), flags=re.DOTALL
        ).strip()
        say(f"*Answer:*\n{answer_text}")

        upload_channel = channel_id or user_id

        # ── Upload CSV file ───────────────────────────────────────────────────
        if csv_content:
            try:
                from datetime import datetime as _dt
                csv_filename = f"results_{_dt.now().strftime('%Y%m%d_%H%M%S')}.csv"
                app.client.files_upload_v2(
                    channel=upload_channel,
                    content=csv_content,
                    filename=csv_filename,
                    title="📄 Query Results (CSV)",
                )
                print(f"[slack] CSV uploaded to channel {upload_channel}")
            except Exception as csv_err:
                print(f"[WARN] CSV upload failed: {csv_err}")

        # ── Send follow-up suggestions ────────────────────────────────────────
        if followups:
            time.sleep(1.0)
            followups_text = "\n".join([f"• _{q}_" for q in followups])
            say(f"*Suggested Follow-ups:*\n{followups_text}")

        # ── Upload chart images ───────────────────────────────────────────────
        if chart_paths:
            for idx, chart_path in enumerate(chart_paths):
                try:
                    chart_filename = os.path.basename(chart_path)
                    with open(chart_path, "rb") as f:
                        chart_bytes = f.read()
                    comment = "Here is a chart for your query:" if idx == 0 else "And another related chart:"
                    app.client.files_upload_v2(
                        channel=upload_channel,
                        file=chart_bytes,
                        filename=chart_filename,
                        title=f"📊 Query Chart {idx + 1}",
                        initial_comment=comment,
                    )
                    print(f"[slack] Chart uploaded to channel {upload_channel}")
                except Exception as upload_err:
                    print(f"[WARN] Chart upload failed: {upload_err}")

        end_time = time.perf_counter()
        print(f"\n[INFO] Total time for query '{query}': {end_time - start_time:.2f}s")

    except Exception as e:
        print(f"[ERROR] {e}")
        say(f"⚠️ Something went wrong: `{str(e)}`")


# ── Event Handlers ────────────────────────────────────────────────────────────

@app.event("app_home_opened")
def handle_app_home_opened(event, client):
    """
    Fires when a user opens the bot in Slack.
    Clears the App Home tab and sends a proactive dashboard DM on first daily visit.
    """
    user_id = event.get("user")
    if not user_id:
        return

    try:
        client.views_publish(
            user_id=user_id,
            view={
                "type": "home",
                "blocks": [
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": "👋 *Send me a message to get started!*\nAsk me anything about the database."
                        }
                    }
                ]
            }
        )
    except Exception as e:
        print(f"[WARN] Failed to clear App Home view: {e}")

    if is_first_login_today(user_id):
        send_dashboard_dm(client, user_id)


@app.event("app_mention")
def handle_mention(event, say):
    """Respond when the bot is @mentioned in any channel."""
    user_id = event.get("user")
    channel_id = event.get("channel")
    query = strip_mention(event.get("text", ""))
    run_query(query, user_id, say, channel_id=channel_id)


@app.event("message")
def handle_dm(event, say, client):
    """Respond to direct messages (channel_type = 'im')."""
    if event.get("bot_id") or event.get("subtype"):
        return
    if event.get("channel_type") != "im":
        return

    user_id = event.get("user")
    channel_id = event.get("channel")
    query = event.get("text", "").strip()
    run_query(query, user_id, say, channel_id=channel_id)


# ── Entry Point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("🚀 Starting Slack Bot (Socket Mode)…")
    handler = SocketModeHandler(app, SLACK_APP_TOKEN)
    print("✅ Slack Bot is running! Send a DM or @mention the bot.")
    handler.start()