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

# ── Agent (lazy-loaded so Slack connects instantly) ───────────────────────────
_agent_lock = threading.Lock()
_agent_executor = None

# ── Daily Login Tracking ──────────────────────────────────────────────────────
# Maps user_id -> ISO date string (YYYY-MM-DD) of their last interaction
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
        # Open (or reuse) the DM channel with this user
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





def get_agent():
    """Return the SQL agent executor, initializing it on first call."""
    global _agent_executor
    if _agent_executor is None:
        with _agent_lock:
            if _agent_executor is None:  # double-checked locking
                print("⏳  Initializing SQL Agent (first query)...")
                from langgraph_agent import build_graph
                _agent_executor = build_graph()
                print("✅  SQL Agent ready.")
    return _agent_executor

# ── Helpers ───────────────────────────────────────────────────────────────────
def strip_mention(text: str) -> str:
    """Remove bot @mention from text."""
    return re.sub(r"<@[A-Z0-9]+>", "", text).strip()

def run_query(query: str, user_id: str, say, channel_id: str = None) -> None:
    """Run a user query through the agent and reply via say()."""
    if not query:
        say("Hi there! Ask me anything about the database. 😊")
        return


    say(f"🔍 Processing: _{query}_")

    try:
        agent = get_agent()
        # Only pass fields that should reset per-query.
        # Do NOT include 'history' or other persistent fields — MemorySaver
        # carries them forward via the thread checkpoint.
        state = {
            "query": query,
            "iteration": 0,
            "error": "",
        }
        
        final_result = None
        chart_path = None
        csv_content = None
        final_followups = []
        config = {"configurable": {"thread_id": user_id}}
        
        start_time = time.perf_counter()
        for event in agent.stream(state, config=config, stream_mode="updates"):
            for node_name, node_output in event.items():
                if node_name == "classify_intent_node":
                    print(f"[{node_name}] Classified Intent: {node_output.get('intent')}")
                elif node_name == "generate_sql_node":
                    print(f"[{node_name}] Generated SQL: {node_output.get('sql_query')}")
                elif node_name == "execute_sql_node":
                    if node_output.get("error"):
                        print(f"[{node_name}] Execution Failed: {node_output.get('error')}")
                    else:
                        print(f"[{node_name}] Execution Suceeded.")
                elif node_name == "generate_answer_node":
                    print(f"[{node_name}] Generated final answer.")
                    final_result = node_output.get("final_answer")
                    
                    # EARLY RESPONSE: Send the text answer (summary only) as soon as it's ready
                    import re as _re
                    answer_text = _re.sub(r"```[Cc][Ss][Vv]\r?\n.*?(?:\s*```|$)", "", str(final_result), flags=_re.DOTALL).strip()
                    say(f"*Answer:*\n{answer_text}")
                elif node_name == "generate_chart_node":
                    chart_path = node_output.get("chart_path")
                    csv_content = node_output.get("csv_content")
                    final_result = node_output.get("final_answer", final_result)
                    if chart_path:
                        print(f"[{node_name}] Chart created: {chart_path}")
                    else:
                        print(f"[{node_name}] No chart generated.")
                    if csv_content:
                        print(f"[{node_name}] CSV data captured for upload.")
                elif node_name == "generate_followup_node":
                    final_followups = node_output.get("followup_questions", [])
                    if final_followups:
                        print(f"[{node_name}] Follow-up questions generated.")

        # The primary answer text is already sent early in the stream loop above.

        upload_channel = channel_id or user_id

        # Upload CSV as a downloadable file if available
        if csv_content:
            try:
                from datetime import datetime as _dt
                csv_filename = f"results_{_dt.now().strftime('%Y%m%d_%H%M%S')}.csv"
                app.client.files_upload_v2(
                    channel=upload_channel,
                    content=csv_content,
                    filename=csv_filename,
                    title="📄 Query Results (CSV)"
                )
                print(f"[slack] CSV uploaded to channel {upload_channel}")
            except Exception as csv_err:
                print(f"[WARN] CSV upload failed: {csv_err}")

        # If a chart was generated, upload it to Slack as an inline image
        if chart_path:
            try:
                import os as _os
                chart_filename = _os.path.basename(chart_path)
                with open(chart_path, "rb") as f:
                    chart_bytes = f.read()
                app.client.files_upload_v2(
                    channel=upload_channel,
                    file=chart_bytes,
                    filename=chart_filename,
                    title="📊 Query Chart",
                    initial_comment="Here is the chart for your query:"
                )
                print(f"[slack] Chart uploaded to channel {upload_channel}")
            except Exception as upload_err:
                print(f"[WARN] Chart upload failed: {upload_err}")

        # Send follow-up questions last
        if final_followups:
            followups_text = "\n".join([f"• _{q}_" for q in final_followups])
            say(f"*Suggested Follow-ups:*\n{followups_text}")


        end_time = time.perf_counter()
        print(f"\n[INFO] Total time for query '{query}': {end_time - start_time:.2f} seconds")

    except Exception as e:
        print(f"[ERROR] {e}")
        say(f"⚠️ Something went wrong: `{str(e)}`")

# ── Event Handlers ────────────────────────────────────────────────────────────

@app.event("app_home_opened")
def handle_app_home_opened(event, client):
    """
    Fires when a user opens the bot in Slack.
    - Clears the App Home tab (replaces any cached content with a blank view).
    - On the first open of each day, sends a proactive dashboard DM.
    """
    user_id = event.get("user")
    if not user_id:
        return

    # Clear any previously published App Home content
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
    # Ignore bot messages and non-DMs
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
    print("🚀 Starting Slack Bot (Socket Mode)...")
    handler = SocketModeHandler(app, SLACK_APP_TOKEN)
    print("✅ Slack Bot is running! Send a DM or @mention the bot.")
    handler.start()
