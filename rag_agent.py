"""
rag_agent.py
------------
Pure RAG pipeline for the Slack bot.

Flow:
  1. build_vectorstore()   – load all DB rows → embed → FAISS (done once at startup)
  2. retrieve_context()    – similarity search for relevant rows
  3. generate_answer()     – LLM answers using retrieved rows + conversation history
  4. generate_chart()      – parse csv block from answer → matplotlib chart PNGs
  5. generate_followups()  – LLM suggests 3 follow-up questions
  6. run_rag_query()        – orchestrates the full pipeline per user query
"""

import os
import re
import csv as csv_module
import io
import json
from datetime import datetime
from typing import Optional

import matplotlib
matplotlib.use("Agg")  # non-interactive backend
import matplotlib.pyplot as plt

from dotenv import load_dotenv
from langchain_google_genai import ChatGoogleGenerativeAI, GoogleGenerativeAIEmbeddings
from langchain_community.vectorstores import FAISS
from langchain_core.documents import Document

load_dotenv()

# ── Constants ─────────────────────────────────────────────────────────────────
# Path to the Excel file used as the data source for embeddings
FAISS_INDEX_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "faiss_index")
CHARTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "charts")
HISTORY_LIMIT = 5        # last N turns kept in context
TOP_K_DOCS = 30          # rows retrieved from FAISS per query

# ── Lazy globals ──────────────────────────────────────────────────────────────
_vectorstore: Optional[FAISS] = None
_llm: Optional[ChatGoogleGenerativeAI] = None
_embeddings: Optional[GoogleGenerativeAIEmbeddings] = None


# ── Resource helpers ──────────────────────────────────────────────────────────

def get_llm() -> ChatGoogleGenerativeAI:
    global _llm
    if _llm is None:
        _llm = ChatGoogleGenerativeAI(
            model="gemini-2.5-flash-lite",
            google_api_key=os.getenv("GOOGLE_API_KEY"),
            temperature=0.1,
        )
    return _llm


def get_embeddings() -> GoogleGenerativeAIEmbeddings:
    global _embeddings
    if _embeddings is None:
        _embeddings = GoogleGenerativeAIEmbeddings(
            model="models/gemini-embedding-001",
            google_api_key=os.getenv("GOOGLE_API_KEY"),
        )
    return _embeddings


# ── Vectorstore ───────────────────────────────────────────────────────────────

def build_vectorstore() -> FAISS:
    """
    Load the pre-built FAISS index from disk. The index must be created
    beforehand by running:  python create_vectorstore.py
    """
    global _vectorstore
    if _vectorstore is not None:
        return _vectorstore

    if not os.path.isdir(FAISS_INDEX_PATH):
        raise FileNotFoundError(
            f"FAISS index not found at '{FAISS_INDEX_PATH}'.\n"
            "Please run:  python create_vectorstore.py"
        )

    print(f"⏳  Loading RAG vectorstore from disk: {FAISS_INDEX_PATH} …")
    _vectorstore = FAISS.load_local(
        FAISS_INDEX_PATH,
        get_embeddings(),
        allow_dangerous_deserialization=True,
    )
    print("✅  Vectorstore loaded from disk.")
    return _vectorstore


def retrieve_context(query: str, k: int = TOP_K_DOCS) -> str:
    """
    Similarity-search the FAISS vectorstore for the top-k rows relevant to
    `query`. Returns a single joined string ready to insert into an LLM prompt.
    """
    vs = build_vectorstore()
    results = vs.similarity_search(query, k=k)
    return "\n".join(doc.page_content for doc in results)


# ── History helpers ───────────────────────────────────────────────────────────

def _format_history(history: list[dict]) -> str:
    """Format the last HISTORY_LIMIT turns for LLM context."""
    if not history:
        return ""
    csv_pattern = re.compile(r"```csv\n.*?```", re.DOTALL)
    items = []
    for turn in history[-HISTORY_LIMIT:]:
        q = turn.get("query", "")
        a = csv_pattern.sub("[tabular data omitted]", str(turn.get("answer", ""))).strip()
        items.append(f"Q: {q}\nA: {a}")
    return "\n\n".join(items)


# ── Core pipeline steps ───────────────────────────────────────────────────────

def generate_answer(query: str, context: str, history: list[dict]) -> str:
    """
    Use the LLM to answer `query` given `context` (retrieved DB rows) and
    conversation `history`. Returns the raw answer string (may contain a
    ```csv block if there is tabular data).
    """
    history_str = _format_history(history)
    history_section = (
        f"Previous conversation:\n{history_str}\n\n" if history_str else ""
    )

    prompt = f"""You are a helpful data analyst assistant. Answer the user's question using ONLY the data provided below.

{history_section}Relevant data from the database:
{context}

User question: {query}

INSTRUCTIONS:
1. Answer based solely on the data provided above. Do not make up values.
2. If the answer involves multiple rows/columns of data, wrap it in a ```csv code fence with a header row.
   Example:
   ```csv
   Region,Revenue,RO
   North,500000,1200
   ```
3. If the answer is a single value or a short natural-language reply, output plain text only — no csv block.
4. Always provide a brief prose summary BEFORE any csv block (include row count and key insight).
5. You can respond to greetings and general questions naturally.
6. Return only text directly relevant to the question."""

    llm = get_llm()
    response = llm.invoke(prompt)
    return response.content.strip()


# ── CSV / Chart helpers (ported from langgraph_agent.py) ─────────────────────

def _parse_csv_block(text: str) -> Optional[str]:
    """Extract raw CSV from a ```csv … ``` fence."""
    match = re.compile(r"```[Cc][Ss][Vv]\r?\n(.*?)(?:\s*```|$)", re.DOTALL).search(text)
    return match.group(1).strip() if match else None


def _extract_csv_from_answer(final_answer: str) -> Optional[str]:
    """Try fenced block first, fall back to unfenced CSV detection."""
    fenced = _parse_csv_block(final_answer)
    if fenced:
        return fenced

    lines = final_answer.splitlines()
    best_run: list[str] = []
    csv_lines: list[str] = []
    ref_commas = None

    for line in lines:
        stripped = line.strip()
        comma_count = stripped.count(",")
        if not stripped or comma_count == 0:
            if len(csv_lines) > len(best_run):
                best_run = csv_lines
            csv_lines = []
            ref_commas = None
            continue
        if ref_commas is None:
            ref_commas = comma_count
            csv_lines = [stripped]
        elif comma_count == ref_commas:
            csv_lines.append(stripped)
        else:
            if len(csv_lines) > len(best_run):
                best_run = csv_lines
            csv_lines = [stripped]
            ref_commas = comma_count

    if len(csv_lines) > len(best_run):
        best_run = csv_lines

    if len(best_run) >= 2:
        print("[_extract_csv_from_answer] Using raw CSV fallback.")
        return "\n".join(best_run)
    return None


def generate_chart(final_answer: str) -> tuple[list[str], Optional[str]]:
    """
    Parse a CSV block from `final_answer`, ask the LLM to define charts, then
    render them as PNG files saved to the charts/ directory.

    Returns (chart_paths, csv_content).
    """
    csv_text = _extract_csv_from_answer(final_answer)
    if not csv_text:
        print("[generate_chart] Skipping: no CSV text extracted.")
        return [], None

    reader = csv_module.reader(io.StringIO(csv_text))
    rows = list(reader)
    if len(rows) < 3:
        print(f"[generate_chart] Skipping chart: not enough rows ({len(rows)}).")
        return [], csv_text

    # Ask LLM to define chart specs
    chart_prompt = f"""You are a data visualization assistant.

Below is CSV data:
{csv_text}

Task:
1. Identify the label column (usually the first) and all numeric columns.
2. For EACH numeric column define a chart.
3. Choose the BEST chart type ("bar", "line", or "pie") for each.
4. Extract the labels and numeric values for that chart.

Return ONLY a valid JSON list (up to 3 charts). No markdown, no extra text.
Example:
[
  {{"chart_type": "bar", "title": "Revenue by Region", "x_label": "Region", "y_label": "Revenue", "labels": ["A","B"], "values": [10,20]}}
]"""

    try:
        llm = get_llm()
        raw = llm.invoke(chart_prompt).content.strip()
        raw = re.sub(r"^```[a-z]*\n?", "", raw, flags=re.IGNORECASE)
        raw = re.sub(r"```$", "", raw).strip()
        charts_data = json.loads(raw)
        if not isinstance(charts_data, list):
            charts_data = [charts_data]
    except Exception as e:
        print(f"[generate_chart] Chart LLM call failed ({e}); skipping chart.")
        return [], csv_text

    os.makedirs(CHARTS_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    chart_paths: list[str] = []

    for i, cd in enumerate(charts_data[:3]):
        if not isinstance(cd, dict):
            continue

        chart_type = cd.get("chart_type", "bar")
        labels = cd.get("labels", [])
        values = cd.get("values", [])
        title = cd.get("title", f"Chart {i + 1}")
        x_label = cd.get("x_label", "")
        y_label = cd.get("y_label", "")

        if not labels or not values or len(labels) != len(values):
            print(f"[generate_chart] Skipping chart {i+1}: mismatched labels/values.")
            continue

        # Sort bar charts descending and limit to top 10
        if chart_type == "bar" or chart_type not in ("line", "pie"):
            paired = sorted(
                zip(values, labels),
                key=lambda x: float(x[0]) if x[0] is not None else 0,
                reverse=True,
            )
            values, labels = zip(*paired) if paired else ([], [])
            values, labels = list(values), list(labels)

        labels = labels[:10]
        values = values[:10]

        try:
            values = [float(v) for v in values]
        except (ValueError, TypeError) as e:
            print(f"[generate_chart] Skipping chart {i+1}: value conversion error: {e}")
            continue

        chart_path = os.path.join(CHARTS_DIR, f"chart_{timestamp}_{i}.png")
        fig, ax = plt.subplots(figsize=(10, 6))

        if chart_type == "pie":
            ax.pie(values, labels=labels, autopct="%1.1f%%", startangle=140)
            ax.set_title(title, fontsize=14, fontweight="bold")
        elif chart_type == "line":
            ax.plot(labels, values, marker="o", linewidth=2, color="steelblue")
            ax.set_xlabel(x_label)
            ax.set_ylabel(y_label)
            ax.set_title(title, fontsize=14, fontweight="bold")
            ax.tick_params(axis="x", rotation=45)
            ax.grid(axis="y", linestyle="--", alpha=0.7)
        else:  # bar (default)
            bars = ax.bar(labels, values, color="steelblue", edgecolor="white")
            ax.set_xlabel(x_label)
            ax.set_ylabel(y_label)
            ax.set_title(title, fontsize=14, fontweight="bold")
            ax.tick_params(axis="x", rotation=45)
            ax.grid(axis="y", linestyle="--", alpha=0.7)
            for bar, val in zip(bars, values):
                ax.text(
                    bar.get_x() + bar.get_width() / 2,
                    bar.get_height(),
                    f"{val:,.0f}",
                    ha="center", va="bottom", fontsize=9,
                )

        plt.tight_layout()
        plt.savefig(chart_path, dpi=150)
        plt.close(fig)
        chart_paths.append(chart_path)
        print(f"[generate_chart] Saved chart: {chart_path}")

    return chart_paths, csv_text


def generate_followups(query: str, answer: str) -> list[str]:
    """Ask the LLM for 3 relevant follow-up questions."""
    prompt = f"""You are a helpful data assistant. The user asked: "{query}"
We provided: {answer}

Generate 3 relevant follow-up questions the user might want to ask next.
Output ONLY a JSON list of 3 strings, no markdown, no extra text.
Example: ["What is the total revenue?", "Which region performed best?", "Show me the breakdown by product."]"""

    llm = get_llm()
    raw = llm.invoke(prompt).content.strip()
    raw = re.sub(r"^```[a-z]*\n?", "", raw, flags=re.IGNORECASE)
    raw = re.sub(r"```$", "", raw).strip()

    try:
        followups = json.loads(raw)
        if not isinstance(followups, list):
            followups = []
    except json.JSONDecodeError:
        followups = []

    return followups[:3]


# ── Main entry point ──────────────────────────────────────────────────────────

def run_rag_query(
    query: str,
    user_id: str,
    history_store: dict[str, list],
) -> tuple[str, list[str], Optional[str], list[str]]:
    """
    Orchestrate the full RAG pipeline for a single user query.

    Args:
        query:         The user's natural-language question.
        user_id:       Slack user ID used as conversation key.
        history_store: Shared dict mapping user_id → list of turn dicts.

    Returns:
        (final_answer, chart_paths, csv_content, followup_questions)
    """
    history = history_store.get(user_id, [])

    # 1. Retrieve relevant rows
    print(f"[rag] Retrieving context for: {query!r}")
    context = retrieve_context(query)

    # 2. Generate answer
    print("[rag] Generating answer…")
    final_answer = generate_answer(query, context, history)
    print("[rag] Answer generated.")

    # 3. Generate chart + extract CSV
    chart_paths, csv_content = generate_chart(final_answer)

    # 4. Generate follow-ups
    followups = generate_followups(query, final_answer)

    # 5. Update conversation history
    history_store.setdefault(user_id, []).append(
        {"query": query, "answer": final_answer}
    )

    return final_answer, chart_paths, csv_content, followups
