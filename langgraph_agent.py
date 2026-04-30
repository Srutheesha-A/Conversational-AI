import os
import re
import json
import operator
import time
from datetime import datetime
from typing import TypedDict, Annotated, Optional
from langgraph.checkpoint.memory import MemorySaver
from dotenv import load_dotenv
from langgraph.graph import StateGraph, START, END
from langchain_community.utilities.sql_database import SQLDatabase
from langchain_google_genai import ChatGoogleGenerativeAI
import matplotlib
matplotlib.use("Agg")  # non-interactive backend
import matplotlib.pyplot as plt

load_dotenv()

DB_PATH = "sqlite:///database.sqlite"

class AgentState(TypedDict, total=False):
    query: str
    sql_query: str
    sql_result: str
    error: str
    iteration: int
    final_answer: str
    intent: str
    history: Annotated[list[dict], operator.add]
    chart_path: Optional[str]
    followup_questions: list[str]
    csv_content: Optional[str]

# ── Global Resources ──────────────────────────────────────────────────────────
_db = None
_llm = None
_schema_cache = None

def get_db():
    global _db
    if _db is None:
        _db = SQLDatabase.from_uri(DB_PATH)
    return _db

def get_llm():
    global _llm
    if _llm is None:
        _llm = ChatGoogleGenerativeAI(
            model="gemini-2.5-flash-lite",
            google_api_key=os.getenv("GOOGLE_API_KEY"),
            temperature=0.1
        )
    return _llm

def get_schema():
    global _schema_cache
    if _schema_cache is None:
        _schema_cache = get_db().get_table_info()
    return _schema_cache

def format_history(history: list[dict], limit: int = 5) -> str:
    """
    Format history for the LLM context.
    - Only includes the last `limit` turns.
    - Strips large CSV blocks from answers to save tokens.
    """
    if not history:
        return ""
    
    recent_history = history[-limit:]
    formatted_items = []
    
    csv_pattern = re.compile(r"```csv\n.*?```", re.DOTALL)
    
    for item in recent_history:
        query = item.get("query", "")
        answer = str(item.get("answer", ""))
        
        # Strip CSV block if present to reduce context size
        clean_answer = csv_pattern.sub("[Large CSV data table]", answer).strip()
        
        formatted_items.append(f"Q: {query}\nA: {clean_answer}")
        
    return "\n\n".join(formatted_items)

def classify_intent_node(state: AgentState):
    query = state.get("query", "")
    history = state.get("history", [])
    
    if not history:
        # If no history, it's definitely a new query
        return {"intent": "new"}
        
    history_str = format_history(history)
    
    prompt = f"""You are an intent classification assistant. Evaluate the user's latest query to determine if it is a "new" independent question or a "followup" to the previous conversation.

Previous Conversation History:
{history_str}

User's Latest Query: {query}

Is the query a "new" question (needs a fresh database query independent of context) or a "followup" (refers to or builds upon the previous context, e.g., using pronouns like "them", "these", "those")?
Output ONLY the word "new" or "followup"."""

    llm = get_llm()
    
    response = llm.invoke(prompt)
    intent = response.content.strip().lower()
    if "followup" in intent:
        intent = "followup"
    else:
        intent = "new"
        
    return {"intent": intent}

def generate_sql_node(state: AgentState):
    schema = get_schema()
    
    query = state.get("query", "")
    error = state.get("error", "")
    iteration = state.get("iteration", 0)
    sql_query_prev = state.get("sql_query", "")
    intent = state.get("intent", "new")
    history = state.get("history", [])

    # Recover the last successful SQL query from history on a followup turn
    if not sql_query_prev and not error and history:
        last_turn = history[-1]
        if isinstance(last_turn, dict):
            sql_query_prev = last_turn.get("sql_query", "")
    
    prompt = f"You are an expert SQL developer. Your task is to accurately convert the user's natural language question into a SQLite SQL query based ONLY on the provided schema.\n\n"
    
    if intent == "followup" and history:
        history_str = format_history(history)
        prompt += f"Context from previous conversation:\n{history_str}\n\n"
        
        if sql_query_prev and not error:
            prompt += f"""IMPORTANT: The user is asking a follow-up question. Instead of querying the full table again, build your new SQL query ON TOP of the previous SQL result below (e.g. use it as a subquery or CTE):

Previous SQL:
{sql_query_prev}

Your new SQL should filter/extend the above result to answer the follow-up question.

"""

    prompt += f"""Schema:
{schema}

User query: {query}
Instructions:
1. ##CRITICAL: The descriptions of each columns in the table is given below.
    ** PART_NUMBER- Unique Part Number
    ** PART_MONTHLY_FORECAST- Parts demand forecast
    ** PART_MONTHLY_SALES_DEMAND- Actuals sales of part
    ** PART_FORECAST_VS_VARIANCE_PERCENTAGE-	Variance between actual sales & demand forecasted for each part
    ** PART_LEAD_TIME-	total lead time from manufacturing to warehouse
    ** PART_BO_QUANTITY-	Backorders quantity required for this part
    ** PART_WEEK_OVER_WEEKCHANGE_BO_PERCENTAGE-	Week over week change in the required backorders in percentage terms
    ** PART_AVERAGE_BO_AGE-	Average Backorder age
    ** PART_OLDEST_BO_DATE-	oldest backorder date for this part
    ** PART_INTERNATIONAL_IN_TRANSIT_QUANTITY-	In-transit quantity of this part which is in international transit mode
    ** PART_DOMESTIC_IN_TRANSIT_QUANTITY-	In-transit quantity of this part which is in domestic transit mode
    ** PART_DWELL_TIME-	from how many days this part is dwelling in the in-transit mode
    ** PART_DELAY_DAYS-	how many days this part is delayed from actual delivery date
    ** PART_SPAC_QUANTITY-	special urgent Backorders category called as SPAC, SPAC quantity required for this part
    ** PART_WEEK_OVER_WEEKCHANGE_SPAC_PERCENTAGE-	special urgent Backorders category called as SPAC, Week over week change in the required SPAC in percentage terms
    ** PART_AVERAGE_SPAC_AGE-	Average SPAC  age
    ** PART_OLDEST_SPAC_DATE-	oldest SPAC date for this part
    ** PART_PCN_ISSUE-	this parts new version part's issue date
    ** PART_PCN_MAIL-	this parts new version part's mail date
    ** PART_AUTO_COMMIT_STATUS-	this parts new version part's auto commit  status

    ** PART_SUPERSESSION_CHAIN-	this parts older and current version, entire supersession chain
    ** PART_PAST_DUE_ORDERS_QUANTITY-	Parts Past due orders qnty which is past the due date of delivery/shipment from supplier
    ** PART_AVERAGE_PAST_DUE_AGING-	past due date average aging
    ** PART_OLDEST_PAST_DUE_DATE-	oldest past due date
    ** PART_CURRENT_DUE_ORDERS-	which is due for delivery/shipment from supplier for this week
    ** PART_SPAC_COVERAGE_QUANTITY-	the quantity which can be used to fulfil the required SPAC

    ** PART_SPAC_COVERAGE_PERCENTAGE-	the quantity which can be used to fulfil the required SPAC in terms of percentage

    ** PART_BO_COVERAGE_QUANTITY-	the quantity which can be used to fulfil the required BO

    ** PART_BO_COVERAGE_PERCENTAGE-	the quantity which can be used to fulfil the required BO in terms of percentage

    ** PART_OPEN_ASN_QUANTITY-	The ASN open for this part
    ** PART_OLDEST_ASN_DATE-	oldest ASN date for this part
    ** PART_WRITE_OFF_QUANTITY-	quantity which has written-off
    ** PART_PLUS_UP_QUANTITY-	quantity which has plus-ups
    ** PART_DECK_NUMBER-	code of part owner who is responsible for this parts management
    ** PART_CPC_LEVEL-	category/type of this part
    ** PART_SUPPLIER_NAME_ID-	supplier of this part
    ** PART_TOP_50-	is this part a member of top SPAC tracker report
 2. Use them to understand the data and write an accurate SQL query.
 """

    if error:
        prompt += f"\nWarning: The previous generated SQL gave the following error:\n{error}\nPrevious SQL:\n{sql_query_prev}\n\nPlease fix the SQL query.\n"
        
    prompt += "\nOutput ONLY the SQL query, without any markdown formatting or explanation, e.g., 'SELECT * FROM users;'."
    
    llm = get_llm()
    
    response = llm.invoke(prompt)
    
    sql_query = response.content.strip()
    if sql_query.startswith("```sql"):
        sql_query = sql_query[6:]
    if sql_query.endswith("```"):
        sql_query = sql_query[:-3]
    
    # Some extra cleanup just in case
    sql_query = sql_query.strip("` \n")
    
    return {"sql_query": sql_query, "iteration": iteration + 1}

def execute_sql_node(state: AgentState):
    db = get_db()
    sql_query = state.get("sql_query", "")
    
    try:
        # Run query
        result = db.run(sql_query)
        return {"sql_result": str(result), "error": ""}
    except Exception as e:
        return {"error": str(e)}

def generate_answer_node(state: AgentState):
    query = state.get("query", "")
    sql_result = state.get("sql_result", "")
    sql_query = state.get("sql_query", "")
    
    schema = get_schema()
    
    prompt = f"""You are a data analyst. The user asked the following question: "{query}"

We successfully executed the following SQL query:
```sql
{sql_query}
```

Against this database schema:
{schema}

And got the following raw results from the database:
{sql_result}

INSTRUCTIONS:
1. ##CRITICAL: If there is more than one row or column of data in the sql result, you MUST wrap the tabular data in a ```csv code fence.
   CORRECT (always do this):
   ```csv
   Column1,Column2,Column3
   val1,val2,val3
   ```
   WRONG (never output raw CSV without fences):
   Column1,Column2,Column3
   val1,val2,val3
   Do NOT use markdown tables. Do NOT output raw CSV without the opening ```csv and closing ``` fences.
2. ##IMPORTANT: Do not create csv if there is only one column and one row. In that case just provide a plain text answer.
3. If the query doesn't need tabular data as the answer, just provide a plain text answer.
   Example:
   Query: which supplier has most of the delayed shipments?
   Answer: Supplier A has the most delayed shipments.
4. ##CRITICAL: Provide a brief summary of the results (including row count) BEFORE the csv block. Summary should contain some details of the data in the csv.
6. You should be able to answer general greetings.
7. ##CRITICAL: Return only text relevant to the query. Do not add any extra text or explanation."""
    
    llm = get_llm()
    
    response = llm.invoke(prompt)
    answer = response.content.strip()
    
    new_history_item = {"query": query, "answer": answer, "sql_query": sql_query}
    return {"final_answer": answer, "history": [new_history_item]}

def _parse_csv_block(final_answer: str):
    """Extract raw CSV text from a ```csv ... ``` fence in the answer.

    Handles common LLM output variations:
    - Case-insensitive fence name (```CSV, ```Csv, etc.)
    - Windows-style line endings (\\r\\n)
    - Trailing whitespace/newlines before closing fence
    - Missing closing fence (LLM truncation)
    """
    # Closing ``` is optional — if absent, capture to end of string
    csv_pattern = re.compile(r"```[Cc][Ss][Vv]\r?\n(.*?)(?:\s*```|$)", re.DOTALL)
    match = csv_pattern.search(final_answer)
    if not match:
        return None
    return match.group(1).strip()


def _extract_csv_from_answer(final_answer: str):
    """Try to extract CSV content from the answer.

    First attempts to find a fenced ```csv block.  If that fails, falls back to
    detecting raw (unfenced) CSV: looks for 2+ consecutive lines that each have
    the same number of commas (≥1) — a strong signal the LLM forgot the fence.
    Returns the raw CSV string, or None if no CSV-like content is detected.
    """
    # Primary: fenced block
    fenced = _parse_csv_block(final_answer)
    if fenced:
        return fenced

    # Fallback: unfenced CSV detection
    lines = final_answer.splitlines()
    csv_lines = []
    best_run = []
    ref_commas = None

    for line in lines:
        stripped = line.strip()
        comma_count = stripped.count(",")
        # Skip empty lines or lines with no commas
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

    # Need at least a header + 1 data row to be worth uploading
    if len(best_run) >= 2:
        print("[_extract_csv_from_answer] No fenced block found; using raw CSV fallback.")
        return "\n".join(best_run)

    return None


def generate_chart_node(state: AgentState):
    """Detect CSV block in the final answer and generate a chart PNG."""
    import csv as csv_module
    import io

    final_answer = str(state.get("final_answer", ""))

    csv_text = _extract_csv_from_answer(final_answer)
    if not csv_text:
        return {"chart_path": None, "csv_content": None}

    # Parse CSV to get rows
    reader = csv_module.reader(io.StringIO(csv_text))
    rows = list(reader)
    if len(rows) < 3:  # need at least header + 2 data rows to bother charting
        return {"chart_path": None, "csv_content": csv_text}

    prompt = f"""You are a data visualization assistant.

Below is CSV data extracted from a query result:

{csv_text}

Your task:
1. Decide the BEST chart type to visualise this data: choose one of ["bar", "line", "pie"].
2. Extract the labels (first column values, excluding the header row) and numeric values (last numeric column, excluding the header row).
3. Produce a short, descriptive chart title.

Return ONLY a valid JSON object with no extra text, in exactly this format:
Example: {{"chart_type": "...", "title": "...", "x_label": "...", "y_label": "...", "labels": [...], "values": [...]}}

Rules:
- "values" must be a list of numbers (floats or ints).
- Do NOT include markdown or code fences.
"""

    try:
        llm = get_llm()
        response = llm.invoke(prompt)
        raw = response.content.strip()

        # Strip any accidental code fences
        raw = re.sub(r"^```[a-z]*\n?", "", raw)
        raw = re.sub(r"```$", "", raw).strip()

        chart_data = json.loads(raw)
    except Exception as chart_llm_err:
        print(f"[generate_chart_node] Chart LLM call failed ({chart_llm_err}); skipping chart but CSV will still be uploaded.")
        return {"chart_path": None, "csv_content": csv_text}

    chart_type = chart_data.get("chart_type")
    labels = chart_data.get("labels", [])
    values = chart_data.get("values", [])
    title = chart_data.get("title", "Chart")
    x_label = chart_data.get("x_label", "")
    y_label = chart_data.get("y_label", "")

    if not labels or not values or len(labels) != len(values):
        return {"chart_path": None, "csv_content": csv_text}

    try:
        values = [float(v) for v in values]
    except (ValueError, TypeError):
        return {"chart_path": None, "csv_content": csv_text}

    charts_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "charts")
    os.makedirs(charts_dir, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    chart_filename = f"chart_{timestamp}.png"
    chart_path = os.path.join(charts_dir, chart_filename)

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
    else:  # default: bar
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
                ha="center", va="bottom", fontsize=9
            )

    plt.tight_layout()
    plt.savefig(chart_path, dpi=150)
    plt.close(fig)

    return {"final_answer": str(final_answer), "chart_path": chart_path, "csv_content": csv_text}

def generate_followup_node(state: AgentState):
    query = state.get("query", "")
    final_answer = state.get("final_answer", "")
    
    prompt = f"""You are a helpful data assistant. The user asked: "{query}"
We provided the following answer:
{final_answer}

Generate 3 relevant follow-up questions the user might want to ask next to explore the data further. 
Output ONLY a JSON list of 3 strings, with no markdown formatting or extra text.
Example: ["What is the total revenue?", "Which region performed best?", "Show me the breakdown by product."]"""

    # Use a separate LLM instance or just the same one with a different temp if needed
    # but for now reusing the global one for speed
    llm = get_llm()
    
    response = llm.invoke(prompt)
    raw = response.content.strip()
    
    # Strip any potential markdown code fences
    raw = re.sub(r"^```[a-z]*\n?", "", raw)
    raw = re.sub(r"```$", "", raw).strip()
    
    try:
        followups = json.loads(raw)
        if not isinstance(followups, list):
            followups = []
    except json.JSONDecodeError:
        followups = []
        
    return {"followup_questions": followups[:3]}

def should_continue(state: AgentState):
    # Loop back to generator if there's an error and we haven't tried too many times (e.g. max 5 iterations)
    iteration: int = state.get("iteration") or 0  # type: ignore[assignment]
    if state.get("error") and iteration < 5:
        return "generate_sql_node"
    return "generate_answer_node"

def build_graph():
    builder = StateGraph(AgentState)
    builder.add_node("classify_intent_node", classify_intent_node)
    builder.add_node("generate_sql_node", generate_sql_node)
    builder.add_node("execute_sql_node", execute_sql_node)
    builder.add_node("generate_answer_node", generate_answer_node)
    builder.add_node("generate_chart_node", generate_chart_node)
    builder.add_node("generate_followup_node", generate_followup_node)

    # Define edges
    builder.add_edge(START, "classify_intent_node")
    builder.add_edge("classify_intent_node", "generate_sql_node")
    builder.add_edge("generate_sql_node", "execute_sql_node")

    # Conditional edge from execution
    builder.add_conditional_edges(
        "execute_sql_node",
        should_continue,
        {
            "generate_sql_node": "generate_sql_node",
            "generate_answer_node": "generate_answer_node"
        }
    )
    # Chart node runs after answer is generated, then we're done
    # Chart node and Followup node run in parallel after answer is generated
    builder.add_edge("generate_answer_node", "generate_chart_node")
    builder.add_edge("generate_answer_node", "generate_followup_node")
    builder.add_edge("generate_chart_node", END)
    builder.add_edge("generate_followup_node", END)

    memory = MemorySaver()
    return builder.compile(checkpointer=memory)

def main():
    graph = build_graph()
    
    print("\n" + "="*50)
    print("LangGraph SQL Agent Loop is Ready!")
    print("Type 'quit' or 'exit' to stop.")
    print("="*50 + "\n")
    
    while True:
        try:
            query = input("\nEnter your question about the database: ").strip()
            if not query:
                continue
                
            if query.lower() in ['quit', 'exit']:
                print("Exiting...")
                break
                
            print("\nProcessing via LangGraph...")
            
            # Only pass fields that reset per-query.
            # Do NOT pass 'history' or other persistent fields —
            # MemorySaver carries them forward via the thread checkpoint.
            state = {
                "query": query,
                "iteration": 0,
                "error": "",
            }
            
            final_result = None
            final_followups = []
            
            # Stream the execution to observe loops
            config = {"configurable": {"thread_id": "cli_user_session"}}
            
            start_time = time.perf_counter()
            for event in graph.stream(state, config=config, stream_mode="updates"):
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
                    elif node_name == "generate_chart_node":
                        chart_path = node_output.get("chart_path")
                        final_result = node_output.get("final_answer", final_result)
                        if chart_path:
                            print(f"[{node_name}] Chart created: {chart_path}")
                        else:
                            print(f"[{node_name}] No chart generated (no table detected).")
                    elif node_name == "generate_followup_node":
                        final_followups = node_output.get("followup_questions", [])
                        if final_followups:
                            print(f"[{node_name}] Follow-up questions generated.")

            end_time = time.perf_counter()
            total_time = end_time - start_time

            if final_result is not None:
                print(f"\nFinal Answer:\n{final_result}")
                if final_followups:
                    print("\nSuggested Follow-ups:")
                    for i, q in enumerate(final_followups, 1):
                        print(f"  {i}. {q}")
            else:
                print("\nFailed to get an answer after maximum retries.")
            
            print(f"\nTotal time taken: {total_time:.2f} seconds")
            
        except KeyboardInterrupt:
            print("\nExiting...")
            break
        except Exception as e:
            print(f"\nSystem Error: {str(e)}")

if __name__ == "__main__":
    main()
