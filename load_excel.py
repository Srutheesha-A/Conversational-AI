import sqlite3
import sys
import os

try:
    import openpyxl
except ImportError:
    print("openpyxl is required. Install it with: pip install openpyxl")
    sys.exit(1)


def load_excel(db_path, excel_path, table_name, sheet_name=0):
    """
    Load a single sheet from an Excel (.xlsx / .xls) file into a SQLite table.

    Parameters
    ----------
    db_path    : str  – Path to the SQLite database file.
    excel_path : str  – Path to the Excel file.
    table_name : str  – Target table name in the database.
    sheet_name : str | int – Sheet name or 0-based index (default: first sheet).
    """
    print(f"Loading '{excel_path}' (sheet={sheet_name!r}) into '{table_name}'...")

    wb = openpyxl.load_workbook(excel_path, read_only=True, data_only=True)

    # Resolve sheet
    if isinstance(sheet_name, int):
        ws = wb.worksheets[sheet_name]
    else:
        ws = wb[sheet_name]

    rows_iter = ws.iter_rows(values_only=True)

    # First row = headers
    headers = [str(h) if h is not None else f"col_{i}" for i, h in enumerate(next(rows_iter))]

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # (Re-)create table — all columns stored as TEXT for simplicity
    cols_def = ", ".join([f'"{h}" TEXT' for h in headers])
    cursor.execute(f'DROP TABLE IF EXISTS "{table_name}"')
    cursor.execute(f'CREATE TABLE "{table_name}" ({cols_def})')

    placeholders = ", ".join(["?"] * len(headers))
    insert_query = f'INSERT INTO "{table_name}" VALUES ({placeholders})'

    batch = []
    count = 0
    for row in rows_iter:
        # Convert every cell to string (or empty string for None)
        batch.append(tuple("" if v is None else str(v) for v in row))
        if len(batch) >= 10000:
            cursor.executemany(insert_query, batch)
            batch = []
            count += 10000
            print(f"  Inserted {count} rows...")

    if batch:
        cursor.executemany(insert_query, batch)
        count += len(batch)
        print(f"  Inserted {count} rows...")

    conn.commit()
    conn.close()
    wb.close()
    print(f"Finished loading '{table_name}' ({count} rows).")


if __name__ == "__main__":
    db_file = os.path.join(os.path.dirname(__file__), "database.sqlite")

    # ── Edit these paths / table names as needed ──────────────────────────────
    excel_file = r"c:\Users\Administrator\Downloads\data.xlsx"
    target_table = "excel_data"
    # To load a specific sheet by name, set sheet_name="Sheet1"
    # To load by index (0-based), set sheet_name=0
    sheet = 0
    # ──────────────────────────────────────────────────────────────────────────

    load_excel(db_file, excel_file, target_table, sheet_name=sheet)
    print(f"Database updated at {db_file}")
