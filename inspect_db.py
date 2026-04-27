import sqlite3
conn = sqlite3.connect('database.sqlite')
cur = conn.cursor()
cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
tables = cur.fetchall()
print('Tables:', tables)
for tbl in tables:
    tname = tbl[0]
    cur.execute(f'PRAGMA table_info("{tname}")')
    cols = cur.fetchall()
    print(f'\nTable: {tname}')
    for col in cols:
        print(f'  {col[1]} ({col[2]})')
    cur.execute(f'SELECT COUNT(*) FROM "{tname}"')
    cnt = cur.fetchone()
    print(f'  Row count: {cnt[0]}')
conn.close()
