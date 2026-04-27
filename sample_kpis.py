import sqlite3
conn = sqlite3.connect('database.sqlite')
cur = conn.cursor()

# Inventory KPIs
cur.execute("SELECT COUNT(DISTINCT store_id) FROM inventory_data")
stores = cur.fetchone()[0]
cur.execute("SELECT COUNT(DISTINCT product_id) FROM inventory_data")
products = cur.fetchone()[0]
cur.execute("SELECT SUM(CAST(units_sold AS REAL)) FROM inventory_data")
total_sold = cur.fetchone()[0]
cur.execute("SELECT SUM(CAST(units_sold AS REAL)*CAST(price AS REAL)) FROM inventory_data")
total_rev = cur.fetchone()[0]

# Supply chain KPIs
cur.execute("SELECT COUNT(*) FROM supply_chain_logistics")
total_shipments = cur.fetchone()[0]
cur.execute("SELECT COUNT(*) FROM supply_chain_logistics WHERE delivery_status='Delayed'")
delayed = cur.fetchone()[0]
cur.execute("SELECT COUNT(*) FROM supply_chain_logistics WHERE delivery_status='Delivered'")
delivered = cur.fetchone()[0]
cur.execute("SELECT SUM(CAST(shipping_cost_ngn AS REAL)) FROM supply_chain_logistics")
total_cost = cur.fetchone()[0]

print(f"Stores: {stores}, Products: {products}")
print(f"Total Units Sold: {total_sold:,.0f}")
print(f"Total Revenue: {total_rev:,.2f}")
print(f"Total Shipments: {total_shipments:,}")
print(f"Delayed: {delayed:,}, Delivered: {delivered:,}")
print(f"Total Shipping Cost: {total_cost:,.2f}")

# Distinct delivery statuses
cur.execute("SELECT DISTINCT delivery_status FROM supply_chain_logistics")
print("Statuses:", cur.fetchall())

conn.close()
