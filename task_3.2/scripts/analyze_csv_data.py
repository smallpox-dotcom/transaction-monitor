import pandas as pd
import sqlite3
from datetime import datetime

print("=" * 60)
print("📊 ANALYZING CSV DATA")
print("=" * 60)

df1 = pd.read_csv("data/raw/transactions.csv")
df2 = pd.read_csv("data/raw/transactions_auth_codes.csv")

print("\n📁 transactions.csv:")
print(f"   Shape: {df1.shape}")
print(f"   Columns: {list(df1.columns)}")
print(f"   Unique timestamps: {df1['timestamp'].nunique()}")
print(f"   Unique statuses: {df1['status'].unique()}")

print("\n📁 transactions_auth_codes.csv:")
print(f"   Shape: {df2.shape}")
print(f"   Columns: {list(df2.columns)}")
print(f"   Unique timestamps: {df2['timestamp'].nunique()}")
print(
    f"   Unique auth codes: {df2['auth_code'].unique() if 'auth_code' in df2.columns else 'N/A'}"
)

print("\n📈 HISTORICAL METRICS BY STATUS:")

for status in ["failed", "denied", "reversed", "approved"]:
    if status in df1["status"].values:
        mean_value = df1[df1["status"] == status]["count"].mean()
        std_value = df1[df1["status"] == status]["count"].std()
        max_value = df1[df1["status"] == status]["count"].max()

        print(f"\n   {status.upper()}:")
        print(f"      Mean: {mean_value:.2f}")
        print(f"      Std: {std_value:.2f}")
        print(f"      Max: {max_value:.2f}")

conn = sqlite3.connect("data/processed/transactions.db")

print("\n🗄️ DATABASE CHECK:")

query = """
SELECT 
    timestamp,
    SUM(CASE WHEN status = 'failed' THEN transaction_count ELSE 0 END) AS failed,
    SUM(CASE WHEN status = 'denied' THEN transaction_count ELSE 0 END) AS denied,
    SUM(CASE WHEN status = 'reversed' THEN transaction_count ELSE 0 END) AS reversed,
    SUM(CASE WHEN status = 'approved' THEN transaction_count ELSE 0 END) AS approved,
    SUM(transaction_count) AS total
FROM transactions
GROUP BY timestamp
ORDER BY timestamp
LIMIT 5
"""

df_sample = pd.read_sql_query(query, conn)

print("\n   Aggregated data sample:")
print(df_sample.to_string())

conn.close()
