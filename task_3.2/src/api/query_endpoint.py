from flask import Flask, request, jsonify
import sqlite3
import pandas as pd
from datetime import datetime

app = Flask(__name__)


@app.route("/api/query/transactions", methods=["GET"])
def query_transactions():
    try:
        start_date = request.args.get("start_date")
        end_date = request.args.get("end_date")
        status = request.args.get("status")
        limit = request.args.get("limit", 100, type=int)

        conn = sqlite3.connect("data/processed/transactions.db")

        query = """
            SELECT 
                timestamp,
                SUM(CASE WHEN status = 'failed' THEN transaction_count ELSE 0 END) as failed,
                SUM(CASE WHEN status = 'denied' THEN transaction_count ELSE 0 END) as denied,
                SUM(CASE WHEN status = 'reversed' THEN transaction_count ELSE 0 END) as reversed,
                SUM(CASE WHEN status = 'approved' THEN transaction_count ELSE 0 END) as approved,
                SUM(transaction_count) as total,
                COUNT(DISTINCT strftime('%Y-%m-%d %H:%M', timestamp)) as minutes_count
            FROM transactions
            WHERE 1=1
        """

        params = []

        if start_date:
            query += " AND timestamp >= ?"
            params.append(start_date)

        if end_date:
            query += " AND timestamp <= ?"
            params.append(end_date)

        if status:
            query += " AND status = ?"
            params.append(status)

        query += " GROUP BY timestamp ORDER BY timestamp DESC LIMIT ?"
        params.append(limit)

        df = pd.read_sql_query(query, conn, params=params)
        conn.close()

        stats = {}

        if not df.empty:
            for col in ["failed", "denied", "reversed", "approved", "total"]:
                stats[col] = {
                    "mean": float(df[col].mean()),
                    "std": float(df[col].std()),
                    "max": int(df[col].max()),
                    "min": int(df[col].min()),
                    "p95": float(df[col].quantile(0.95)),
                    "sum": int(df[col].sum()),
                }

        return jsonify(
            {
                "success": True,
                "filters": {
                    "start_date": start_date,
                    "end_date": end_date,
                    "status": status,
                    "limit": limit,
                },
                "statistics": stats,
                "data": df.to_dict(orient="records"),
                "row_count": len(df),
            }
        )

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/query/anomaly-patterns", methods=["GET"])
def query_anomaly_patterns():
    try:
        conn = sqlite3.connect("data/processed/transactions.db")

        query_hourly = """
            WITH hourly_stats AS (
                SELECT 
                    strftime('%H', timestamp) as hour,
                    AVG(CASE WHEN status = 'failed' THEN transaction_count ELSE 0 END) as avg_failed,
                    AVG(CASE WHEN status = 'denied' THEN transaction_count ELSE 0 END) as avg_denied,
                    AVG(CASE WHEN status = 'reversed' THEN transaction_count ELSE 0 END) as avg_reversed,
                    AVG(transaction_count) as avg_total,
                    COUNT(*) as samples
                FROM transactions
                GROUP BY strftime('%H', timestamp)
            )
            SELECT 
                hour,
                avg_failed,
                avg_denied,
                avg_reversed,
                avg_total,
                samples,
                (avg_failed / NULLIF(avg_total, 0)) * 100 as failed_rate_pct,
                (avg_denied / NULLIF(avg_total, 0)) * 100 as denied_rate_pct,
                (avg_reversed / NULLIF(avg_total, 0)) * 100 as reversed_rate_pct
            FROM hourly_stats
            ORDER BY hour
        """

        query_daily = """
            SELECT 
                date(timestamp) as day,
                SUM(CASE WHEN status = 'failed' THEN transaction_count ELSE 0 END) as total_failed,
                SUM(CASE WHEN status = 'denied' THEN transaction_count ELSE 0 END) as total_denied,
                SUM(CASE WHEN status = 'reversed' THEN transaction_count ELSE 0 END) as total_reversed,
                SUM(transaction_count) as total_transactions,
                COUNT(*) as minutes_count
            FROM transactions
            GROUP BY date(timestamp)
            ORDER BY total_failed DESC
            LIMIT 10
        """

        df_hourly = pd.read_sql_query(query_hourly, conn)
        df_daily = pd.read_sql_query(query_daily, conn)

        conn.close()

        return jsonify(
            {
                "success": True,
                "hourly_patterns": df_hourly.to_dict(orient="records"),
                "worst_days": df_daily.to_dict(orient="records"),
                "analysis_timestamp": datetime.now().isoformat(),
            }
        )

    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(debug=True, port=5000)
