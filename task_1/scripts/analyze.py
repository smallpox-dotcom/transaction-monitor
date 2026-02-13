import argparse
import sqlite3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
from matplotlib.lines import Line2D
from matplotlib.patches import Patch
from datetime import datetime


def get_tables_from_db(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'checkout_%'")
    return [table[0] for table in cursor.fetchall()]


def detect_anomalies(df, threshold=0.30):
    df = df.copy()
    
    baseline = df["avg_last_week"]
    df["diff"] = df["today"] - baseline
    df["pct"] = df["diff"] / baseline.replace(0, 1)
    df["abs_pct"] = df["pct"].abs()
    
    if len(baseline[baseline > 0]) > 0:
        min_threshold = np.percentile(baseline[baseline > 0], 10)
    else:
        min_threshold = 5
    
    df["abs_threshold"] = np.maximum(min_threshold, baseline * threshold)
    
    std = df["avg_last_week"].std()
    mean = df["avg_last_week"].mean()
    
    critical_high = (df["pct"] > 1.0) & (df["diff"] > 10)
    critical_low = (df["pct"] < -0.5) & (df["avg_last_week"] > 10)
    outage = (df["today"] == 0) & (df["avg_last_week"] > 15)
    extreme_deviation = (df["today"].abs() > mean + 3 * std) & (df["today"] > 10)
    
    suspicious_high = (df["pct"] > 0.5) & (df["pct"] <= 1.0) & (df["diff"] > 5)
    suspicious_low = (df["pct"] < -0.3) & (df["pct"] >= -0.5) & (df["avg_last_week"] > 5)
    
    base_anomaly = ((df["diff"].abs() > df["abs_threshold"]) & (df["abs_pct"] > threshold) & (baseline >= 5))
    
    df["anomaly_level"] = "normal"
    df.loc[critical_high | critical_low | outage | extreme_deviation, "anomaly_level"] = "critical"
    df.loc[suspicious_high | suspicious_low, "anomaly_level"] = "suspicious"
    
    mild_condition = base_anomaly & (df["anomaly_level"] == "normal")
    df.loc[mild_condition, "anomaly_level"] = "mild"
    
    conditions = [
        (df["anomaly_level"] == "critical"),
        (df["anomaly_level"] == "suspicious"),
        (df["anomaly_level"] == "mild")
    ]
    
    scores = [
        np.clip(df["abs_pct"] * 5 + (df["diff"].abs() / (df["abs_threshold"] + 1)), 7, 10),
        np.clip(df["abs_pct"] * 3 + (df["diff"].abs() / (df["abs_threshold"] + 1)), 4, 7),
        np.clip(df["abs_pct"] * 2 + (df["diff"].abs() / (df["abs_threshold"] + 1)), 1, 4)
    ]
    
    df["severity_score"] = np.select(conditions, scores, default=0)
    df["confidence"] = np.where(
        df["avg_last_week"] > 20,
        np.clip(100 - df["abs_pct"] * 20, 70, 100),
        np.clip(100 - df["abs_pct"] * 30, 50, 100)
    )
    
    return df


def generate_detailed_analysis(critical_anomalies, suspicious_anomalies, mild_anomalies, df, table_name):
    analysis = []
    
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    table_id = table_name.replace('checkout_', '')
    
    analysis.append("# POS Sales Anomaly Report")
    analysis.append("")
    analysis.append(f"## Checkout {table_id}")
    analysis.append("")
    analysis.append(f"**Generated:** {timestamp}")
    analysis.append("")
    analysis.append("---")
    analysis.append("")
    
    total_sales = df['today'].sum()
    avg_sales = df['today'].mean()
    avg_weekly = df['avg_last_week'].mean()
    
    analysis.append("## Overall Performance")
    analysis.append("")
    analysis.append("| Metric | Value |")
    analysis.append("|--------|-------|")
    analysis.append(f"| Total Transactions Today | {total_sales:.0f} |")
    analysis.append(f"| Average Today | {avg_sales:.1f} transactions |")
    analysis.append(f"| Average Weekly | {avg_weekly:.1f} transactions |")
    
    if avg_weekly > 0:
        overall_change = ((avg_sales - avg_weekly) / avg_weekly) * 100
        analysis.append(f"| Trend vs Weekly Avg | {overall_change:+.1f}% |")
    analysis.append("")
    analysis.append("---")
    analysis.append("")
    
    if not critical_anomalies.empty:
        analysis.append("## Critical Anomalies")
        analysis.append("")
        analysis.append("**IMMEDIATE ACTION REQUIRED**")
        analysis.append("")
        
        for idx, (_, anomaly) in enumerate(critical_anomalies.iterrows(), 1):
            analysis.append(f"### Critical Anomaly #{idx}")
            analysis.append("")
            analysis.append(f"**Time:** {anomaly['time']}")
            analysis.append("")
            
            analysis.append("#### Sales Data")
            analysis.append("")
            analysis.append("| Metric | Value |")
            analysis.append("|--------|-------|")
            analysis.append(f"| Current | {anomaly['today']:.0f} |")
            analysis.append(f"| Weekly Avg | {anomaly['avg_last_week']:.0f} |")
            analysis.append(f"| Yesterday | {anomaly['yesterday']:.0f} |")
            analysis.append(f"| Same Day Last Week | {anomaly['same_day_last_week']:.0f} |")
            analysis.append("")
            
            deviation_pct = anomaly['pct'] * 100
            deviation_abs = anomaly['diff']
            
            analysis.append("#### Deviation Analysis")
            analysis.append("")
            if deviation_pct > 0:
                analysis.append(f"- **Change:** +{deviation_pct:.0f}% (+{deviation_abs:.0f} transactions)")
                analysis.append(f"- **Ratio:** {anomaly['pct']+1:.1f}x weekly average")
            else:
                analysis.append(f"- **Change:** {deviation_pct:.0f}% ({deviation_abs:.0f} transactions)")
                analysis.append(f"- **Ratio:** {abs(anomaly['pct']):.1f}x below weekly average")
            analysis.append("")
            
            analysis.append("#### Root Cause Analysis")
            analysis.append("")
            
            if anomaly['today'] == 0 and anomaly['avg_last_week'] > 15:
                analysis.append("- **Type:** TOTAL OUTAGE")
                analysis.append("- **Likely causes:** Payment system failure or connectivity issue")
                analysis.append("- **Immediate actions:** Contact location manager")
                
            elif anomaly['pct'] > 1.0:
                analysis.append("- **Type:** EXTREME SALES PEAK")
                analysis.append("- **Likely causes:** Special promotion or data error")
                analysis.append("- **Verification:** Check with location")
                
            elif anomaly['pct'] < -0.5:
                analysis.append("- **Type:** DRASTIC SALES DROP")
                analysis.append("- **Likely causes:** System outage or unusual conditions")
                analysis.append("- **Investigation:** Review system logs")
                
            elif anomaly['today'].abs() > anomaly['avg_last_week'].mean() + 3 * anomaly['avg_last_week'].std():
                analysis.append("- **Type:** STATISTICAL OUTLIER")
                analysis.append("- **Analysis:** 3+ standard deviations from mean")
                
            else:
                analysis.append("- **Type:** THRESHOLD BREACH")
                analysis.append("- **Exceeds configured sensitivity threshold**")
            analysis.append("")
            
            analysis.append("#### Risk Assessment")
            analysis.append("")
            analysis.append(f"- **Severity Score:** {anomaly['severity_score']:.1f}/10")
            analysis.append(f"- **Confidence:** {anomaly['confidence']:.0f}%")
            
            if anomaly['severity_score'] >= 9:
                analysis.append("- **Risk Level:** CRITICAL")
            elif anomaly['severity_score'] >= 7:
                analysis.append("- **Risk Level:** HIGH")
            else:
                analysis.append("- **Risk Level:** MEDIUM")
            
            analysis.append("")
            analysis.append("---")
            analysis.append("")
    
    if not suspicious_anomalies.empty:
        analysis.append("## Suspicious Anomalies")
        analysis.append("")
        analysis.append("**MONITOR CLOSELY**")
        analysis.append("")
        analysis.append(f"Total suspicious anomalies: {len(suspicious_anomalies)}")
        analysis.append("")
        analysis.append("| Time | Transactions | Deviation |")
        analysis.append("|------|-------------|-----------|")
        
        for _, anomaly in suspicious_anomalies.nlargest(3, 'severity_score').iterrows():
            deviation = anomaly['pct'] * 100
            direction = "+" if deviation > 0 else ""
            analysis.append(f"| {anomaly['time']} | {anomaly['today']:.0f} | {direction}{deviation:.0f}% |")
        
        if len(suspicious_anomalies) > 3:
            analysis.append(f"| ... and {len(suspicious_anomalies) - 3} more | | |")
        
        analysis.append("")
        analysis.append("---")
        analysis.append("")
    
    if not mild_anomalies.empty:
        analysis.append("## Mild Anomalies")
        analysis.append("")
        analysis.append("**NORMAL FLUCTUATIONS**")
        analysis.append("")
        analysis.append(f"Total mild anomalies: {len(mild_anomalies)}")
        analysis.append("")
        analysis.append("These are within expected business variations.")
        analysis.append("No immediate action required.")
        analysis.append("")
        analysis.append("---")
        analysis.append("")
    
    analysis.append("## Recommendations")
    analysis.append("")
    
    if not critical_anomalies.empty:
        analysis.append(f"### Critical ({len(critical_anomalies)})")
        analysis.append("")
        analysis.append("1. Assign to operations team")
        analysis.append("2. Contact location")
        analysis.append("3. Document resolution")
        analysis.append("")
    
    if not suspicious_anomalies.empty:
        analysis.append(f"### Suspicious ({len(suspicious_anomalies)})")
        analysis.append("")
        analysis.append("1. Review within 24 hours")
        analysis.append("2. Check for patterns")
        analysis.append("")
    
    if critical_anomalies.empty and suspicious_anomalies.empty:
        analysis.append("All systems operating normally.")
        analysis.append("Continue regular monitoring.")
        analysis.append("")
    
    analysis.append("---")
    analysis.append("")
    analysis.append(f"*Report generated automatically by POS Sales Analysis System*")
    
    return analysis


def save_analysis_to_file(analysis_text, table_name):
    output_dir = "./outputs/reports"
    os.makedirs(output_dir, exist_ok=True)
    
    table_id = table_name.replace('checkout_', '')
    filename = f"{output_dir}/checkout_{table_id}_report.md"
    
    with open(filename, 'w', encoding='utf-8') as f:
        f.write("\n".join(analysis_text))
    
    return filename


def get_dashboard_filename(table_name):
    table_id = table_name.replace('checkout_', '')
    return f"./outputs/visualizations/checkout_{table_id}_dashboard.png"


def create_visualization(df, table_name, output_path):
    plt.style.use("seaborn-v0_8-darkgrid")
    
    fig = plt.figure(figsize=(20, 10))
    gs = fig.add_gridspec(1, 12)
    
    ax = fig.add_subplot(gs[0, :10])
    
    table_id = table_name.replace('checkout_', '')
    
    ax.axhline(y=0, color='black', linestyle='-', linewidth=1.5, alpha=0.7, zorder=0)
    
    color = plt.cm.Set2(0)
    
    ax.fill_between(
        df["time"],
        df["avg_last_week"] * 0.7,
        df["avg_last_week"] * 1.3,
        alpha=0.08,
        color=color,
        label='Normal range (±30%)'
    )
    
    line1, = ax.plot(df["time"], df["yesterday"], '--', alpha=0.6, 
                    color='blue', linewidth=1.8, label='Yesterday')
    line2, = ax.plot(df["time"], df["avg_last_week"], ':', alpha=0.6,
                    color='#666666', linewidth=1.8, label='Week Avg')
    line3, = ax.plot(df["time"], df["avg_last_month"], '-.', alpha=0.6,
                    color='green', linewidth=1.8, label='Month Avg')
    
    today_line, = ax.plot(df["time"], df["today"], '-', linewidth=3.5,
                        color=color, alpha=0.9, label='Today')
    
    critical_points = df[df["anomaly_level"] == "critical"]
    suspicious_points = df[df["anomaly_level"] == "suspicious"]
    mild_points = df[df["anomaly_level"] == "mild"]
    
    if not critical_points.empty:
        ax.scatter(critical_points["time"], critical_points["today"], s=120,
                  c='red', edgecolors='black', zorder=5, 
                  alpha=0.9, marker='o', linewidths=1.5, label='Critical')
    
    if not suspicious_points.empty:
        ax.scatter(suspicious_points["time"], suspicious_points["today"], s=100,
                  c='orange', edgecolors='black', zorder=5, 
                  alpha=0.9, marker='o', linewidths=1.2, label='Suspicious')
    
    if not mild_points.empty:
        ax.scatter(mild_points["time"], mild_points["today"], s=80,
                  c='gold', edgecolors='black', zorder=5, 
                  alpha=0.9, marker='o', linewidths=1.0, label='Mild')
    
    ax.set_title(f"Checkout {table_id} Sales Dashboard", fontsize=22, weight='bold', pad=20)
    ax.set_ylabel("Number of Transactions", fontsize=16, weight='bold', labelpad=10)
    ax.set_xlabel("Time", fontsize=16, weight='bold', labelpad=10)
    
    ax.grid(True, alpha=0.2, linestyle='-', linewidth=0.5)
    ax.grid(True, which='major', axis='y', alpha=0.3, linestyle='--', linewidth=0.3)
    
    ax.tick_params(axis='both', which='major', labelsize=13)
    ax.tick_params(axis='x', rotation=45)
    
    y_min, y_max = ax.get_ylim()
    if y_min < 0 or y_max > 0:
        ax.set_ylim(min(y_min, 0), max(y_max * 1.05, 5))
    
    legend_ax = fig.add_subplot(gs[0, 10:])
    legend_ax.axis('off')
    
    from matplotlib.patches import Patch
    from matplotlib.lines import Line2D
    
    line_legend_elements = [
        Line2D([0], [0], color='blue', linestyle='--', linewidth=1.8, label='Yesterday'),
        Line2D([0], [0], color='#666666', linestyle=':', linewidth=1.8, label='Week Avg'),
        Line2D([0], [0], color='green', linestyle='-.', linewidth=1.8, label='Month Avg'),
        Line2D([0], [0], color=color, linestyle='-', linewidth=3.5, label='Today'),
        Patch(facecolor=color, alpha=0.08, edgecolor='none', label='Normal range (±30%)')
    ]
    
    anomaly_legend_elements = [
        Line2D([0], [0], marker='o', color='w', markerfacecolor='red', 
               markersize=12, markeredgecolor='black', markeredgewidth=1.5, label='Critical'),
        Line2D([0], [0], marker='o', color='w', markerfacecolor='orange', 
               markersize=10, markeredgecolor='black', markeredgewidth=1.2, label='Suspicious'),
        Line2D([0], [0], marker='o', color='w', markerfacecolor='gold', 
               markersize=8, markeredgecolor='black', markeredgewidth=1.0, label='Mild')
    ]
    
    legend1 = legend_ax.legend(
        handles=line_legend_elements,
        loc='upper center',
        bbox_to_anchor=(0.5, 0.8),
        fontsize=11,
        title="Data Series:",
        title_fontproperties={'weight': 'bold', 'size': 13},
        framealpha=0.9,
        borderpad=1.0,
        labelspacing=0.7,
        handlelength=2.0,
        handletextpad=0.7,
        columnspacing=0.7
    )
    legend_ax.add_artist(legend1)
    
    legend2 = legend_ax.legend(
        handles=anomaly_legend_elements,
        loc='upper center',
        bbox_to_anchor=(0.5, 0.45),
        fontsize=11,
        title="Anomaly Levels:",
        title_fontproperties={'weight': 'bold', 'size': 13},
        framealpha=0.9,
        borderpad=1.0,
        labelspacing=0.7,
        handlelength=2.0,
        handletextpad=0.7,
        columnspacing=0.7
    )
    
    plt.tight_layout()
    plt.subplots_adjust(left=0.05, right=0.92, top=0.92, bottom=0.1)
    
    plt.savefig(output_path, dpi=150, bbox_inches='tight', pad_inches=0.2)
    
    print(f"   Dashboard: checkout_{table_id}_dashboard.png")
    plt.show()


def process_single_table(table_name, conn, threshold, export_csv, no_analysis):
    print(f"\nProcessing: {table_name}")
    print("-" * 40)
    
    query = f"""
        SELECT time, today, yesterday, same_day_last_week, avg_last_week, avg_last_month
        FROM {table_name}
        ORDER BY time
    """
    df = pd.read_sql(query, conn)
    df["source"] = table_name
    
    if df.empty:
        print(f"   No data in table {table_name}")
        return None
    
    df = detect_anomalies(df, threshold)
    
    table_id = table_name.replace('checkout_', '')
    
    critical_anomalies = df[df["anomaly_level"] == "critical"]
    suspicious_anomalies = df[df["anomaly_level"] == "suspicious"]
    mild_anomalies = df[df["anomaly_level"] == "mild"]
    
    total = df['today'].sum()
    avg_today = df['today'].mean()
    avg_week = df['avg_last_week'].mean()
    
    print(f"   Total transactions: {total:.0f}")
    print(f"   Average today: {avg_today:.1f}")
    
    if avg_week > 0:
        change = ((avg_today - avg_week) / avg_week) * 100
        arrow = "▲" if change > 0 else "▼"
        color = "\033[92m" if change > 0 else "\033[91m"
        reset = "\033[0m"
        print(f"   vs weekly avg: {color}{arrow} {abs(change):.1f}%{reset}")
    
    alert_count = {"critical": len(critical_anomalies), 
                  "suspicious": len(suspicious_anomalies), 
                  "mild": len(mild_anomalies)}
    
    if alert_count['critical'] > 0:
        print(f"   Critical: {alert_count['critical']}")
    else:
        print(f"   Critical: {alert_count['critical']}")
    
    print(f"   Suspicious: {alert_count['suspicious']}")
    print(f"   Mild: {alert_count['mild']}")
    
    if not no_analysis:
        analysis_text = generate_detailed_analysis(critical_anomalies, suspicious_anomalies, 
                                                 mild_anomalies, df, table_name)
        analysis_file = save_analysis_to_file(analysis_text, table_name)
        print(f"   Report: checkout_{table_id}_report.md")
    
    os.makedirs("./outputs/visualizations", exist_ok=True)
    dashboard_file = get_dashboard_filename(table_name)
    create_visualization(df, table_name, dashboard_file)
    
    if export_csv:
        os.makedirs("./outputs/exports", exist_ok=True)
        csv_file = f"./outputs/exports/checkout_{table_id}_data.csv"
        
        export_df = df[['time', 'today', 'yesterday', 'same_day_last_week', 
                       'avg_last_week', 'avg_last_month', 'diff', 'pct', 
                       'anomaly_level', 'severity_score', 'confidence']].copy()
        export_df['pct'] = export_df['pct'] * 100
        
        severity_order = {"critical": 3, "suspicious": 2, "mild": 1, "normal": 0}
        export_df['severity_num'] = export_df['anomaly_level'].map(severity_order)
        export_df = export_df.sort_values(['severity_num', 'severity_score'], ascending=[False, False])
        export_df = export_df.drop('severity_num', axis=1)
        
        export_df.to_csv(csv_file, index=False, float_format='%.2f')
        print(f"   CSV: checkout_{table_id}_data.csv")
    
    return alert_count


def main():
    parser = argparse.ArgumentParser(
        description="Analyze POS sales data and generate individual reports",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python analyze.py                    # Analyze all checkout tables
  python analyze.py --table checkout_1 # Analyze specific table
  python analyze.py --threshold 0.25   # Custom sensitivity
  python analyze.py --export           # Export CSV data
  python analyze.py --no-analysis      # Skip report generation
        """
    )
    
    parser.add_argument("--db", default="./outputs/database/monitor.db", help="Database path")
    parser.add_argument("--table", nargs="*", help="Specific table(s) to analyze")
    parser.add_argument("--export", action="store_true", help="Export CSV data")
    parser.add_argument("--threshold", type=float, default=0.30, help="Sensitivity threshold")
    parser.add_argument("--no-analysis", action="store_true", help="Skip report generation")
    
    args = parser.parse_args()
    
    conn = sqlite3.connect(args.db)
    
    try:
        import sqlite3 as sqlite3_module
        conn = sqlite3_module.connect(args.db)
        
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'checkout_%'")
        all_tables = [table[0] for table in cursor.fetchall()]
        
        if args.table:
            tables = args.table
        else:
            tables = all_tables
        
        if not tables:
            print("No checkout tables found")
            print("Run 'python ingest.py' to load data first")
            return
        
        print("=" * 70)
        print("POS SALES ANALYSIS SYSTEM")
        print("=" * 70)
        print(f"Total tables: {len(tables)}")
        print(f"Threshold: {args.threshold}")
        print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        print("-" * 70)
        
        total_critical = 0
        total_suspicious = 0
        total_mild = 0
        processed_tables = 0
        
        for table in tables:
            alert_count = process_single_table(table, conn, args.threshold, 
                                             args.export, args.no_analysis)
            
            if alert_count:
                total_critical += alert_count['critical']
                total_suspicious += alert_count['suspicious']
                total_mild += alert_count['mild']
                processed_tables += 1
        
        print("\n" + "=" * 70)
        print("ANALYSIS SUMMARY")
        print("=" * 70)
        print(f"Processed tables: {processed_tables}")
        print(f"Critical anomalies: {total_critical}")
        print(f"Suspicious anomalies: {total_suspicious}")
        print(f"Mild anomalies: {total_mild}")
        
        print(f"\nReports: ./outputs/reports/ (Markdown format)")
        print(f"Dashboards: ./outputs/visualizations/")
        
        if args.export:
            print(f"CSV data: ./outputs/exports/")
        
        if total_critical > 0:
            print(f"\nACTION REQUIRED: {total_critical} critical anomalies across {processed_tables} checkouts!")
        else:
            print(f"\nAll {processed_tables} checkouts operating normally.")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()