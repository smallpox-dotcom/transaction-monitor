import subprocess
import argparse
import sys
import os
import time
from datetime import datetime
import sqlite3


def get_existing_checkout_tables():
    db_path = "./outputs/database/monitor.db"
    if not os.path.exists(db_path):
        return []
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'checkout_%'")
        tables = [row[0] for row in cursor.fetchall()]
        conn.close()
        return tables
    except:
        return []


def get_existing_reports():
    reports_dir = "./outputs/reports"
    if not os.path.exists(reports_dir):
        return []
    
    report_files = [f for f in os.listdir(reports_dir) if f.endswith('_report.txt')]
    return [f.replace('_report.txt', '').replace('checkout_', '') for f in report_files]


def get_existing_dashboards():
    viz_dir = "./outputs/visualizations"
    if not os.path.exists(viz_dir):
        return []
    
    dashboard_files = [f for f in os.listdir(viz_dir) if f.endswith('_dashboard.png')]
    return [f.replace('_dashboard.png', '').replace('checkout_', '') for f in dashboard_files]


def check_table_needs_processing(table_name, skip_existing):
    if not skip_existing:
        return True
    
    table_id = table_name.replace('checkout_', '')
    
    report_exists = table_id in get_existing_reports()
    dashboard_exists = table_id in get_existing_dashboards()
    
    if report_exists and dashboard_exists:
        print(f"   Skipping {table_name} (report and dashboard exist)")
        return False
    
    return True


def run_ingestion(checkout_dir="./data/raw"):
    print("\n" + "=" * 70)
    print("STAGE 1: DATA INGESTION")
    print("=" * 70)
    
    print(f"Checking directory: {checkout_dir}")
    
    if not os.path.exists(checkout_dir):
        print(f"Directory not found: {checkout_dir}")
        print("Creating directory...")
        os.makedirs(checkout_dir, exist_ok=True)
        print(f"Created: {checkout_dir}")
        print("Please add CSV files and run pipeline again")
        return False
    
    csv_files = [f for f in os.listdir(checkout_dir) if f.endswith('.csv') and f.startswith('checkout_')]
    
    if not csv_files:
        print(f"No checkout_*.csv files found in {checkout_dir}")
        print(f"Expected: checkout_1.csv, checkout_2.csv, etc.")
        return False
    
    print(f"Found {len(csv_files)} CSV file(s):")
    for csv_file in csv_files:
        print(f"   * {csv_file}")
    
    existing_tables = get_existing_checkout_tables()
    existing_table_ids = [t.replace('checkout_', '') for t in existing_tables]
    
    new_files = []
    for csv_file in csv_files:
        table_id = csv_file.replace('checkout_', '').replace('.csv', '')
        if table_id not in existing_table_ids:
            new_files.append(csv_file)
    
    if not new_files and existing_tables:
        print(f"\nAll CSV files already ingested")
        print("No new data to process")
        return True
    
    if new_files:
        print(f"\nNew files to ingest: {len(new_files)}")
        for csv_file in new_files:
            print(f"   * {csv_file}")
    
    command = [sys.executable, "./scripts/ingest.py", "--checkout-dir", checkout_dir]
    
    print(f"\nRunning: {' '.join(command)}")
    
    start_time = time.time()
    result = subprocess.run(command, capture_output=True, text=True)
    elapsed_time = time.time() - start_time
    
    print(result.stdout)
    
    if result.returncode != 0:
        print(f"Ingestion failed:")
        print(result.stderr)
        return False
    
    print(f"Ingestion completed in {elapsed_time:.1f} seconds")
    return True

def run_analysis(threshold=0.30, export_csv=False, skip_existing=True):
    print("\n" + "=" * 70)
    print("STAGE 2: ANOMALY ANALYSIS")
    print("=" * 70)
    
    tables = get_existing_checkout_tables()
    
    if not tables:
        print("No checkout tables found")
        print("Run ingestion stage first")
        return False
    
    print(f"Found {len(tables)} table(s) in database:")
    
    tables_to_process = []
    for table in tables:
        if check_table_needs_processing(table, skip_existing):
            tables_to_process.append(table)
            print(f"   * {table} - Will process")
        else:
            print(f"   * {table} - Skipping")
    
    if not tables_to_process:
        print(f"\nAll tables already processed")
        print("No new analysis needed")
        return True
    
    print(f"\nTables to analyze: {len(tables_to_process)}")
    
    total_start_time = time.time()
    success_count = 0
    
    for table in tables_to_process:
        print(f"\nProcessing: {table}")
        print("-" * 40)
        
        command = [
            sys.executable, "./scripts/analyze.py",
            "--table", table,
            "--threshold", str(threshold)
        ]
        
        if export_csv:
            command.append("--export")
        
        start_time = time.time()
        result = subprocess.run(command, capture_output=True, text=True)
        elapsed_time = time.time() - start_time
        
        if result.returncode == 0:
            print(f"Completed in {elapsed_time:.1f}s")
            print(result.stdout)
            success_count += 1
        else:
            print(f"Failed for {table}")
            print(result.stderr)
    
    total_elapsed_time = time.time() - total_start_time
    
    print(f"\nAnalysis summary:")
    print(f"   * Total tables: {len(tables)}")
    print(f"   * Processed: {success_count}")
    print(f"   * Skipped: {len(tables) - len(tables_to_process)}")
    print(f"   * Failed: {len(tables_to_process) - success_count}")
    print(f"   * Total time: {total_elapsed_time:.1f} seconds")
    
    return success_count > 0


def show_summary():
    print("\n" + "=" * 70)
    print("PIPELINE SUMMARY")
    print("=" * 70)
    
    db_path = "./outputs/database/monitor.db"
    if os.path.exists(db_path):
        tables = get_existing_checkout_tables()
        print(f"Database: {db_path}")
        print(f"Tables: {len(tables)} checkout tables")
        for table in tables:
            print(f"   * {table}")
    else:
        print("Database not found")
    
    reports = get_existing_reports()
    print(f"\nAnalysis reports: {len(reports)} files")
    if reports:
        for report in reports[:5]:
            print(f"   * checkout_{report}_report.txt")
        if len(reports) > 5:
            print(f"   ... and {len(reports) - 5} more")
    
    dashboards = get_existing_dashboards()
    print(f"\nDashboards: {len(dashboards)} files")
    if dashboards:
        for dashboard in dashboards[:5]:
            print(f"   * checkout_{dashboard}_dashboard.png")
        if len(dashboards) > 5:
            print(f"   ... and {len(dashboards) - 5} more")
    
    csv_dir = "./outputs/exports"
    if os.path.exists(csv_dir):
        csv_files = [f for f in os.listdir(csv_dir) if f.endswith('.csv')]
        if csv_files:
            print(f"\nCSV exports: {len(csv_files)} files")
            for csv_file in csv_files[:3]:
                print(f"   * {csv_file}")
            if len(csv_files) > 3:
                print(f"   ... and {len(csv_files) - 3} more")
    
    print("\n" + "=" * 70)


def main():
    parser = argparse.ArgumentParser(
        description="POS Sales Monitoring Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python pipeline.py                    # Run full pipeline (skip existing)
  python pipeline.py --force            # Force recreate all files
  python pipeline.py --threshold 0.25   # Custom sensitivity
  python pipeline.py --export           # Export CSV data
  python pipeline.py --checkout-dir data  # Custom directory
  python pipeline.py --ingestion-only   # Run only data ingestion
  python pipeline.py --analysis-only    # Run only analysis
        """
    )
    
    parser.add_argument("--checkout-dir", default="./data/raw", 
                       help="Directory containing checkout CSV files")
    parser.add_argument("--threshold", type=float, default=0.30, 
                       help="Anomaly detection sensitivity")
    parser.add_argument("--export", action="store_true", 
                       help="Export CSV data")
    parser.add_argument("--force", action="store_true", 
                       help="Force recreate all reports and dashboards")
    parser.add_argument("--ingestion-only", action="store_true", 
                       help="Run only data ingestion")
    parser.add_argument("--analysis-only", action="store_true", 
                       help="Run only anomaly analysis")
    
    args = parser.parse_args()
    
    print("=" * 70)
    print("POS SALES MONITORING PIPELINE")
    print("=" * 70)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Data directory: {args.checkout_dir}")
    print(f"Sensitivity: {args.threshold}")
    print(f"CSV export: {'Yes' if args.export else 'No'}")
    print(f"Skip existing: {'No' if args.force else 'Yes'}")
    print("=" * 70)
    
    os.makedirs("./outputs/database", exist_ok=True)
    os.makedirs("./outputs/reports", exist_ok=True)
    os.makedirs("./outputs/visualizations", exist_ok=True)
    os.makedirs("./outputs/exports", exist_ok=True)
    os.makedirs(args.checkout_dir, exist_ok=True)
    
    total_start_time = time.time()
    
    if args.ingestion_only:
        success = run_ingestion(args.checkout_dir)
        if not success:
            print("\nPIPELINE FAILED: Ingestion failed")
            sys.exit(1)
            
    elif args.analysis_only:
        success = run_analysis(args.threshold, args.export, not args.force)
        if not success:
            print("\nPIPELINE FAILED: Analysis failed")
            sys.exit(1)
            
    else:
        ingestion_success = run_ingestion(args.checkout_dir)
        if not ingestion_success:
            print("\nPIPELINE FAILED: Ingestion failed")
            sys.exit(1)
        
        time.sleep(1)
        
        analysis_success = run_analysis(args.threshold, args.export, not args.force)
        if not analysis_success:
            print("\nPIPELINE FAILED: Analysis failed")
            sys.exit(1)
    
    total_elapsed_time = time.time() - total_start_time
    
    show_summary()
    
    print("\n" + "=" * 70)
    print("PIPELINE COMPLETED SUCCESSFULLY")
    print("=" * 70)
    print(f"Total time: {total_elapsed_time:.1f} seconds")
    print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("\nOutput directories:")
    print("   * ./outputs/database/     - SQLite database")
    print("   * ./outputs/reports/      - Text analysis reports")
    print("   * ./outputs/visualizations/ - Dashboard PNG files")
    if args.export:
        print("   * ./outputs/exports/     - CSV data exports")
    print("=" * 70)


if __name__ == "__main__":
    main()