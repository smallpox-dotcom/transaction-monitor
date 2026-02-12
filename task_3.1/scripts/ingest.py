import sqlite3
import pandas as pd
import argparse
import os
import glob


def create_database(db_path):
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    return conn


def table_exists(table_name, conn):
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
    return cursor.fetchone() is not None


def load_csv_to_table(file_path, table_name, conn):
    try:
        df = pd.read_csv(file_path)
        
        if table_exists(table_name, conn):
            print(f"Skipping '{table_name}' (table exists)")
            return False
        else:
            df.to_sql(table_name, conn, if_exists="fail", index=False)
            print(f"Created '{table_name}' with {len(df)} records")
            return True
            
    except ValueError as e:
        if "Table" in str(e) and "already exists" in str(e):
            print(f"Table '{table_name}' exists (skipping)")
            return False
        else:
            print(f"Error: {e}")
            return False
    except Exception as e:
        print(f"Error: {e}")
        return False


def process_checkout_files(directory_path, conn):
    if not os.path.isdir(directory_path):
        print(f"Directory not found: {directory_path}")
        return 0, []
    
    csv_files = glob.glob(os.path.join(directory_path, "checkout_*.csv"))
    
    if not csv_files:
        print(f"No checkout_*.csv files found")
        return 0, []
    
    print(f"Found {len(csv_files)} file(s):")
    for csv_file in csv_files:
        print(f"  * {os.path.basename(csv_file)}")
    
    new_count = 0
    processed_files = []
    
    for csv_file in csv_files:
        filename = os.path.basename(csv_file)
        table_name = os.path.splitext(filename)[0]
        
        print(f"\nProcessing: {filename}")
        
        if load_csv_to_table(csv_file, table_name, conn):
            new_count += 1
            processed_files.append(filename)
    
    return new_count, processed_files


def list_tables(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    return [table[0] for table in cursor.fetchall()]


def main():
    parser = argparse.ArgumentParser(
        description="Load POS checkout data into SQLite database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python ingest.py                      # Process all checkout files
  python ingest.py --list               # List existing tables
  python ingest.py --checkout-dir data  # Custom directory
        """
    )
    
    parser.add_argument("--checkout-dir", default="../data/raw", help="CSV files directory")
    parser.add_argument("--list", action="store_true", help="List tables")
    parser.add_argument("--db", default="../outputs/database/monitor.db", help="Database path")
    
    args = parser.parse_args()
    
    conn = create_database(args.db)
    
    try:
        if args.list:
            tables = list_tables(conn)
            if tables:
                print("\nDatabase tables:")
                for table in tables:
                    cursor = conn.cursor()
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    count = cursor.fetchone()[0]
                    print(f"  * {table}: {count} records")
            else:
                print("No tables found")
            return
        
        print("=" * 40)
        print("DATA INGESTION")
        print("=" * 40)
        
        print(f"\nChecking: {args.checkout_dir}")
        
        new_count, new_files = process_checkout_files(args.checkout_dir, conn)
        
        if new_count > 0:
            print(f"\nAdded {new_count} new table(s):")
            for file in new_files:
                print(f"  * {file}")
        else:
            print(f"\nNo new tables added")
        
        tables = list_tables(conn)
        print(f"\nDatabase has {len(tables)} tables:")
        for table in tables:
            print(f"  * {table}")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()