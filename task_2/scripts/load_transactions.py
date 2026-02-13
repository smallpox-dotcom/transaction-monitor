import sqlite3
import pandas as pd
import argparse
import os

def create_database(db_path):
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    return conn

def load_transactions(file_path, conn):
    try:
        df = pd.read_csv(file_path)
        print(f"\nLoading {os.path.basename(file_path)}")
        print(f"   Shape: {df.shape}")
        
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df.to_sql('transactions', conn, if_exists='replace', index=False)
        print(f"   Loaded {len(df)} records")
        return True
    except Exception as e:
        print(f"   Error: {e}")
        return False

def load_auth_codes(file_path, conn):
    try:
        df = pd.read_csv(file_path)
        print(f"\nLoading {os.path.basename(file_path)}")
        print(f"   Shape: {df.shape}")
        
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df.to_sql('auth_codes', conn, if_exists='replace', index=False)
        print(f"   Loaded {len(df)} records")
        return True
    except Exception as e:
        print(f"   Error: {e}")
        return False

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default="data/raw")
    parser.add_argument("--db", default="data/processed/transactions.db")
    args = parser.parse_args()
    
    conn = create_database(args.db)
    
    trans_file = os.path.join(args.data_dir, "transactions.csv")
    if os.path.exists(trans_file):
        load_transactions(trans_file, conn)
    
    auth_file = os.path.join(args.data_dir, "transactions_auth_codes.csv")
    if os.path.exists(auth_file):
        load_auth_codes(auth_file, conn)
    
    conn.close()
    print("\nDatabase created successfully")

if __name__ == "__main__":
    main()