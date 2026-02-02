# scripts/etl_events_main.py
"""
Main Incremental ETL for Events Data
Fetches from order_logs + orders → syncs to ClickHouse events_data
"""

import pandas as pd
import mysql.connector
import clickhouse_connect
import sys
import os
from datetime import datetime

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import MYSQL_CONFIG, CH_CONFIG, ETL_CONFIG, EVENT_TYPE_MAPPING

def log(message):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp}] {message}", flush=True)

def get_last_synced_id():
    tracking_file = ETL_CONFIG['tracking_file']
    try:
        if os.path.exists(tracking_file):
            with open(tracking_file, 'r') as f:
                last_id = int(f.read().strip())
                log(f"✓ Last synced order_log_id: {last_id}")
                return last_id
        else:
            log("✓ No previous sync - starting from beginning")
            return 0
    except:
        return 0

def save_last_synced_id(last_id):
    tracking_file = ETL_CONFIG['tracking_file']
    with open(tracking_file, 'w') as f:
        f.write(str(last_id))
    log(f"✓ Saved last synced ID: {last_id}")

def extract_events(last_synced_id):
    log("Connecting to MySQL...")
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    batch_size = ETL_CONFIG['batch_size']
    
    query = f"""
    SELECT 
        ol.order_log_id,
        ol.order_id,
        ol.order_status_id,
        ol.created_at as created_at_log,
        o.*
    FROM order_logs ol
    INNER JOIN orders o ON ol.order_id = o.order_id
    WHERE ol.order_log_id > {last_synced_id}
    ORDER BY ol.order_log_id
    LIMIT {batch_size}
    """
    
    log(f"✓ Querying order_logs > {last_synced_id} (batch: {batch_size})")
    df = pd.read_sql(query, conn)
    conn.close()
    
    log(f"✓ Extracted {len(df)} events")
    return df

def transform_events(df):
    if df.empty:
        return df
    
    log("Transforming...")
    
    # Rename key columns
    df = df.rename(columns={
        'order_log_id': 'event_id',
        'created_at_log': 'event_timestamp'
    })
    
    # Add event_type
    df['event_type'] = df['order_status_id'].map(EVENT_TYPE_MAPPING)
    
    # Fill NULLs
    for col in df.select_dtypes(include=['number']).columns:
        df[col] = df[col].fillna(0)
    
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].fillna('')
    
    # Convert datetimes
    for col in ['event_timestamp', 'created_at', 'updated_at']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    log(f"✓ Transformed - shape: {df.shape}")
    return df

def load_events(df, ch_client):
    if df.empty:
        return 0
    
    log(f"Loading {len(df)} events to ClickHouse...")
    ch_client.insert_df('events_data', df)
    
    max_id = df['event_id'].max()
    log(f"✓ Loaded - max event_id: {max_id}")
    return max_id

def main():
    log("="*70)
    log("EVENTS ETL - INCREMENTAL")
    log("="*70)
    
    try:
        # Connect
        log("\n[1/5] Connecting...")
        ch_client = clickhouse_connect.get_client(**CH_CONFIG)
        log("✓ Connected")
        
        # Get last ID
        log("\n[2/5] Checking last sync...")
        last_id = get_last_synced_id()
        
        # Extract
        log("\n[3/5] Extracting...")
        df = extract_events(last_id)
        
        if df.empty:
            log("\n✓ No new events - up to date!")
            return 0
        
        # Transform
        log("\n[4/5] Transforming...")
        df = transform_events(df)
        
        # Load
        log("\n[5/5] Loading...")
        max_id = load_events(df, ch_client)
        
        # Save
        save_last_synced_id(max_id)
        
        total = ch_client.command("SELECT COUNT(*) FROM events_data")
        log(f"\n✓ Total events in ClickHouse: {total:,}")
        log(f"✓ COMPLETED - Synced {len(df)} events")
        
        return 0
    except Exception as e:
        log(f"\n✗ FAILED: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
