# scripts/etl_events_full.py
"""
Full ETL - Initial Load
Loads ALL historical events from order_logs to ClickHouse
Run this ONCE before starting incremental sync
"""

import pandas as pd
import mysql.connector
import clickhouse_connect
import sys
import os
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import MYSQL_CONFIG, CH_CONFIG, EVENT_TYPE_MAPPING

def log(message):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp}] {message}", flush=True)

def extract_all_events():
    log("Connecting to MySQL...")
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    
    # Get total count
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM order_logs")
    total_count = cursor.fetchone()[0]
    log(f"✓ Total events to sync: {total_count:,}")
    cursor.close()
    
    # Extract in batches
    batch_size = 1000
    all_data = []
    
    for offset in range(0, total_count, batch_size):
        log(f"  → Extracting batch: {offset+1} to {min(offset+batch_size, total_count)}")
        
        query = f"""
        SELECT 
            ol.order_log_id,
            ol.order_id,
            ol.order_status_id,
            ol.created_at as created_at_log,
            o.*
        FROM order_logs ol
        INNER JOIN orders o ON ol.order_id = o.order_id
        ORDER BY ol.order_log_id
        LIMIT {batch_size} OFFSET {offset}
        """
        
        df = pd.read_sql(query, conn)
        all_data.append(df)
    
    conn.close()
    
    # Combine all batches
    final_df = pd.concat(all_data, ignore_index=True)
    log(f"✓ Extracted {len(final_df):,} total events")
    
    return final_df

def transform_events(df):
    log("Transforming data...")
    
    # Rename
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
    
    # Datetimes
    for col in ['event_timestamp', 'created_at', 'updated_at']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    log(f"✓ Transformed - shape: {df.shape}")
    return df

def load_events_batch(df, ch_client):
    log(f"Loading {len(df):,} events in batches...")
    
    batch_size = 1000
    for i in range(0, len(df), batch_size):
        batch = df.iloc[i:i+batch_size]
        ch_client.insert_df('events_data', batch)
        log(f"  ✓ Loaded batch {i//batch_size + 1}: rows {i+1}-{min(i+batch_size, len(df))}")
    
    log("✓ All batches loaded!")

def main():
    log("="*70)
    log("EVENTS ETL - FULL INITIAL LOAD")
    log("="*70)
    
    try:
        # Connect
        log("\n[1/4] Connecting to ClickHouse...")
        ch_client = clickhouse_connect.get_client(**CH_CONFIG)
        log("✓ Connected")
        
        # Clear existing data
        log("\n[2/4] Clearing existing data...")
        ch_client.command("TRUNCATE TABLE events_data")
        log("✓ Table truncated")
        
        # Extract
        log("\n[3/4] Extracting from MySQL...")
        df = extract_all_events()
        
        # Transform
        df = transform_events(df)
        
        # Load
        log("\n[4/4] Loading to ClickHouse...")
        load_events_batch(df, ch_client)
        
        # Verify
        total = ch_client.command("SELECT COUNT(*) FROM events_data")
        max_id = ch_client.command("SELECT MAX(event_id) FROM events_data")
        
        log("\n" + "="*70)
        log(f"✓ FULL LOAD COMPLETED!")
        log(f"  Total events: {total:,}")
        log(f"  Max event_id: {max_id}")
        log("="*70)
        
        # Save last ID for incremental sync
        tracking_file = 'logs/last_sync_id.txt'
        with open(tracking_file, 'w') as f:
            f.write(str(max_id))
        log(f"✓ Saved last_sync_id: {max_id}")
        
        return 0
        
    except Exception as e:
        log(f"\n✗ FAILED: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
