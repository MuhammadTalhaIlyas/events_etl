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

def get_mysql_columns(conn, table_name):
    """Get list of columns from MySQL table"""
    cursor = conn.cursor()
    cursor.execute(f"DESCRIBE {table_name}")
    columns = [row[0] for row in cursor.fetchall()]
    cursor.close()
    return columns

def get_clickhouse_columns(ch_client):
    """Get list of columns from ClickHouse events_data table"""
    query = "DESCRIBE TABLE events_data"
    result = ch_client.query(query)
    columns = [row[0] for row in result.result_rows]
    return columns

def build_select_query(orders_cols, last_synced_id, batch_size):
    """
    Build SELECT query with only columns that exist in both MySQL and ClickHouse
    """
    
    # Required columns from order_logs
    select_parts = [
        "ol.order_log_id",
        "ol.order_id",
        "ol.order_status_id",
        "ol.created_at as created_at_log"
    ]
    
    # Target columns for ClickHouse (excluding the ones we already have from order_logs)
    target_cols = [
        'order_number', 'order_type_id', 'customer_id', 'segment_id',
        'customer_type', 'is_first_order', 'is_favourite_order', 'new_customer_foc',
        'grand_total', 'vat', 'delivery_fee', 'discount',
        'wallet_amount', 'wallet_discount', 'wallet_cashback', 'is_cash_back',
        'invoice_amount', 'payment_method_id', 'promocode_id', 'promotion_id',
        'coupon_quantity', 'reward', 'qitaf_rewardpoints',
        'delivery_date', 'delivery_time', 'delivery_type', 'delivered_quantity',
        'country_id', 'city_id', 'area_id', 'store_id', 'sale_office_id',
        'route_id', 'agent_id', 'address_id', 'source_id', 'channel_id',
        'sub_channel_id', 'device_type', 'app_version',
        'total_items_quantity', 'total_unique_item_count',
        'gift_item_quantity', 'foc_item_quantity',
        'is_recurring', 'is_split_order', 'corporate_invoice', 'loyalty_programs',
        'is_bfm_customer', 'order_customer_bfm_club_id', 'is_stc_tayamouz_customer',
        'fulfilment_id', 'invoice_date', 'created_at', 'updated_at'
    ]
    
    # Only select columns that exist in MySQL orders table
    for col in target_cols:
        if col in orders_cols:
            select_parts.append(f"o.{col}")
    
    select_clause = ",\n        ".join(select_parts)
    
    query = f"""
    SELECT 
        {select_clause}
    FROM order_logs ol
    INNER JOIN orders o ON ol.order_id = o.order_id
    WHERE ol.order_log_id > {last_synced_id}
    ORDER BY ol.order_log_id
    LIMIT {batch_size}
    """
    
    return query

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
    os.makedirs(os.path.dirname(tracking_file), exist_ok=True)
    with open(tracking_file, 'w') as f:
        f.write(str(last_id))
    log(f"✓ Saved last synced ID: {last_id}")

def extract_events(last_synced_id):
    log("Connecting to MySQL...")
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    batch_size = ETL_CONFIG['batch_size']
    
    # Get column information
    orders_cols = get_mysql_columns(conn, 'orders')
    
    # Build dynamic query
    query = build_select_query(orders_cols, last_synced_id, batch_size)
    
    log(f"✓ Querying order_logs > {last_synced_id} (batch: {batch_size})")
    df = pd.read_sql(query, conn)
    conn.close()
    
    log(f"✓ Extracted {len(df)} events")
    return df

def transform_events(df, ch_cols):
    if df.empty:
        return df
    
    log("Transforming...")
    
    # Rename key columns
    df = df.rename(columns={
        'order_log_id': 'event_id',
        'created_at_log': 'event_timestamp'
    })
    
    # Add event_type
    df['event_type'] = df['order_status_id'].replace(EVENT_TYPE_MAPPING)
    
    # Add missing columns with default values
    for col in ch_cols:
        if col not in df.columns:
            # Determine default value based on column name
            if 'id' in col.lower() or 'quantity' in col.lower():
                df[col] = 0
            elif 'date' in col.lower() or 'time' in col.lower():
                df[col] = pd.NaT
            elif any(x in col.lower() for x in ['total', 'amount', 'fee', 'discount', 'reward', 'vat']):
                df[col] = 0.0
            elif col.lower().startswith('is_'):
                df[col] = 0
            else:
                df[col] = ''
    
    # Convert columns that should be numeric to numeric type
    # These are columns that end with _id, quantity, or are boolean flags
    numeric_pattern_cols = [col for col in df.columns if 
                           col.endswith('_id') or 
                           'quantity' in col.lower() or 
                           col.startswith('is_') or
                           col in ['segment_id', 'new_customer_foc', 'coupon_quantity', 
                                   'delivered_quantity', 'app_version']]
    
    for col in numeric_pattern_cols:
        if col in df.columns:
            # Convert to numeric, replacing non-numeric values with 0
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('int64')
    
    # Convert monetary columns to float
    monetary_cols = [col for col in df.columns if any(x in col.lower() for x in 
                    ['total', 'amount', 'fee', 'discount', 'reward', 'vat', 'wallet', 'cashback', 'invoice'])]
    
    for col in monetary_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0).astype('float64')
    
    # Fill remaining NULLs for numeric columns
    for col in df.select_dtypes(include=['number']).columns:
        df[col] = df[col].fillna(0)
    
    # Fill NULLs for string columns (only true string columns, not IDs)
    for col in df.select_dtypes(include=['object', 'string']).columns:
        if not col.endswith('_id') and 'quantity' not in col.lower():
            df[col] = df[col].fillna('')
    
    # Convert datetimes
    for col in ['event_timestamp', 'created_at', 'updated_at']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # Convert delivery_date to date (with format to suppress warning)
    if 'delivery_date' in df.columns:
        df['delivery_date'] = pd.to_datetime(df['delivery_date'], format='mixed', errors='coerce')
    
    # Convert delivery_time to datetime (ClickHouse expects DateTime, not Time)
    if 'delivery_time' in df.columns:
        df['delivery_time'] = pd.to_datetime(df['delivery_time'], format='mixed', errors='coerce')
    
    # Convert invoice_date to date
    if 'invoice_date' in df.columns:
        df['invoice_date'] = pd.to_datetime(df['invoice_date'], format='mixed', errors='coerce')
    
    # Reorder columns to match ClickHouse table
    df = df[[col for col in ch_cols if col in df.columns]]
    
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
        log("\n[1/6] Connecting...")
        ch_client = clickhouse_connect.get_client(**CH_CONFIG)
        log("✓ Connected")
        
        # Get ClickHouse columns
        log("\n[2/6] Getting ClickHouse table schema...")
        ch_cols = get_clickhouse_columns(ch_client)
        log(f"✓ ClickHouse table has {len(ch_cols)} columns")
        
        # Get last ID
        log("\n[3/6] Checking last sync...")
        last_id = get_last_synced_id()
        
        # Extract
        log("\n[4/6] Extracting...")
        df = extract_events(last_id)
        
        if df.empty:
            log("\n✓ No new events - up to date!")
            return 0
        
        # Transform
        log("\n[5/6] Transforming...")
        df = transform_events(df, ch_cols)
        
        # Load
        log("\n[6/6] Loading...")
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