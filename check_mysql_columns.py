#!/usr/bin/env python3
# check_mysql_columns.py
"""
Check which columns exist in MySQL orders and order_logs tables
"""

import mysql.connector
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import MYSQL_CONFIG

def get_table_columns(table_name):
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = conn.cursor()
    
    query = f"DESCRIBE {table_name}"
    cursor.execute(query)
    
    columns = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    
    return columns

def main():
    print("="*70)
    print("MySQL Table Column Discovery")
    print("="*70)
    
    # Get columns from order_logs
    print("\n[1/2] Checking order_logs table...")
    order_logs_cols = get_table_columns('order_logs')
    print(f"✓ Found {len(order_logs_cols)} columns in order_logs:")
    for col in order_logs_cols:
        print(f"  - {col}")
    
    # Get columns from orders
    print(f"\n[2/2] Checking orders table...")
    orders_cols = get_table_columns('orders')
    print(f"✓ Found {len(orders_cols)} columns in orders:")
    for col in orders_cols:
        print(f"  - {col}")
    
    print("\n" + "="*70)
    print("Copy these column names to update your ETL script")
    print("="*70)

if __name__ == "__main__":
    main()