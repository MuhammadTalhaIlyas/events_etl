# config/config.py
"""
Configuration file for Events ETL System
MySQL and ClickHouse connection settings
"""

# MySQL Configuration
MYSQL_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '',  # Update if you have a password
    'database': 'info_db'
}

# ClickHouse Configuration
CH_CONFIG = {
    'host': 'localhost',
    'port': 8123,
    'username': 'default',
    'password': '',  # Update if you have a password
    'database': 'main_data'
}

# ETL Configuration
ETL_CONFIG = {
    'batch_size': 500,  # Process 500 events at a time
    'sync_interval': 300,  # 5 minutes in seconds
    'tracking_file': 'logs/last_sync_id.txt'  # Store last synced order_log_id
}

# Event Type Mapping: order_status_id → event_type
EVENT_TYPE_MAPPING = {
    2: 2,   # Confirmed
    3: 3,   # Shipped
    4: 4,   # Delivered
    5: 5,   # Placed
    6: 6,   # Canceled
    7: 7,   # On Hold
    8: 8,   # On Hold by Sales
    9: 9,   # No Answer
    10: 10, # On Hold Refund
    11: 11, # Payment Refund
    12: 12, # Payment Pending
    14: 14  # Address Update Pending
}