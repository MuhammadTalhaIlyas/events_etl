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

# Column Mapping: MySQL → ClickHouse
# Maps MySQL column names to ClickHouse column names
COLUMN_MAPPING = {
    # From order_logs
    'order_log_id': 'event_id',
    'order_id': 'order_id',
    'order_status_id': 'order_status_id',
    'created_at_log': 'event_timestamp',
    
    # From orders
    'order_number': 'order_number',
    'order_type_id': 'order_type_id',
    'customer_id': 'customer_id',
    'segment_id': 'segment_id',
    'customer_type': 'customer_type',
    'is_first_order': 'is_first_order',
    'is_favourite_order': 'is_favourite_order',
    'new_customer_foc': 'new_customer_foc',
    'grand_total': 'grand_total',
    'vat': 'vat',
    'delivery_fee': 'delivery_fee',
    'discount': 'discount',
    'wallet_amount': 'wallet_amount',
    'wallet_discount': 'wallet_discount',
    'wallet_cashback': 'wallet_cashback',
    'is_cash_back': 'is_cash_back',
    'invoice_amount': 'invoice_amount',
    'payment_method_id': 'payment_method_id',
    'promocode_id': 'promocode_id',
    'promotion_id': 'promotion_id',
    'coupon_quantity': 'coupon_quantity',
    'reward': 'reward',
    'qitaf_rewardpoints': 'qitaf_rewardpoints',
    'delivery_date': 'delivery_date',
    'delivery_time': 'delivery_time',
    'delivery_type': 'delivery_type',
    'delivered_quantity': 'delivered_quantity',
    'country_id': 'country_id',
    'city_id': 'city_id',
    'area_id': 'area_id',
    'store_id': 'store_id',
    'sale_office_id': 'sale_office_id',
    'route_id': 'route_id',
    'agent_id': 'agent_id',
    'address_id': 'address_id',
    'source_id': 'source_id',
    'channel_id': 'channel_id',
    'sub_channel_id': 'sub_channel_id',
    'device_type': 'device_type',
    'app_version': 'app_version',
    'total_items_quantity': 'total_items_quantity',
    'total_unique_item_count': 'total_unique_item_count',
    'gift_item_quantity': 'gift_item_quantity',
    'foc_item_quantity': 'foc_item_quantity',
    'is_recurring': 'is_recurring',
    'is_split_order': 'is_split_order',
    'corporate_invoice': 'corporate_invoice',
    'loyalty_programs': 'loyalty_programs',
    'is_bfm_customer': 'is_bfm_customer',
    'order_customer_bfm_club_id': 'order_customer_bfm_club_id',
    'is_stc_tayamouz_customer': 'is_stc_tayamouz_customer',
    'fulfilment_id': 'fulfilment_id',
    'invoice_date': 'invoice_date',
    'created_at': 'created_at',
    'updated_at': 'updated_at'
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
