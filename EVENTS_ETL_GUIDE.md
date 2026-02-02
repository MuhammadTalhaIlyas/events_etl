# EVENTS ETL - COMPLETE SETUP GUIDE

## What This System Does

Automatically syncs order events from MySQL to ClickHouse every 5 minutes:
- **MySQL**: `order_logs` table (each status change = 1 event) + `orders` table (order details)
- **ClickHouse**: `main_data.events_data` table
- **Tracking**: Uses `order_log_id` to track progress
- **Batch Size**: 500 events per sync

---

## QUICK START (Copy-Paste Commands)

```bash
# 1. Create project
cd ~/Desktop
mkdir events_etl && cd events_etl
mkdir -p config scripts logs

# 2. Setup Python
python3 -m venv venv
source venv/bin/activate
pip install pandas mysql-connector-python clickhouse-connect

# 3. Copy all files (provided separately)
# config/config.py
# scripts/etl_events_full.py
# scripts/etl_events_main.py
# scheduler_events.py
# start_events_scheduler.sh
# stop_events_scheduler.sh

# 4. Make scripts executable
chmod +x start_events_scheduler.sh stop_events_scheduler.sh

# 5. Run initial load (ONE TIME)
python3 scripts/etl_events_full.py

# 6. Start automatic scheduler
./start_events_scheduler.sh

# 7. Monitor
tail -f logs/scheduler.log
```

---

## DETAILED SETUP

### 1. System Requirements
- macOS
- Python 3.8+
- MySQL running on localhost
- ClickHouse running on localhost
- Access to `info_db` database (MySQL)
- Access to `main_data` database (ClickHouse)

### 2. Project Structure

```
~/Desktop/events_etl/
├── venv/                           # Python virtual environment
├── config/
│   └── config.py                   # Connection settings
├── scripts/
│   ├── etl_events_full.py         # Initial full load
│   └── etl_events_main.py         # Incremental sync
├── logs/
│   ├── scheduler.log              # Scheduler activity
│   ├── last_sync_id.txt           # Last synced order_log_id
│   └── errors.log                 # Error logs
├── scheduler_events.py             # 5-minute scheduler
├── start_events_scheduler.sh      # Start script
├── stop_events_scheduler.sh       # Stop script
└── scheduler.pid                   # Process ID (auto-created)
```

### 3. Configuration

**config/config.py** - Update these values:

```python
MYSQL_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '',  # ← YOUR MYSQL PASSWORD
    'database': 'info_db'
}

CH_CONFIG = {
    'host': 'localhost',
    'port': 8123,
    'username': 'default',
    'password': '',  # ← YOUR CLICKHOUSE PASSWORD
    'database': 'main_data'
}

ETL_CONFIG = {
    'batch_size': 500,  # Process 500 events per sync
    'sync_interval': 300,  # 5 minutes
    'tracking_file': 'logs/last_sync_id.txt'
}
```

### 4. Initial Setup Commands

```bash
# Navigate to Desktop
cd ~/Desktop

# Create project directory
mkdir events_etl
cd events_etl

# Create subdirectories
mkdir -p config scripts logs

# Create Python virtual environment
python3 -m venv venv

# Activate virtual environment
source venv/bin/activate

# Install libraries
pip install --upgrade pip
pip install pandas
pip install mysql-connector-python
pip install clickhouse-connect

# Verify installations
python3 << EOF
import pandas
import mysql.connector
import clickhouse_connect
print("✓ All libraries installed successfully!")
EOF
```

### 5. Copy Files

Copy all provided files to their locations:
- `config/config.py`
- `scripts/etl_events_full.py`
- `scripts/etl_events_main.py`
- `scheduler_events.py`
- `start_events_scheduler.sh`
- `stop_events_scheduler.sh`

```bash
# Make scripts executable
chmod +x start_events_scheduler.sh
chmod +x stop_events_scheduler.sh
```

### 6. Test Connections

```bash
# Activate environment
source venv/bin/activate

# Test MySQL
python3 << EOF
import mysql.connector
from config.config import MYSQL_CONFIG
try:
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    print("✓ MySQL connection successful!")
    print(f"  Database: {MYSQL_CONFIG['database']}")
    conn.close()
except Exception as e:
    print(f"✗ MySQL connection failed: {e}")
EOF

# Test ClickHouse
python3 << EOF
import clickhouse_connect
from config.config import CH_CONFIG
try:
    client = clickhouse_connect.get_client(**CH_CONFIG)
    print("✓ ClickHouse connection successful!")
    print(f"  Database: {CH_CONFIG['database']}")
    tables = client.command("SHOW TABLES")
    print(f"  Tables: {tables}")
except Exception as e:
    print(f"✗ ClickHouse connection failed: {e}")
EOF
```

### 7. Run Initial Full Load

**IMPORTANT**: Run this ONCE to load all historical data:

```bash
python3 scripts/etl_events_full.py
```

Expected output:
```
======================================================================
EVENTS ETL - FULL INITIAL LOAD
======================================================================

[1/4] Connecting to ClickHouse...
✓ Connected

[2/4] Clearing existing data...
✓ Table truncated

[3/4] Extracting from MySQL...
✓ Total events to sync: 234,567
  → Extracting batch: 1 to 1000
  → Extracting batch: 1001 to 2000
  ...

[4/4] Loading to ClickHouse...
  ✓ Loaded batch 1: rows 1-1000
  ...

======================================================================
✓ FULL LOAD COMPLETED!
  Total events: 234,567
  Max event_id: 6340134
======================================================================
```

### 8. Verify Initial Load

```bash
# Check ClickHouse
clickhouse-client --query "SELECT COUNT(*) as total_events FROM main_data.events_data"

# Check sample data
clickhouse-client --query "SELECT event_id, order_id, event_type, event_timestamp FROM main_data.events_data ORDER BY event_id DESC LIMIT 10 FORMAT Pretty"

# Compare with MySQL
sudo mysql -e "USE info_db; SELECT COUNT(*) as total_logs FROM order_logs;"
```

Numbers should match!

### 9. Test Incremental Sync

```bash
# Run incremental sync manually
python3 scripts/etl_events_main.py
```

Should show: "No new events - up to date!"

### 10. Insert Test Event

```bash
# Insert a test event in MySQL
sudo mysql << EOF
USE info_db;
INSERT INTO order_logs (order_id, order_status_id, created_at)
VALUES (2567889, 5, NOW());
EOF

# Run sync again
python3 scripts/etl_events_main.py
```

Should show: "Synced 1 events"

### 11. Start Automatic Scheduler

```bash
./start_events_scheduler.sh
```

Output:
```
Scheduler started! PID: 12345
Monitor: tail -f logs/scheduler.log
```

### 12. Monitor the System

```bash
# Watch scheduler activity
tail -f logs/scheduler.log

# Check if scheduler is running
ps aux | grep scheduler_events.py

# Check last synced ID
cat logs/last_sync_id.txt
```

---

## DAILY OPERATIONS

### Start the Scheduler
```bash
cd ~/Desktop/events_etl
./start_events_scheduler.sh
```

### Stop the Scheduler
```bash
./stop_events_scheduler.sh
```

### Check Status
```bash
# Is it running?
ps aux | grep scheduler_events.py

# View recent activity
tail -30 logs/scheduler.log

# View last sync
cat logs/last_sync_id.txt
```

### Manual Sync
```bash
source venv/bin/activate
python3 scripts/etl_events_main.py
```

### View Logs
```bash
# Live monitoring
tail -f logs/scheduler.log

# Recent entries
tail -100 logs/scheduler.log

# Search for errors
grep "FAILED" logs/scheduler.log

# Today's syncs
grep "$(date +%Y-%m-%d)" logs/scheduler.log
```

---

## VERIFICATION & MONITORING

### Check Sync Status
```bash
echo "=== MySQL Order Logs Count ==="
sudo mysql -e "USE info_db; SELECT COUNT(*) FROM order_logs;" | tail -1

echo "=== ClickHouse Events Count ==="
clickhouse-client --query "SELECT COUNT(*) FROM main_data.events_data"

echo "=== Last Synced ID ==="
cat logs/last_sync_id.txt

echo "=== Latest Events in ClickHouse ==="
clickhouse-client --query "SELECT event_id, order_id, event_type FROM main_data.events_data ORDER BY event_id DESC LIMIT 5 FORMAT Pretty"
```

### Event Types Distribution
```bash
clickhouse-client --query "
SELECT 
    event_type,
    COUNT(*) as count,
    MIN(event_timestamp) as first_event,
    MAX(event_timestamp) as last_event
FROM main_data.events_data
GROUP BY event_type
ORDER BY event_type
FORMAT Pretty"
```

### Daily Events
```bash
clickhouse-client --query "
SELECT 
    toDate(event_timestamp) as date,
    COUNT(*) as events
FROM main_data.events_data
WHERE event_timestamp >= today() - 7
GROUP BY date
ORDER BY date DESC
FORMAT Pretty"
```

---

## TROUBLESHOOTING

### Scheduler Not Running
```bash
# Check process
ps aux | grep scheduler

# If not found, start it
./start_events_scheduler.sh

# View errors
tail -50 logs/scheduler.log
```

### No New Events Syncing
```bash
# 1. Check last sync ID
cat logs/last_sync_id.txt

# 2. Check MySQL for newer events
sudo mysql -e "USE info_db; SELECT MAX(order_log_id) FROM order_logs;" | tail -1

# 3. Manual sync with debug
python3 scripts/etl_events_main.py

# 4. If stuck, reset tracking (CAUTION: may cause duplicates)
echo "0" > logs/last_sync_id.txt
python3 scripts/etl_events_main.py
```

### Connection Errors
```bash
# MySQL
sudo mysql -e "SELECT 1"

# ClickHouse
clickhouse-client --query "SELECT 1"

# Update passwords in config/config.py if needed
```

### Library Errors
```bash
source venv/bin/activate
pip install --upgrade pandas mysql-connector-python clickhouse-connect
```

---

## SUCCESS CHECKLIST

✅ Virtual environment created
✅ Libraries installed
✅ Configuration updated
✅ MySQL connection works
✅ ClickHouse connection works
✅ Initial full load completed
✅ MySQL count = ClickHouse count
✅ Incremental sync works
✅ Test event synced successfully
✅ Scheduler started
✅ Logs showing regular syncs every 5 minutes

---

## NEXT STEPS

1. **Monitor for 24 hours** - Ensure scheduler runs reliably
2. **Set up alerting** - Email/Slack when sync fails
3. **Create dashboards** - Visualize event data in ClickHouse
4. **Add more tables** - Extend to order_items, payments, etc.
5. **Build analytics** - Revenue analysis, customer behavior, etc.

---

**Your Events ETL System is Ready!** 🚀
