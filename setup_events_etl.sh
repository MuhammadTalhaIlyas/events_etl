#!/bin/bash
# setup_events_etl.sh
# Complete setup script for Events ETL system

echo "========================================"
echo "Events ETL System - Complete Setup"
echo "========================================"

# Step 1: Create project directory
echo ""
echo "[1/8] Creating project directory..."
cd ~/Desktop
mkdir -p events_etl
cd events_etl

# Step 2: Create directory structure
echo "[2/8] Creating directory structure..."
mkdir -p config
mkdir -p scripts
mkdir -p logs
mkdir -p sql

# Step 3: Create Python virtual environment
echo "[3/8] Creating Python virtual environment..."
python3 -m venv venv

# Step 4: Activate virtual environment
echo "[4/8] Activating virtual environment..."
source venv/bin/activate

# Step 5: Install required Python libraries
echo "[5/8] Installing Python libraries..."
pip install --upgrade pip
pip install pandas
pip install mysql-connector-python
pip install clickhouse-connect

# Step 6: Verify installations
echo "[6/8] Verifying installations..."
python3 -c "import pandas; print('✓ pandas installed')"
python3 -c "import mysql.connector; print('✓ mysql-connector-python installed')"
python3 -c "import clickhouse_connect; print('✓ clickhouse-connect installed')"

# Step 7: Create empty log files
echo "[7/8] Creating log files..."
touch logs/etl_events.log
touch logs/scheduler.log
touch logs/errors.log

# Step 8: Summary
echo "[8/8] Setup complete!"
echo ""
echo "========================================"
echo "Project Structure Created:"
echo "========================================"
echo "events_etl/"
echo "├── venv/              (Python virtual environment)"
echo "├── config/            (Configuration files)"
echo "├── scripts/           (ETL scripts)"
echo "├── logs/              (Log files)"
echo "└── sql/               (SQL files)"
echo ""
echo "Next steps:"
echo "1. cd ~/Desktop/events_etl"
echo "2. source venv/bin/activate"
echo "3. Create configuration and ETL scripts"
echo "========================================"
