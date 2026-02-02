#!/bin/bash
# start_events_scheduler.sh

cd ~/Desktop/events_etl
source venv/bin/activate
nohup python3 scheduler_events.py >> logs/scheduler.log 2>&1 &
echo $! > scheduler.pid
echo "Scheduler started! PID: $(cat scheduler.pid)"
echo "Monitor: tail -f logs/scheduler.log"
