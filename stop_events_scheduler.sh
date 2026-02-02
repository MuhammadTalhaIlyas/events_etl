#!/bin/bash
# stop_events_scheduler.sh

cd ~/Desktop/events_etl

if [ -f scheduler.pid ]; then
    PID=$(cat scheduler.pid)
    if ps -p $PID > /dev/null; then
        kill $PID
        echo "Scheduler stopped (PID: $PID)"
        rm scheduler.pid
    else
        echo "Scheduler not running"
        rm scheduler.pid
    fi
else
    echo "No PID file found"
    pkill -f "python3 scheduler_events.py"
fi
