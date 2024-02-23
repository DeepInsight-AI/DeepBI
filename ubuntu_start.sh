#!/bin/bash
if [ ! -d "log" ]; then
    mkdir log
fi

source venv/bin/activate
./bin/run ./manage.py runserver -h0.0.0.0  -p 8338 >./log/web.log 2>&1 &
echo $! >./user_upload_files/.web.pid.txt
./bin/run ./manage.py rq scheduler >./log/scheduler.log 2>&1 &
echo $! >./user_upload_files/.scheduler.pid.txt
./bin/run ./manage.py rq worker  >./log/worker.log 2>&1 &
echo $! >./user_upload_files/.worker.pid.txt
./bin/run ./manage.py run_ai  >./log/ai.log 2>&1 &
echo $! >./user_upload_files/.ai.pid.txt
./bin/run ./manage.py run_ai_api  >./log/run_ai_api.log 2>&1 &
echo $! >./user_upload_files/.run_ai_api.pid.txt
