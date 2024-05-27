#!/bin/bash
if [ ! -d "log" ]; then
    mkdir log
fi

pid=$(cat ./user_upload_files/.web.pid.txt)
kill $pid
pid=$(cat ./user_upload_files/.scheduler.pid.txt)
kill $pid
pid=$(cat ./user_upload_files/.worker.pid.txt)
kill $pid
pid=$(cat ./user_upload_files/.ai.pid.txt)
kill $pid
pid=$(cat ./user_upload_files/.run_ai_api.pid.txt)
kill $pid
