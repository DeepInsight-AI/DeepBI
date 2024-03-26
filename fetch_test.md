source venv/bin/activate
./bin/run ./manage.py runserver -h0.0.0.0  -p 8338 >flask.log 2>&1 &
./bin/run ./manage.py rq scheduler >rq_sc.log 2>&1 &
./bin/run ./manage.py rq worker  >rqwk.log 2>&1 &
./bin/run ./manage.py run_ai >run_ai.log 2>&1 &
./bin/run ./manage.py run_ai_api >run_ai_api.log 2>&1 &
python ai_chat_api.py>./log/chat_api.log 2>&1 &