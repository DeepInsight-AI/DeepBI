source venv/bin/activate
./bin/run ./manage.py run_ai >run_ai.log 2>&1 &
python ai_chat_api.py>chat_api.log 2>&1 &