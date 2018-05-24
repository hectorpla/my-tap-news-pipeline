export $(cat .env | xargs)

redis-server &

python3 news_pipeline/news_monitor.py &
python3 news_pipeline/news_fetcher.py &
# python3 news_classfication/classifier_server.py &
python3 news_pipeline/news_deduper.py &
# python3 preference_model/clicklearner.py &

echo"=================================================="
read -p "PRESS[ANYKEY]TOTERMINATEPROCESSES." PRESSKEY

pkill -f news_monitor.py
pkill -f news_fetcher.py
pkill -f news_deduper.py
# pkill -f classifier_server.py
# pkill -f clicklearner.py

redis-cli shutdown

unset $(cat .env | sed -E 's/(.*)=.*/\1/' | xargs)