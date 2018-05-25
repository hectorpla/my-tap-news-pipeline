RUN_PATH="run_forever.sh"

source "$RUN_PATH"
read -p "PRESS[ANYKEY]TO TERMINATE PROCESSES." PRESSKEY

# TODO: be careful of the coupling between run_forever.sh and launch.sh

pkill -f news_monitor.py
pkill -f news_fetcher.py
pkill -f news_deduper.py
# pkill -f classifier_server.py
# pkill -f clicklearner.py

# redis-cli shutdown

# unset $(cat .env | sed -E 's/(.*)=.*/\1/' | xargs)