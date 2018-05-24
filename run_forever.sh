export $(cat .env | xargs)

# redis-server &

# TODO: be careful of the coupling between run_forever.sh and launch.sh

echo "===================== pipeline running ============================="
python3 news_pipeline/news_monitor.py &
python3 news_pipeline/news_fetcher.py &
# python3 news_classfication/classifier_server.py &
python3 news_pipeline/news_deduper.py 
# python3 preference_model/clicklearner.py &

