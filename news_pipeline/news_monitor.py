import datetime
import hashlib
import os
import sys

import redis

# for unregular import
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))

import news_client
from cloud_amqp_client import AMQPClient
# from config_reader import get_config

# TODO is this the best way to address it?
# config = get_config(os.path.join(os.path.dirname(__file__),'..','config', 'config.json'))
config = os.environ
REDIS_HOST = config['redis_host']
REDIS_PORT = config['redis_port']

SCRAPE_NEWS_TASK_QUEUE_URL = config['scrape_task_queue_url']
SCRAPE_NEWS_TASK_QUEUE_NAME = config['scrape_task_queue_name']

NEWS_SOURCES = [
    'cnn',
    'bbc-sport',
    'the-new-york-times',
    'bloomberg',
    'buzzfeed',
    'nbc-news'
]

SLEEP_TIME_IN_SECONDS = 60
NEWS_TIME_OUT_IN_SECONDS = 3600 * 24 * 3


def run():
    redis_client = redis.StrictRedis(REDIS_HOST, REDIS_PORT)
    amqp_client = AMQPClient(SCRAPE_NEWS_TASK_QUEUE_URL, 
                             SCRAPE_NEWS_TASK_QUEUE_NAME)
    amqp_client.connect()

    try:
        while True:
            news_list = news_client.get_news_from_sources(NEWS_SOURCES)
            num_news = 0

            for news in news_list:
                digest = hashlib.md5(news['title'].encode('utf-8')).hexdigest()

                if redis_client.get(digest):
                    continue

                num_news += 1
                news['digest'] = digest
                redis_client.set(digest, True)
                redis_client.expire(digest, NEWS_TIME_OUT_IN_SECONDS)

                print(news)
                amqp_client.send_message(news)

            print('News Monitor: fectched {} news'.format(num_news))
            amqp_client.sleep(SLEEP_TIME_IN_SECONDS)
    except KeyboardInterrupt:
        print('keyboard interrupt')
    # except SigTerm
    finally:
        amqp_client.close()

if __name__ == '__main__':
    run()
