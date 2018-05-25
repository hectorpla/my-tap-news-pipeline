import datetime
import hashlib
import os
import sys
import logging, coloredlogs

import redis

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))
import news_client
from cloud_amqp_client import AMQPClient

logger = logging.getLogger(__name__)
coloredlogs.install(level=os.environ.get('LOGGER_LEVEL', 'INFO'), logger=logger)

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


def run(redis_host=REDIS_HOST, redis_port=REDIS_PORT, 
        scrape_queue_url=SCRAPE_NEWS_TASK_QUEUE_URL, 
        scrape_queue_name=SCRAPE_NEWS_TASK_QUEUE_NAME):
    redis_client = redis.StrictRedis(redis_host, redis_port)
    amqp_client = AMQPClient(scrape_queue_url, scrape_queue_name)
    amqp_client.connect()

    while True:
        logger.debug('News monitor: iter..')
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

            logger.debug('News Monitor: got news {}'.format(news))
            amqp_client.send_message(news)

        logger.info('News Monitor: fectched {} news'.format(num_news))
        amqp_client.sleep(SLEEP_TIME_IN_SECONDS)


if __name__ == '__main__':
    run()
