import os
import sys

import redis

import news_fetcher
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))
import news_client
from cloud_amqp_client import AMQPClient
import queue_cleaner

# from config_reader import get_config
# config = get_config('../config/config.json')
config = os.environ

SCRAPE_QUEUE_URL = config["scrape_task_queue_url"]
DEDUPE_QUEUE_URL = config["dedupe_task_queue_url"]
SCRAPE_NEWS_TASK_QUEUE_NAME = config["scrape_task_queue_name"]
DEDUPE_NEWS_TASK_QUEUE_NAME = config["dedupe_task_queue_name"]

TEST_SCRAPE_TASK = [
    'not a dict',
    {
        'url': 'some-other-source.com',
        'source': 'not cnn',
    },
    {
        'title': 'Uber pulls self-driving cars after first fatal crash of autonomous vehicle', 
        'url': 'http://money.cnn.com/2018/03/19/technology/uber-autonomous-car-fatal-crash/index.html', 
        'source': 'cnn',
        'publishedAt': '2018-03-18T20:36:47Z'
    },
    {
        'title': 'Loophole would protect self-driving car companies from lawsuits', 
        'url': 'http://money.cnn.com/2018/03/14/technology/self-driving-car-senate-loophole/index.html', 
        'source': 'cnn',
        'publishedAt': '2018-03-17T20:36:47Z'
    }
]

REDIS_HOST = 'localhost'
REDIS_PORT = '6379'
redis_client = redis.StrictRedis(REDIS_HOST, REDIS_PORT)

def test_basic():
    print('news_fetcher_test: cleaning all queues...')
    queue_cleaner.clear_all()
    print('flushing all cache in Redis')
    redis_client.flushall() # TODO dangerous to run when deployed

    scrape_queue_client = AMQPClient(SCRAPE_QUEUE_URL, SCRAPE_NEWS_TASK_QUEUE_NAME)
    scrape_queue_client.connect()
    assert scrape_queue_client.is_connected()

    print('test_fetcher_basic: adding news onto scrape queue...')
    for message in TEST_SCRAPE_TASK:
        scrape_queue_client.send_message(message)


    print('getting messages from the queue and process...')
    news_fetcher.SLEEP_TIME_IN_SECONDS = 1
    news_fetcher.run(len(TEST_SCRAPE_TASK))

    should_be_empty_msg = scrape_queue_client.get_message()
    print('news_fetcher_test(expecting None):', should_be_empty_msg)
    assert should_be_empty_msg is None
    scrape_queue_client.close()

    queue_cleaner.clear_queue(DEDUPE_QUEUE_URL, DEDUPE_NEWS_TASK_QUEUE_NAME)
    print('news_fetcher test passed')

if __name__ == '__main__':
    test_basic()