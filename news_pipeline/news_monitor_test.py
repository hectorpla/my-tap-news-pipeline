import time
import os, sys
from multiprocessing import Process

import news_monitor
import redis

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))
import news_client
from cloud_amqp_client import AMQPClient
from queue_cleaner import clear_queue

QUEUE_URL =  'http://localhost'
QUEUE_NAME = 'fetch-queue-test'

REDIS_HOST = 'localhost'
REDIS_PORT = 6379

# TODO: this test is not unit test, it involves network connction like mq
def test_monitor_basic():
    news_monitor.NEWS_SOURCES = news_client.MOCK_SOURCES
    MOCK_DATA = news_client.MOCK_DATA

    print('test_monitor_basic: cleaning queue "{}" first---------'.format(QUEUE_NAME))
    clear_queue(QUEUE_URL, QUEUE_NAME)
    # TODO: redis server flush all
    redis_client = redis.StrictRedis(REDIS_HOST, REDIS_PORT)
    redis_client.flushall()

    print('test_monitor_basic: adding message to queue "{}"--------'.format(QUEUE_NAME))
    amqp_client = AMQPClient(QUEUE_URL, QUEUE_NAME)
    amqp_client.connect()

    proc = Process(target=news_monitor.run, name='monitor_run', 
        args=(REDIS_HOST, REDIS_PORT, QUEUE_URL, QUEUE_NAME))
    proc.start()
    print('test_monitor_basic: executing... (wait for 2 seconds to cut)')
    time.sleep(2)

    proc.terminate()

    for i in range(len(MOCK_DATA)):
        message = amqp_client.get_message()
        del message['digest']
        print(message, MOCK_DATA[i])
        assert message == MOCK_DATA[i]

    print('test_monitor_basic: [x] test_monitor_basic test passed')


if __name__ == '__main__':
    test_monitor_basic()