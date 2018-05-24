import os
import sys

from cloud_amqp_client import AMQPClient
from config_reader import get_config

# config = get_config(os.path.join(os.path.dirname(__file__),
#                                  '..', 'config', 'config.json'))
config = os.environ

SCRAPE_NEWS_TASK_QUEUE_URL = config["scrape_task_queue_url"]
SCRAPE_NEWS_TASK_QUEUE_NAME = config["scrape_task_queue_name"]
DEDUPE_NEWS_TASK_QUEUE_URL = config["dedupe_task_queue_url"]
DEDUPE_NEWS_TASK_QUEUE_NAME = config["dedupe_task_queue_name"]

CLICK_QUEUE_URL = config['new_click_queue_url']
CLICK_QUEUE_NAME = config['new_click_queue_name']

def clear_queue(queue_url, queue_name):
    count = 0
    amqp_client = AMQPClient(queue_url, queue_name)
    amqp_client.connect()

    print('xxx cleaning queue "{}"'.format(queue_name))
    while True:
        message = amqp_client.get_message()
        if message is None:
            break
        print(message)
        count += 1
    
    amqp_client.close()
    
    print('xxx cleaned {} message on {}'.format(count, queue_name))

def clear_all():
    """Dangerous to call after deployment"""
    clear_queue(SCRAPE_NEWS_TASK_QUEUE_URL, SCRAPE_NEWS_TASK_QUEUE_NAME)
    clear_queue(DEDUPE_NEWS_TASK_QUEUE_URL, DEDUPE_NEWS_TASK_QUEUE_NAME)
    clear_queue(CLICK_QUEUE_URL, CLICK_QUEUE_NAME)

if __name__ == '__main__':    
    clear_all()
        