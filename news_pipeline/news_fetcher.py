import os
import sys

from newspaper import Article

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))
import news_client
from cloud_amqp_client import AMQPClient

# from config_reader import get_config
# config = get_config(os.path.join(os.path.dirname(__file__),'..','config', 'config.json'))
config = os.environ
SCRAPE_QUEUE_URL = config["scrape_task_queue_url"]
DEDUPE_QUEUE_URL = config["dedupe_task_queue_url"]
SCRAPE_NEWS_TASK_QUEUE_NAME = config["scrape_task_queue_name"]
DEDUPE_NEWS_TASK_QUEUE_NAME = config["dedupe_task_queue_name"]

SLEEP_TIME_IN_SECONDS = 5

scrape_queue_client = AMQPClient(SCRAPE_QUEUE_URL, SCRAPE_NEWS_TASK_QUEUE_NAME)
scrape_queue_client.connect()
dedupe_queue_client = AMQPClient(DEDUPE_QUEUE_URL, DEDUPE_NEWS_TASK_QUEUE_NAME)
dedupe_queue_client.connect()

assert scrape_queue_client.is_connected()
assert dedupe_queue_client.is_connected()


def handle_message(msg):
    print('News Fetcher getting message:', msg)
    if msg is None or not isinstance(msg, dict):
        print('news fetcher: message is broken')
        return

    task = msg

    # if task['source'] == 'cnn':
    #     print('scraping')
    # else:
    #     print('other source than CNN not implemented')
    #     return

    # TODO do with scraper module
    
    aritcle = Article(task['url'])
    # TODO: test if crash
    # if not aritcle.is_valid_url():
    #     print('News fetcher: not a valid url')
    #     return
    aritcle.download()
    aritcle.parse()

    task['text'] = aritcle.text
    dedupe_queue_client.send_message(task)
    print('News Fetcher: message sent to dedupe queue')


def run(times=-1):
    while True:
        msg = scrape_queue_client.get_message()
        if msg is not None:
            try:
                handle_message(msg)
            except Exception as e:
                print(e)
        # if decreas count here, weird behavior, decreasing happens before processing message
        scrape_queue_client.sleep(SLEEP_TIME_IN_SECONDS)
        if times > 0: times -= 1
        # print(times)
        if times == 0: break

    # TODO clean up queue connection after interrupted signal


if __name__ == '__main__':
    run()