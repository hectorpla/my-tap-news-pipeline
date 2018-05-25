import os
import sys
import logging, coloredlogs

from newspaper import Article

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))
import news_client
from cloud_amqp_client import AMQPClient

logger = logging.getLogger(__name__)
coloredlogs.install(level=os.environ.get('LOGGER_LEVEL', 'INFO'), logger=logger)

config = os.environ
SCRAPE_QUEUE_URL = config["scrape_task_queue_url"]
DEDUPE_QUEUE_URL = config["dedupe_task_queue_url"]
SCRAPE_NEWS_TASK_QUEUE_NAME = config["scrape_task_queue_name"]
DEDUPE_NEWS_TASK_QUEUE_NAME = config["dedupe_task_queue_name"]

SLEEP_TIME_IN_SECONDS = 5


def handle_message(msg):
    logger.debug('News Fetcher getting message: {}'.format(msg))
    if msg is None or not isinstance(msg, dict):
        logger.info('news fetcher: message is broken')
        return

    task = msg

    # TODO: alternative do with scraper module
    
    aritcle = Article(task['url'])
    # TODO test if crash
    # if not aritcle.is_valid_url():
    #     logger.debug('News fetcher: not a valid url')
    #     return
    aritcle.download()
    aritcle.parse()

    task['text'] = aritcle.text


def run(scrape_queue_url=SCRAPE_QUEUE_URL, scrape_queue_name=SCRAPE_NEWS_TASK_QUEUE_NAME,
        dedupe_queue_url=DEDUPE_QUEUE_URL, dedupe_queue_name=DEDUPE_NEWS_TASK_QUEUE_NAME,
        times=-1):
    scrape_queue_client = AMQPClient(scrape_queue_url, scrape_queue_name)
    scrape_queue_client.connect()
    dedupe_queue_client = AMQPClient(dedupe_queue_url, dedupe_queue_name)
    dedupe_queue_client.connect()

    assert scrape_queue_client.is_connected()
    assert dedupe_queue_client.is_connected()

    while True:
        logger.debug('News fetcher: iter..')
        msg = scrape_queue_client.get_message()
        if msg is not None:
            try:
                handle_message(msg)
                dedupe_queue_client.send_message(msg)
                logger.info('News Fetcher: message sent to dedupe queue (url: {})'
                    .format(msg.get('url')))
            except Exception as e:
                logger.warning('News fetcher: handling error: {}'.format(e))
        # if decreas count here, weird behavior, decreasing happens before processing message
        scrape_queue_client.sleep(SLEEP_TIME_IN_SECONDS)
        if times > 0: times -= 1
        if times == 0: break

    # TODO clean up queue connection after interrupted signal


if __name__ == '__main__':
    run()