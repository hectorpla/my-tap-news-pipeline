import datetime
import os
import sys

from dateutil import parser
from sklearn.feature_extraction.text import TfidfVectorizer

sys.path.append(os.path.join(os.path.dirname(__file__),'..','utils'))
import mongodb_client
import classifier_client
from cloud_amqp_client import AMQPClient
# from config_reader import get_config


# TODO: this global are bad, and makes it uncovered by tests
# config = get_config(os.path.join(os.path.dirname(__file__),'..','config', 'config.json'))
config = os.environ
DB_NAME = config['news_db']
COLLECTION_NAME = config['new_collection']
DEDUPE_QUEUE_URL = config['dedupe_task_queue_url']
DEDUPE_QUEUE_NAME = config['dedupe_task_queue_name']

SLEEP_TIME_IN_SECONDS = 5

dedupe_queue_client = AMQPClient(DEDUPE_QUEUE_URL, DEDUPE_QUEUE_NAME)
dedupe_queue_client.connect()

assert dedupe_queue_client.is_connected()

NEWS_SIMILARITY_THRESHOLD = 0.8

class NotContainPublishTimeError(Exception):
    def __str__(self):
        return 'News not containing publish time!!!'

def handle_message(msg):
    # print('dedupter handling message', msg)
    if msg is None or not isinstance(msg, dict):
        print('News Deduper: message is broken')
        return

    task = msg
    if 'text' not in task or not task['text']:
        print('News Deduper publishedAt, not containing text')
        return

    if 'publishedAt' not in task or not task['publishedAt']:
        raise NotContainPublishTimeError

    published_at = parser.parse(task['publishedAt'])
    day_begin = datetime.datetime(published_at.year,
                                  published_at.month,
                                  published_at.day,
                                  0, 0, 0, 0)
    day_end = day_begin + datetime.timedelta(days=1)

    news_collection = mongodb_client.get_db(DB_NAME).get_collection(COLLECTION_NAME)
    
    # efficiency problem if the db grows
    news_on_the_day = news_collection.find({
        'publishedAt': {'$gte': day_begin, '$lt': day_end}
    })

    documents = [task['text']]
    documents.extend(news['text'] for news in news_on_the_day)

    tf_idf = TfidfVectorizer().fit_transform(documents)
    similarity_matrix = tf_idf * tf_idf.T
    # print('News Deduper', similarity_matrix)

    num_rows = similarity_matrix.shape[0]
    if any(similarity_matrix[0, i] > NEWS_SIMILARITY_THRESHOLD for i in range(1, num_rows)):
        print('News Deduper: similar document, throw it away')
        return

    # reformat the published date
    task['publishedAt'] = published_at

    # TODO: feature extraction should be same in backfill procedure
    # TODO actually should set another queue for classification
    if 'title' in task:
        try:
            task['category'] = classifier_client.classify(task['title'])
        except Exception as e:
            print("News Deduper: failed to classify using the classifier client", e)
            
    print('News Deduper: putting into database', task['digest'])
    news_collection.replace_one({'digest': task['digest']}, task, upsert=True)

def run(times=-1):
    while True:
        msg = dedupe_queue_client.get_message()
        if msg is not None:
            try:
                handle_message(msg)
            except NotContainPublishTimeError as e:
                print("News Deduper:", e)
        dedupe_queue_client.sleep(SLEEP_TIME_IN_SECONDS)
        if times > 0: times -= 1
        if times == 0: break

if __name__ == '__main__':
    run()
