import os
import sys

from pymongo import MongoClient

# from config_reader import get_config

# config = get_config(
#     os.path.join(
#         os.path.dirname(__file__),
#         '..', 'config', 'config.json'))
config = os.environ

HOST_NAME = config['mongo_host']
PORT = config['mongo_port']
DB_NAME = config['news_db']
COLLECTION_NAME = config['new_collection']

client = MongoClient(HOST_NAME, int(PORT))

def get_db(db=DB_NAME):
    return client[db]

def get_news_collection():
    return client[DB_NAME].get_collection(COLLECTION_NAME)

def main():
    db = client.demo
    collection = db['news']
    one_record = collection.find_one()

    print(one_record)
    print('xx ------------------- check if it return news')

    news_from_production = get_news_collection().find_one()
    print(news_from_production)
    print('xx ------------------- check if it return news')

if __name__ == '__main__':
    main()