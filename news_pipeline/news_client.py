import os
import json

import requests

config = os.environ
NEWS_API_ENDPOINT = "http://newsapi.org/v1/"
API_KEY = config["news_api_key"] # client id
ARTICLES_API = "articles"

CNN = "cnn"
DEFAULT_SOURCES = [CNN]
SORT_BY = "top"

MOCK_DATA = [
    {'title': 'Trump become the president', 'source': 'mock'}, 
    {'title': 'President Xi refill his years', 'source': 'mock'},
    {'title': 'Bacerlona beat Athletico Bilbao', 'source': 'mock'}
]

MOCK_SOURCES = ['mock']

def _buildUrl(endpoint=NEWS_API_ENDPOINT, api_name=ARTICLES_API):
    return endpoint + api_name

def get_news_from_sources(sources=DEFAULT_SOURCES, sortby=SORT_BY):
    results = []

    # for test start
    print('get_news_from_sources:', sources)
    if sources == MOCK_SOURCES:
        print('returning mock data')
        return MOCK_DATA
    # test end

    for source in sources:
        payload = {
            'apiKey': API_KEY,
            'source': source,
            'sortBy': sortby
        }
        response = requests.get(_buildUrl(), params=payload)
        # print(response) # <Response [200]>
        res_json = json.loads(response.content.decode('utf-8')) 
        # print(res_json) # {'status': 'ok'|else, ..., 'articles: list of articles'}
        
        if res_json is None or res_json['status'] != 'ok':
            continue
        if 'source' not in res_json:
            continue
        articles = res_json['articles']
        for news in articles:
            news['source'] = res_json['source']

        results.extend(articles)
    # print(results)
    return results
    

if __name__ == '__main__':
    get_news_from_sources()