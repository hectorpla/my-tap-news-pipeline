import news_client

def titles_of(articles):
    return map(lambda art: art['title'], articles)

def test_basic():
    articles = news_client.get_news_from_sources()

    print('testing get_news_from_sources with default arguments')
    print('\n'.join(titles_of(articles)))
    assert len(articles) > 0
    print('1 test passed')

    articles = news_client.get_news_from_sources(['bbc-sport'])

    print('testing get_news_from_sources with specified arguments')
    print('\n'.join(titles_of(articles)))
    assert len(articles) > 0
    print('2 test passed')
   

if __name__ == '__main__':
    test_basic() 