import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__),'..','utils'))
from config_reader import get_config

from cloud_amqp_client import AMQPClient

config = get_config('../config/config.json')

AMQP_URL = config['new_click_queue_url']

def test_basic():
    # print(amqp_url + "({})".format(type(amqp_url)))
    client = AMQPClient(AMQP_URL, 'my_queue')

    client.connect()

    assert client.is_connected()

    assert client.get_message() is None

    client.send_message('hello world')

    assert client.get_message() == 'hello world'

    obj = { "hello": "world" }
    client.send_message(obj)


    assert client.get_message() == obj

    assert client.get_message() is None

    client.cancel_queue()

    client.close()
    print('[x] cloud amqp_client test passed')

if __name__ == '__main__':
    test_basic()