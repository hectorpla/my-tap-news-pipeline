import os, sys

import clickhandler

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from config_reader import get_config
from cloud_amqp_client import AMQPClient
from queue_cleaner import clear_queue
import rpc_operations


config = get_config(os.path.join(os.path.dirname(__file__),
                                 '..', 'config', 'config.json'))

USER_CLICK_QUEUE_URL = config['new_click_queue_url']
USER_CLICK_QUEUE_NAME = 'test'

TEST_CLICK_DATA = [
    ['user1', 'digest1'],
    ['user2', 'digest2']
]

def test_basic():
    # not a good idea to test, but a way I can think of seperating the queue
    # in test and production
    rpc_operations.USER_CLICK_QUEUE_URL = USER_CLICK_QUEUE_URL
    rpc_operations.USER_CLICK_QUEUE_NAME = USER_CLICK_QUEUE_NAME
    rpc_operations.init()
    clickhandler.USER_CLICK_QUEUE_URL = USER_CLICK_QUEUE_URL
    clickhandler.USER_CLICK_QUEUE_NAME = USER_CLICK_QUEUE_NAME
    
    print('Click Handler test: cleaning up click queue...')
    clear_queue(USER_CLICK_QUEUE_URL, USER_CLICK_QUEUE_NAME)

    print('Click Handler test: logging clicks...')
    for pair in TEST_CLICK_DATA:
        rpc_operations.log_click(*pair)

    print(print('Click Handler test: handling clicks...'))
    clickhandler.run(len(TEST_CLICK_DATA))

    queue_client = AMQPClient(USER_CLICK_QUEUE_URL, USER_CLICK_QUEUE_NAME)
    queue_client.connect()

    should_be_empty = queue_client.get_message()
    print('Click Handler Test: {} should be None'.format(should_be_empty))
    assert should_be_empty is None
    queue_client.close

    print('Click Handler: test passed')
    

if __name__ == '__main__':
    test_basic()