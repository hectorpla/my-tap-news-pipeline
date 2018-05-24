import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))
from config_reader import get_config
from cloud_amqp_client import AMQPClient

config = get_config(os.path.join(os.path.dirname(__file__),
                                 '..', 'config', 'config.json'))

USER_CLICK_QUEUE_URL = config['new_click_queue_url']
USER_CLICK_QUEUE_NAME = config['new_click_queue_name']

SLEEP_TIME_IN_SECONDS = 5

def handle_message(msg):
    print('Click Handler: message {}'.format(msg))


def run(times=-1):
    click_queue_client = AMQPClient(USER_CLICK_QUEUE_URL, USER_CLICK_QUEUE_NAME)
    click_queue_client.connect()
    assert click_queue_client.is_connected()
    print('Click Handler: my queue name: {}'.format(click_queue_client))

    while True:
        message = click_queue_client.get_message()
        try:
            handle_message(message)
        except Exception as e:
            raise e

        click_queue_client.sleep(SLEEP_TIME_IN_SECONDS)
        if times > 0: times -= 1
        if times == 0: break


if __name__ == '__main__':
    run()