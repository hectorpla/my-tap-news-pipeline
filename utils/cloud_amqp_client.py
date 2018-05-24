import pika
import json

import config_reader

DEBUG = False

class AMQPClient():
    EXCHANGE = ''

    def __init__(self, amqp_url, queue_name):
        self._url = amqp_url
        self._queue_name = queue_name

        self._connection = None
        self._channel = None

    def __str__(self):
        return ('my queue name is {} and I am {} connected'
                .format(self._queue_name, 
                        '' if self.is_connected() else 'not'))

    def is_connected(self):
        return self._channel.is_open

    def _connect(self):
        # return pika.SelectConnection(
        #     parameters=pika.URLParameters(self._url),
        #     on_open_callback=self.on_open,
        #     on_open_error_callback=self.on_open_error,
        #     on_close_callback=self.on_close,
        # )
        params = pika.connection.URLParameters(self._url)
        params.socket_timeout = 2
        return pika.BlockingConnection(params)

    def connect(self):
        # TODO: not sure it works
        if self._connection is None or self._connection.is_closed:
            self._connection = self._connect()
        if self._channel is None or self._channel.is_closed:
            self._channel = self._connection.channel()
        self._channel.queue_declare(self._queue_name)

    def on_open(self):
        print('AMQP connection successfully opened')
        self._channel = self._connection.channel()

    def on_open_error(self):
        print('AMQP: open error!!!')

    def on_close(self):
        print('connection closed...')

    def close(self):
        if self._connection:
            self._connection.close()

    def cancel_queue(self):
        self._channel.queue_delete(self._queue_name)

    def send_message(self, message):
        self._channel.basic_publish(
            exchange=self.EXCHANGE,
            routing_key=self._queue_name,
            body=json.dumps(message)
        )
        if DEBUG:
            print("sent message '{}' to queue '{}'".format(message, self._queue_name))

    def get_message(self):
        method_frame, header_frame, body = self._channel.basic_get(self._queue_name)
        if method_frame:
            if DEBUG:
                print("received message '{}' from queue '{}'".format(body, self._queue_name))
            self._channel.basic_ack(method_frame.delivery_tag)
            return json.loads(body.decode('utf-8'))
        return None

    def sleep(self, seconds):
        self._connection.sleep(seconds)

def get_queue_client(config_file_name, url_key, name_key):
    config = config_reader.get_config(config_file_name)
    if config is None:
        print('get_queue_client: config invalid!!!')
        return

    url, name = config[url_key], config[name_key]
    queue_client = AMQPClient(url, name)
    queue_client.connect()

    assert queue_client.is_connected()
    return queue_client