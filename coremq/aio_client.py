"""
CoreMQ
------
A pure-Python messaging queue.

License
-------
The MIT License (MIT)
Copyright (c) 2015 Ross Peoples <ross.peoples@gmail.com>
Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

from common import construct_message, get_logger, load_configuration, validate_header
import json
import socket
import trollius as asyncio


class CoreMqClientFactory(object):
    def __init__(self, protocol, servers, port=6747, loop=None,
                 logger=None, auto_reconnect=True, attempts=1, subscriptions=None):
        if not isinstance(servers, (list, tuple)):
            servers = [servers]

        self.servers = servers
        self.port = port
        self.shutting_down = False
        self.protocol = protocol
        self.loop = loop or asyncio.get_event_loop()
        self.logger = self.get_logger(logger)
        self.connection = None
        self.auto_reconnect = auto_reconnect
        self.attempts = attempts
        self.initial_subscriptions = subscriptions
        self.lost_connection_callback = None
        self.connected_once = False
        self.connected_server = None

    def __call__(self, *args, **kwargs):
        return self.protocol(self, loop=self.loop, logger=self.logger, subscriptions=self.initial_subscriptions)

    @staticmethod
    def get_logger(logger):
        if logger:
            return logger

        c = load_configuration()
        return get_logger(c, 'CoreMQ')


    @asyncio.coroutine
    def connect(self):
        self.connection = None
        self.connected_server = None
        port = self.port
        for server in self.servers:
            if ':' in server:
                server, port = server.split(':')

            for i in range(self.attempts):
                try:
                    self.connection = yield asyncio.From(self.loop.create_connection(self, server, port))
                    self.connected_server = server
                    break
                except (OSError, socket.gaierror, socket.herror) as ex:
                    self.connection = None
                    self.connected_server = None
                    self.logger.warn('Failed to connect to CoreMQ %s: %s. Retrying in 1 second...' % (server, ex))
                    yield asyncio.From(asyncio.sleep(1))

            if self.connection:
                break

        if not self.connection and self.lost_connection_callback and self.connected_once:
            yield asyncio.From(self.loop.create_task(self.lost_connection_callback()))
        elif self.connection:
            self.connected_once = True

    def close(self):
        self.shutting_down = True


class CoreMqClientProtocol(asyncio.Protocol):
    def __init__(self, factory, loop=None, logger=None, subscriptions=None):
        super(CoreMqClientProtocol, self).__init__()

        self.factory = factory
        self.loop = loop or factory.loop
        self.transport = None
        self.reader = asyncio.StreamReader()
        self.writer = None
        self.uuid = None
        self.logger = factory.get_logger(logger)
        self.subscriptions = subscriptions or []
        self.options = dict()
        self.server = None
        self.connected_future = asyncio.Future()

    def connection_made(self, transport):
        self.logger.info('Connected to CoreMQ')
        self.transport = transport
        self.reader.set_transport(transport)
        self.writer = asyncio.StreamWriter(transport, self, self.reader, self.loop)

    def data_received(self, data):
        data = data.decode('utf-8')
        expected_length, data = validate_header(data)

        if len(data) < expected_length:
            data += self.reader.readexactly(expected_length - len(data)).decode('utf-8')

        if len(data) > expected_length:
            more_data = data[expected_length:]
            self.loop.call_soon(self.data_received, more_data)

            data = data[:expected_length]

        if ' ' not in data:
            return None, data

        queue, message = data.split(' ', 1)
        self._new_message(queue, json.loads(message))

    def _new_message(self, queue, message):
        if not self.uuid:
            # first message received, save server name and uuid for future calls
            if 'server' in message:
                self.server = message['server']

            self.uuid = queue
            if self.subscriptions:
                self.subscribe(*self.subscriptions)

            if self.options:
                self.set_options(**self.options)

            self.connected_future.set_result(True)

        self.logger.debug('New message - queue: %s, message: %s' % (queue, message))
        self.new_message(queue, message)

    def new_message(self, queue, message):
        """
        Override this function for new incoming messaging
        :param queue: The name of the queue the message is coming from
        :param message: The message
        """
        pass

    def send_message(self, queue, message):
        data = construct_message(queue, message)
        self.transport.write(data)

    def get_history(self, *queues):
        if not queues:
            queues = self.subscriptions

        if not queues:
            raise ValueError('Must pass at least one queue name')

        if not isinstance(queues, (list, tuple)):
            queues = [queues]

        return self.send_message(self.uuid, dict(coremq_gethistory=queues))

    def subscribe(self, *queues):
        if not queues:
            raise ValueError('Must pass at least one queue name')

        if not isinstance(queues, (list, tuple)):
            queues = [queues]

        for q in queues:
            if q not in self.subscriptions:
                self.subscriptions.append(q)

        self.send_message(self.uuid, dict(coremq_subscribe=queues))

    def unsubscribe(self, *queues):
        if not queues:
            raise ValueError('Must pass at least one queue name')

        if not isinstance(queues, (list, tuple)):
            queues = [queues]

        for q in queues:
            if q in self.subscriptions:
                self.subscriptions.remove(q)

        return self.send_message(self.uuid, dict(coremq_unsubscribe=queues))

    def set_options(self, **options):
        self.options.update(options)

        for key, val in options.items():
            if val is None and key in self.options:
                del self.options[key]

        return self.send_message(self.uuid, dict(coremq_options=options))

    def connection_lost(self, exc):
        if not self.factory.shutting_down and self.factory.auto_reconnect:
            self.logger.warn('Connection to CoreMQ lost unexpectedly. Attempting to reconnect...')
            self.loop.create_task(self.factory.connect())
        elif self.factory.shutting_down:
            self.logger.info('CoreMQ client shut down')
        else:
            self.logger.warn('Connection to CoreMQ lost unexpectedly. Client is now shut down.')

    def close(self):
        self.factory.shutting_down = True