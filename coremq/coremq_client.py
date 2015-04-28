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

import socket
from .coremq_common import get_message, send_message


class MessageQueue(object):
    def __init__(self, server, port=6747):
        self.server = server
        self.port = port
        self.socket = None
        self.connection_id = None
        self.welcome_message = None
        self.subscriptions = []
        self.options = dict()
        self.last_message_time = 0

    def connect(self):
        if self.socket:
            return

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((self.server, self.port))
        self.connection_id, self.welcome_message = get_message(self.socket)

        if self.subscriptions:
            self.subscribe(*self.subscriptions)

        if self.options:
            self.set_options(**self.options)

    def close(self):
        if self.socket:
            self.socket.close()
            self.socket = None

    def send_message(self, queue, message):
        if not self.socket:
            self.connect()

        try:
            send_message(self.socket, queue, message)
        except socket.error:
            # attempt to reconnect if there was a connection error
            self.close()
            self.connect()
            send_message(self.socket, queue, message)

        try:
            return self.get_message()
        except socket.error:
            return None, None

    def get_message(self, timeout=1):
        if not self.socket:
            self.connect()

        try:
            queue, message = get_message(self.socket, timeout=timeout)
        except socket.timeout:
            return None, None
        except socket.error:
            # attempt to reconnect if there was a connection error
            self.close()
            self.connect()
            try:
                queue, message = get_message(self.socket, timeout=timeout)
            except socket.timeout:
                return None, None

        if 'response' in message and message['response'] == 'BYE':
            self.close()

        return queue, message

    def get_history(self, *queues):
        if not queues:
            queues = self.subscriptions

        if not queues:
            raise ValueError('Must pass at least one queue name')

        if not isinstance(queues, (list, tuple)):
            queues = [queues]

        return self.send_message(self.connection_id, dict(coremq_gethistory=queues))

    def subscribe(self, *queues):
        if not queues:
            raise ValueError('Must pass at least one queue name')

        if not isinstance(queues, (list, tuple)):
            queues = [queues]

        for q in queues:
            if q not in self.subscriptions:
                self.subscriptions.append(q)

        return self.send_message(self.connection_id, dict(coremq_subscribe=queues))

    def unsubscribe(self, *queues):
        if not queues:
            raise ValueError('Must pass at least one queue name')

        if not isinstance(queues, (list, tuple)):
            queues = [queues]

        for q in queues:
            if q in self.subscriptions:
                self.subscriptions.remove(q)

        return self.send_message(self.connection_id, dict(coremq_unsubscribe=queues))

    def set_options(self, **options):
        self.options.update(options)

        for key, val in options.items():
            if val is None and key in self.options:
                del self.options[key]

        return self.send_message(self.connection_id, dict(coremq_options=options))