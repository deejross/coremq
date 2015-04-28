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

import json
import sys

if sys.version[0] == '2':
    str_type = basestring
else:
    str_type = str


class ConnectionClosed(Exception):
    pass


class ProtocolError(Exception):
    pass


def send_message(socket, queue, message):
    if not isinstance(queue, str_type):
        raise ValueError('Queue name must be a string')

    if len(queue) < 1:
        raise ValueError('Queue name must be at least one character in length')

    if ' ' in queue:
        raise ValueError('Queue name must not contain spaces')

    if isinstance(message, str):
        message = dict(coremq_string=message)

    if isinstance(message, dict):
        message = json.dumps(message)
    else:
        raise ValueError('Messages should be either a dictionary or a string')

    if len(message) > 99999999:  # 100 MB max int that can fit in message header (8 characters, plus two controls)
        raise ValueError('Message cannot be 100MB or larger')

    if not isinstance(message, bytes):
        message = message.encode('utf-8')

    data = ('+%s %s ' % (len(message) + len(queue) + 1, queue)).encode('utf-8') + message
    socket.send(data)


def get_message(socket, timeout=1):
    socket.settimeout(timeout)
    data = socket.recv(10).decode('utf-8')
    expected_length, data = validate_header(data)

    while len(data) < expected_length:
        data += socket.recv(expected_length - len(data)).decode('utf-8')

    if ' ' not in data:
        return None, data

    queue, message = data.split(' ', 1)
    return queue, json.loads(message)


def validate_header(data):
    """
    Validates that data is in the form of "+5 Hello", with + beginning messages, followed by the length of the
    message as an integer, followed by a space, then the message.
    :param data: The raw data from the socket
    :return: (int, str) - the expected length of the message, the message
    """
    if not data:
        raise ConnectionClosed()

    if data[0] != '+':
        raise ProtocolError('Missing beginning +')

    if ' ' not in data:
        raise ProtocolError('Missing space after length')

    length, data = data.split(' ', 1)

    try:
        length = int(length[1:])
    except ValueError:
        raise ProtocolError('Length integer must be between + and space')

    return length, data