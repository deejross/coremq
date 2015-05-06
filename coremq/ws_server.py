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
import socket
from aio_client import CoreMqClientFactory, CoreMqClientProtocol
from common import comma_string_to_list, get_logger, load_configuration
from autobahn.asyncio.websocket import WebSocketServerProtocol, WebSocketServerFactory
import trollius as asyncio
import uuid


class ServerState(object):
    connections = dict()
    logger = None
    mq_connection = None


class WsProtocol(WebSocketServerProtocol):
    def __init__(self):
        super(WsProtocol, self).__init__()

        self.request = None
        self.uuid = str(uuid.uuid4())
        self.subscriptions = [self.uuid]
        ServerState.connections[self.uuid] = self

    def onConnect(self, request):
        self.request = request
        ServerState.logger.info('Connections: %s' % len(ServerState.connections))

    def onOpen(self):
        pass

    def onMessage(self, payload, isBinary):
        ServerState.logger.debug(payload)
        message = json.loads(payload.decode('utf-8'))

        if isBinary:
            self.sendMessage(payload, isBinary)
            return

        if 'coremq_subscribe' in message:
            queues = message['coremq_subscribe']
            if not queues:
                self.sendMessage(json.dumps(dict(error='No queues found')))
                return
            elif not isinstance(queues, (list, tuple)):
                queues = [queues]

            for q in queues:
                if q not in self.subscriptions:
                    self.subscriptions.append(q)
        else:
            if 'queue' not in message:
                self.sendMessage(json.dumps(dict(error='Command not recognized')))
            else:
                queue = message['queue']
                del message['queue']
                message['coremq_fwdto'] = self.uuid
                if ServerState.mq_connection:
                    ServerState.mq_connection.send_message(queue, message)
                else:
                    self.sendMessage(json.dumps(dict(error='Not connected to CoreMQ')))

    def onClose(self, wasClean, code, reason):
        if self.uuid in ServerState.connections:
            del ServerState.connections[self.uuid]

        ServerState.logger.info('Connections: %s' % len(ServerState.connections))


class WsMqClient(CoreMqClientProtocol):
    def begin_replication(self, server_name):
        self.send_message(self.uuid, dict(coremq_replicant=server_name))

    def new_message(self, queue, message):
        loop = asyncio.get_event_loop()

        # make sure to blackhole any OK responses coming back from begin_replication
        if queue == self.uuid:
            if 'response' in message and 'Replication' in message['response']:
                if not message['response'].startswith('OK:'):
                    ServerState.logger.error('From replication client: %s' % message['response'])
                    self.close()
                    loop.stop()
                    return
                else:
                    return

        message['queue'] = queue

        for i, c in ServerState.connections.items():
            if queue in c.subscriptions:
                if 'coremq_sender' in message and message['coremq_sender'] == self.uuid and\
                        'coremq_fwdto' in message and message['coremq_fwdto'] == i:
                    continue
                c.sendMessage(json.dumps(message))


def main():
    config = load_configuration()
    ServerState.logger = get_logger(config, 'CoreWS')
    address = config.get('CoreWS', 'address', '0.0.0.0')
    port = int(config.get('CoreWS', 'port', '9000'))
    mq_servers = comma_string_to_list(config.get('CoreMQ', 'cluster_nodes', '').split(','))

    ServerState.logger.info('CoreWS Starting up...')
    ws_factory = WebSocketServerFactory('ws://%s:%s/ws' % (address, port))
    ws_factory.protocol = WsProtocol

    loop = asyncio.get_event_loop()
    server_coro = loop.create_server(ws_factory, address, port)
    server = loop.run_until_complete(server_coro)
    ServerState.logger.info('WebSocket Server running')

    mq_factory = CoreMqClientFactory(WsMqClient, mq_servers, loop=loop)

    @asyncio.coroutine
    def connect(*args):
        ServerState.mq_connection = None
        while True:
            yield asyncio.From(mq_factory.connect())
            if not mq_factory.connection:
                ServerState.logger.warn('No CoreMQ servers found. Retrying in 3 seconds...')
                yield asyncio.From(asyncio.sleep(3))
            else:
                conn = mq_factory.connection[1]
                conn.connected_future.add_done_callback(
                    lambda _: conn.begin_replication('%s:%s' % (socket.gethostname(), port))
                )
                ServerState.mq_connection = conn
                break

    mq_factory.lost_connection_callback = connect
    loop.run_until_complete(connect())

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        ServerState.logger.info('Shutting down WebSocket Server...')
        server.close()
        ServerState.logger.info('Shutting down MQ Client...')
        mq_factory.close()
        loop.close()
        ServerState.logger.info('CoreWS is now shut down')


if __name__ == '__main__':
    main()
