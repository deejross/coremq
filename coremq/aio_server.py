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

from common import comma_string_to_list, get_logger, construct_message, load_configuration, validate_header
from aio_client import CoreMqClientFactory, CoreMqClientProtocol
from collections import deque
import json
import socket
import time
import trollius as asyncio
import uuid


class ServerState(object):
    logger = None
    name = socket.gethostname().lower()
    listen_address = ('0.0.0.0', 6747)
    connections = dict()  # maintains current connections, key is random ID given on connect, value is Protocol instance
    cluster_nodes = []
    allowed_replicants = []
    replicant_id_to_name = dict()  # replicants have connection IDs just like clients and this maps that ID to its name
    history = dict()
    master = None  # the MQ master if this server is a replicant


class CoreMqServerProtocol(asyncio.Protocol):
    LOOP = None

    def __init__(self, loop=None):
        super(CoreMqServerProtocol, self).__init__()

        self.LOOP = loop or asyncio.get_event_loop()
        self.uuid = str(uuid.uuid4())
        self.transport = None
        self.peer = None
        self.local_ip = None
        self.reader = asyncio.StreamReader()
        self.subscriptions = []
        self.options = dict()
        self.is_replicant = False
        self.hostname = None
        ServerState.connections[self.uuid] = self

    def connection_made(self, transport):
        self.transport = transport
        self.peer = transport.get_extra_info('peername')
        self.hostname = self.peer[0]
        self.local_ip = transport.get_extra_info('sockname')[0]
        self.reader.set_transport(transport)
        self.send_message(self.uuid, dict(response='OK: Welcome to CoreMQ server', server=ServerState.name))

        try:
            self.hostname = socket.gethostbyaddr(self.peer[0])[0]
        except socket.herror:
            pass

        ServerState.logger.debug('New connections: %s' % self.hostname)

    def data_received(self, data):
        data = data.decode('utf-8')
        expected_length, data = validate_header(data)

        if len(data) < expected_length:
            data += self.reader.readexactly(expected_length - len(data)).decode('utf-8')

        if len(data) > expected_length:
            more_data = data[expected_length:]
            self.LOOP.call_soon(self.data_received, more_data)

            data = data[:expected_length]

        if ' ' not in data:
            self.respond(self.uuid, 'ERROR: Missing queue or message')
            return

        queue, message = data.split(' ', 1)
        message = json.loads(message)
        if not isinstance(message, dict):
            self.respond(self.uuid, 'ERROR: Message must be a dictionary')
            return

        message.update(dict(
            coremq_sender=self.uuid,
            coremq_sent=time.time()
        ))

        self.LOOP.create_task(self.new_message(queue, message))

    @asyncio.coroutine
    def new_message(self, queue, message):
        if 'coremq_server' not in message:
            message['coremq_server'] = '%s:%s' % (ServerState.name, ServerState.listen_address[1])
            quiet = False
        else:
            # received via replication
            quiet = True

        if 'coremq_fwdto' in message and self.is_replicant:
            to = message['coremq_fwdto']
        else:
            to = self.uuid

        ServerState.logger.debug('New message - queue: %s, message: %s' % (queue, message))

        try:
            if 'coremq_subscribe' in message:
                self.subscribe(message['coremq_subscribe'])
                self.respond(to, 'OK: Subscribe successful', quiet)
            elif 'coremq_unsubscribe' in message:
                self.unsubscribe(message['coremq_unsubscribe'])
                self.respond(to, 'OK: Unsubscribe successful', quiet)
            elif 'coremq_options' in message:
                self.set_options(message['coremq_options'])
                self.respond(to, 'OK: Options set', quiet)
            elif 'coremq_gethistory' in message:
                self.get_history(message['coremq_gethistory'], to)
            elif 'coremq_replicant' in message:
                self.begin_replication(message['coremq_replicant'])
            elif 'coremq_status' in message:
                self.get_status(to)
            else:
                self.store_message(queue, message)
                yield asyncio.From(self.broadcast(queue, message))

                self.respond(to, 'OK: Message sent', quiet)
        except Exception as ex:
            ServerState.logger.error(str(ex))
            self.respond(to, 'ERROR: %s' % ex, quiet)
            raise

    def respond(self, to, message, quiet=False):
        if not quiet:
            self.send_message(to, dict(response=message))

    def send_message(self, queue, message):
        data = construct_message(queue, message)

        try:
            self.transport.write(data)
        except Exception:
            self.transport.close()

    def connection_lost(self, exc):
        del ServerState.connections[self.uuid]

        # clean up replicants
        if self.uuid in ServerState.replicant_id_to_name:
            del ServerState.replicant_id_to_name[self.uuid]

        ServerState.logger.debug('Closed connection: %s' % self.hostname)

    def subscribe(self, queues):
        if not queues:
            return

        if not isinstance(queues, (list, tuple)):
            queues = [queues]

        subs = ServerState.connections[self.uuid].subscriptions
        for q in queues:
            if q not in subs:
                subs.append(q)

    def unsubscribe(self, queues):
        if not queues:
            return

        if not isinstance(queues, (list, tuple)):
            queues = [queues]

        subs = ServerState.connections[self.uuid].subscriptions
        for q in queues:
            if q in subs:
                subs.remove(q)

    def set_options(self, options):
        opts = ServerState.connections[self.uuid].options
        opts.update(options)

        for key, val in options.items():
            if val is None and key in opts:
                del opts[key]

    def get_history(self, queues, to):
        result = dict()
        for q in queues:
            if q in ServerState.history:
                result[q] = list(ServerState.history[q])

        self.send_message(to, dict(history=result))

    def begin_replication(self, name):
        allowed = [r.split(':')[0].split('.')[0].lower() for r in ServerState.allowed_replicants]
        if self.peer[0] in allowed or self.hostname.split('.')[0].lower() in allowed:
            if self.uuid not in ServerState.replicant_id_to_name:
                ServerState.replicant_id_to_name[self.uuid] = name
            self.is_replicant = True
            self.respond(self.uuid, 'OK: Replication request successful')
            ServerState.logger.info('New replicant: %s' % self.hostname)
        else:
            self.respond(self.uuid, 'ERROR: Not allowed to be a replicant')

    def get_status(self, to):
        if ServerState.master is None:
            self.send_message(to, dict(
                coremq_fwdto=to,
                master=ServerState.name,
                replicants=list(ServerState.replicant_id_to_name.values()),
                connections=len(ServerState.connections)
            ))
        else:
            self.send_message(to, dict(
                replicant_of=ServerState.master.factory.connected_server,
                replicants=list(ServerState.replicant_id_to_name.values()),
                connections=len(ServerState.connections)
            ))

    @staticmethod
    @asyncio.coroutine
    def store_message(queue, message):
        if queue not in ServerState.history:
            ServerState.history[queue] = deque(maxlen=10)

        ServerState.history[queue].append(message)

    @staticmethod
    @asyncio.coroutine
    def broadcast(queue, message):
        if ServerState.master and 'coremq_master' not in message:
            if 'coremq_fwdto' not in message and 'coremq_sender' in message:
                message['coremq_fwdto'] = message['coremq_sender']

            ServerState.master.send_message(queue, message)

        for i, n in ServerState.replicant_id_to_name.items():
            c = ServerState.connections[i]
            if not ServerState.master:
                message['coremq_master'] = ServerState.name
            if n != message['coremq_server']:
                c.send_message(queue, message)

        for i, c in ServerState.connections.items():
            if i == message.get('coremq_sender', None) or c.is_replicant:
                continue

            if queue in c.subscriptions:
                c.send_message(queue, message)


class ReplicationClientProtocol(CoreMqClientProtocol):
    def begin_replication(self, server_name):
        """
        Attempts to promote this connection to allow replication
        :param server_name: This must match the coremq_server attribute, set by ServerState.name
        """
        return self.send_message(self.uuid, dict(coremq_replicant=server_name))

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

        # hijack an existing server connection and act like a client to forward on the message
        if ServerState.connections:
            conn = ServerState.connections[list(ServerState.connections.keys())[0]]
            self.loop.create_task(conn.new_message(queue, message))


def load_settings():
    c = load_configuration()
    ServerState.cluster_nodes = comma_string_to_list(c.get('CoreMQ', 'cluster_nodes', ','))
    ServerState.allowed_replicants = comma_string_to_list(c.get('CoreMQ', 'allowed_replicants', ''))
    ServerState.allowed_replicants.extend(ServerState.cluster_nodes)

    address = c.get('CoreMQ', 'address', '0.0.0.0')
    port = int(c.get('CoreMQ', 'port', '6747'))
    ServerState.listen_address = (address, port)
    ServerState.logger = get_logger(c, 'CoreMQ')


def promote_to_master():
    ServerState.master = None
    ServerState.logger.warn('Lost connection to master and no others are available. Assuming role of master MQ')
    yield


def find_master():
    loop = asyncio.get_event_loop()
    ServerState.logger.info('Attempting to locate master CoreMQ server for replication...')
    servers = ServerState.cluster_nodes[:]
    removals = []
    port = str(ServerState.listen_address[1])
    for s in servers:
        sp = s
        if ':' not in s:
            sp += ':6747'

        if ServerState.name.split('.', 1)[0] == sp.split('.', 1)[0] and port == sp.split(':')[-1]:
            removals.append(s)

    for r in removals:
        servers.remove(r)

    if not servers:
        ServerState.logger.info('This server is the only one listed in cluster_nodes, Assuming role of master MQ')
        return

    factory = CoreMqClientFactory(ReplicationClientProtocol, servers, loop=loop)
    yield asyncio.From(factory.connect())
    factory.lost_connection_callback = promote_to_master

    if not factory.connection:
        ServerState.logger.warn('No other CoreMQ servers found. Assuming role of master MQ')
    else:
        conn = factory.connection[1]
        conn.connected_future.add_done_callback(
            lambda _: conn.begin_replication('%s:%s' % (ServerState.name, ServerState.listen_address[1]))
        )
        ServerState.master = conn


def main():
    load_settings()
    ServerState.logger.info('CoreMQ Starting up...')

    loop = asyncio.get_event_loop()
    address = ServerState.listen_address
    server_coro = loop.create_server(lambda: CoreMqServerProtocol(), address[0], address[1])
    loop.run_until_complete(server_coro)
    ServerState.logger.info('CoreMQ Server running on %s' % address[1])

    if ServerState.cluster_nodes:
        loop.run_until_complete(find_master())

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        ServerState.logger.info('Shutting down CoreMQ...')
        loop.close()
        ServerState.logger.info('CoreMQ is now shut down')


if __name__ == '__main__':
    main()