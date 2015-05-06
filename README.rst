CoreMQ
======

CoreMQ is a pure-Python 2/3 messaging queue using `asyncio` sockets with JSON object transport. It was developed after finding a lack of Python-based message queue systems, and also for educational purposes.


Current Status
--------------
* Live Publish/Subscribe of messaging
* Point-to-point messaging
* Retrieve up to 10 previous messages per queue
* Master-master replication
* WebSocket server included
* No encryption


General Overview
----------------
Clients connect via TCP and optionally supply a list of queues to subscribe to and any options that should be active for that connection. The client library will by default keep a connection open to the server, and automatically reconnect if the connection is dropped. Messages, which are Python dictionaries or lists, are serialized to JSON, then sent to the server. In addition, messages must be sent to queues. Each client is automatically subscribed to the unique connection ID queue for their connection. This allows point-to-point communications, in addition to the regular pubsub functionality.

Pubsub messages are immediately sent to connected clients. If the client is not connected at the time the message is published, it will not recieve the message. This may be addressed in a later release.


Server Implementations
----------------------
There are currently two implementations: one that uses the ThreadingMixin with SocketServer (server.py), and another that uses the new asyncio module (aio_server.py), which provides an event loop, similar to how Node.js works. After having stress tested them both, the asyncio version proved to handle far more connections. Because of this, the SocketServer implementation is no longer being developed and is scheduled for removal.

Running `python aio_server.py` will start the MQ server.

In addition to the Message Queue server, there is now a separate WebSocket server (ws_server.py) that works with the Message Queue server to provide real-time comminucations to browsers.

Running `python ws_server.py` will start the WS server.

See the configuration section below for more options.


Example Client Usage (socket-based)
-----------------------------------

.. code:: python

  from coremq import MessageQueue
  m = MessageQueue('127.0.0.1')
  m.connect()
  print(m.welcome_message)
  >>> OK: Welcome from CoreMQ!
  m.subscribe('test')
  m.send_message(queue='test', message=dict(a=1))
  m.get_message()


Example Client Usage (asyncio-based)
------------------------------------
Coming soon...


Configuration File
------------------
Both servers look for a coremq.conf file on load in the current working directory. There is an example config file in this repository that can be used as a template. There are two sections, one for [CoreMQ] and one for [CoreWS]:

* address: the interface to listen on, default 0.0.0.0
* port: the port to listen on, default 6747
* log_file: the location of the log file, default stdout
* log_level: logging level, default DEBUG, other options: INFO, WARN, ERROR
* cluster_nodes (CoreMQ only): comma-separated list of CoreMQ servers that should be considered a cluster
* allowed_replicants (CoreMQ only): comma-separated list of servers that should be allowed to monitor all queues (cluster_nodes are automatically part of this list).

The cluster_nodes settings should be exactly the same on every CoreMQ server in order to keep the cluster happy. In order to use CoreWS on a server that is not in cluster_nodes, add its server name to allowed_replicants.


Future Developments
-------------------
This is a list of things I would like to add in the future:

* Memcached-like in-memory store
* Custom JSON serializers/deserializers
* HTTP status page
* Per queue settings (i.e. history length instead of the default of 10 messages)
* Authentication and Authorization for queues
* Status display (maybe an HTTP page)


Contributions
-------------
As always, contributions in any form are appreciated. This is more of a learning experiment and is not currently used in production, however, I hope that this will one day be used to power a WebSockets implementation that can be easily used with Django, or other Python-based web apps.
