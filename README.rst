CoreMQ
======

CoreMQ is a pure-Python 2/3 messaging queue using TCP sockets with JSON object transport. It was developed after finding a lack of Python-based message queue systems, and also for educational purposes.


Current Status
--------------
* Live Publish/Subscribe of messaging
* Point-to-point messaging
* Retrieve up to 10 previous messages per queue
* Very, very alpha; limited testing so far
* No encryption


General Overview
----------------
Clients connect via TCP and optionally supply a list of queues to subscribe to and any options that should be active for that connection. The client library will by default keep a connection open to the server, and automatically reconnect if the connection is dropped. Messages, which are Python dictionaries or lists, are serialized to JSON, then sent to the server. In addition, messages must be sent to queues. Each client is automatically subscribed to the unique connection ID queue for their connection. This allows point-to-point communications, in addition to the regular pubsub functionality.

Pubsub messages are immediately sent to connected clients. If the client is not connected at the time the message is published, it will not recieve the message. This may be addressed in a later release.


Example Server Usage
--------------------
By default, the server is started in its own process when calling start(). It will respond to SIGINT and SIGKILL signals:

.. code:: python

from coremq.coremq_server import start()
start()


Example Client Usage
--------------------

.. code:: python

from coremq import MessageQueue
m = MessageQueue('127.0.0.1')
m.connect()
print(m.welcome_message)
>>> OK: Welcome from CoreMQ!
m.subscribe('test')
m.send_message(queue='test', message=dict(a=1))
m.get_message()


Future Developments
-------------------
This is a list of things I would like to add in the future:
* Memcached-like in-memory store
* WebSockets support
* Custom JSON serializers/deserializers
* HTTP status page
* Master-master replication
* Per queue settings (i.e. history length instead of the default of 10 messages)
* Authentication and Authorization for queues
* Status display (maybe an HTTP page)


Contributions
-------------
As always, contributions in any form are appreciated. This is more of a learning experiment and is not currently used in production, however, I hope that this will one day be used to power a WebSockets implementation that can be easily used with Django, or other Python-based web apps.
