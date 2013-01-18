#!/usr/bin/env python
import json
import sys
import uuid

import pika



# Constants
QUEUE_NAME = 'rpc_queue'


class Singleton(type):
    """
    Singleton metaclass from http://stackoverflow.com/questions/31875/ \
        is-there-a-simple-elegant-way-to-define-singletons-in-python/33201#33201
    """
    def __init__(cls, name, bases, dict):
        super(Singleton, cls).__init__(name, bases, dict)
        cls.instance = None

    def __call__(cls,*args,**kw):
        if cls.instance is None:
            cls.instance = super(Singleton, cls).__call__(*args, **kw)
        return cls.instance




class RpcClient(object):
    __metaclass__ = Singleton
    """
    Generic proxy for distributed PRC servers.
    """
    def __init__(self, host='localhost', fast=False):
        """
        Connect to the broker, declare an exclusive queue to hold results of the
        RPC calls and start consuming messages on that queue.

        fast=True implies turning off forcing the blocking connection to stop
        and look to see if there are any frames from RabbitMQ in the read buffer
        which means that the client is not expecting RPC commands from the
        broker (which is generally true for us).
        """
        # Connect
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(
                host=host))

        self.channel = self.connection.channel()
        self._fast = fast
        if(self._fast):
            self.channel.force_data_events(False)

        # Create a queue with a default (i.e. random) name to hold results.
        result = self.channel.queue_declare(exclusive=True)
        self.callback_queue = result.method.queue

        self.channel.basic_consume(self.on_response, no_ack=True,
                                   queue=self.callback_queue)

        self.response = None
        self._rpccall_id = None
        return

    def on_response(self, ch, method, props, body):
        if self._rpccall_id == props.correlation_id:
            self.response = json.loads(body)
        return

    def is_ready(self):
        return(self.response is not None)

    def result(self):
        return(self.response)

    def wait(self):
        if(self.response is None):
            self.connection.process_data_events()
        return

    def call(self, fn, argv, kwds):
        _ = async_call(fn, argv, kwds, client=self)
        while self.response is None:
            self.connection.process_data_events()
        return(self.response)




def async_call(fn, argv=None, kwds=None, client=None, host='localhost',
    fast=False):
    """
    Invoke fn(*argv, **kwds) on the remote worker node, where kwds={'cwd': cwd}
    for now.

    For the meaning of the false flag, see the RpcClient documentation.

    Return an RpcClient instance. Remember that RpcClient implements the
    Singleton pattern hence all instances are aliases to each other.
    """
    if(argv is None):
        argv = []
    if(kwds is None):
        kwds = {}

    if(client is None):
        client = RpcClient(host=host, fast=fast)

    client.response = None
    client._rpccall_id = str(uuid.uuid4())
    client.channel.basic_publish(exchange='',
                               routing_key=QUEUE_NAME,
                               properties=pika.BasicProperties(
                                     reply_to = client.callback_queue,
                                     correlation_id = client._rpccall_id),
                               body=json.dumps([fn, argv, kwds]))
    return(client)






if(__name__ == '__main__'):
    client = RpcClient()

    try:
        fn = sys.argv[1]
    except:
        print('Usage: client.py <method> <arg list>')
        sys.exit(1)
    argv = sys.argv[2:]
    print " [x] Requesting %s(%s)" % (fn, ', '.join(argv))
    response = client.call(fn, argv)
    print " [.] Got %r" % (response,)
