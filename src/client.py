#!/usr/bin/env python
import json
import sys
import uuid

import pika



# Constants
QUEUE_NAME = 'rpc_queue'



class RpcClient(object):
    """
    Generic proxy for distributed PRC servers.
    """
    def __init__(self):
        """
        Connect to the broker, declare an exclusive queue to hold results of the
        RPC calls and start consuming messages on that queue.
        """
        # Connect
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(
                host='localhost'))

        self.channel = self.connection.channel()

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

    def call(self, fn, argv, cwd=None):
        self.response = None
        self._rpccall_id = str(uuid.uuid4())
        self.channel.basic_publish(exchange='',
                                   routing_key=QUEUE_NAME,
                                   properties=pika.BasicProperties(
                                         reply_to = self.callback_queue,
                                         correlation_id = self._rpccall_id),
                                   body=json.dumps([fn, argv, {'cwd': cwd}]))
        while self.response is None:
            self.connection.process_data_events()
        return(self.response)




def async_call(fn, argv, cwd=None):
    client = RpcClient()

    client.response = None
    client._rpccall_id = str(uuid.uuid4())
    client.channel.basic_publish(exchange='',
                               routing_key=QUEUE_NAME,
                               properties=pika.BasicProperties(
                                     reply_to = client.callback_queue,
                                     correlation_id = client._rpccall_id),
                               body=json.dumps([fn, argv, {'cwd': cwd}]))
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
