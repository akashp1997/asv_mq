# -*- coding: utf-8 -*-
'''
This module contains the Classes required to talk and send data via
the RabbitMQ server using Pika Client library on the AQMP 0-9-1.
The following module contains the Work Queue topologies and not the other ones.
'''

import pika

class Queue(object):
    """Base class for storing the common Functionalities between Worker and Producers"""
    def __init__(self, queue_name, hostname="localhost", port=5672, durable=False):
        self._parameters = pika.ConnectionParameters(hostname, port)
        self._channel = None
        self._queue_name = queue_name
        self._queue = None
        self._durable = durable
        self.create()

    @property
    def params(self):
        """Returns the parameters object"""
        return self._parameters

    @property
    def hostname(self):
        """Returns the hostname of the parameters"""
        return self._parameters.host

    @property
    def port(self):
        """Returns the port number of the parameters"""
        return self._parameters.port

    @property
    def queue_name(self):
        """Returns the queue name"""
        return self._queue_name

    @property
    def is_durable(self):
        """Returns if the queue is durable or not"""
        return self._durable

    def __del__(self):
        """Destroys the object and the connection"""
        self.close()

    def __str__(self):
        """Returns the debug information of the Queue"""
        return "Queue named %s on %s:%d and durable=%s" % (self.queue_name, self.hostname, self.port, str(self.is_durable))

    def create(self):
        """Creates a new Work Queue in the RabbitMQ server"""
        connection = pika.BlockingConnection(self._parameters)
        self._channel = connection.channel()
        self._queue = self._channel.queue_declare(queue=self._queue_name, durable=self._durable)

    def close(self):
        """Closes the channel and the connection"""
        if (self._channel!=None):
            return
        if (self._channel.is_open==True):
            self._channel.close()

class Tasker(Queue):
    """This tasker sends messages one by one to the workers in the Round Robin Dispatch
    Use it just like a publisher"""
    def __init__(self, queue_name, object_type, persistant=True, hostname="localhost", port=5672, durable=False):
        self._persistant = persistant
        self._object_type = object_type
        Queue.__init__(self, queue_name=queue_name, hostname=hostname, port=port, durable=durable)

    @property
    def is_persistant(self):
        """Returns if the messages are persistant or not"""
        return self._persistant

    @property
    def object_type(self):
        """Returns the type of objects that can be sent"""
        return self._object_type

    def __str__(self):
        """Returns the debug information of the tasker"""
        return "Queue %s on %s:%d as %s, durable=%s, persistant=%s" % (self.queue_name, self.hostname, self.port, self.object_type, str(self.is_durable), str(self.is_persistant))

    def publish(self, message):
        """Sends the message to the Work Queue"""
        if (self.type==str):
            body = message
        else:
            try:
                body = message.SerializeToString()
            except:
                raise ValueError("Are you sure that the object is a Protocol Buffers/String message?")
        properties = None
        if(self.is_persistant):
            properties = pika.BasicProperties(delivery_mode=2)
        try:
            self._channel.basic_publish(exchange='', routing_key=self.queue_name, body=body, properties=properties)
        except:
            raise pika.exceptions.ChannelError("Could not send the message to the queue.")

class Worker(Queue):
    """This worker can be spawned multiple times to do parallel processing"""
    def __init__(self, queue_name, object_type, callback, callback_args=None, prefetch_qos=1, hostname="localhost", port=5672, durable=False):
        self._object_type = object_type
        self._prefetch_qos = prefetch_qos
        self._callback = callback
        self._callback_args = callback_args
        Queue.__init__(self, queue_name=queue_name, hostname=hostname, port=port, durable=durable)

    @property
    def prefetch(self):
        """Returns the number of messages prefetched by the worker"""
        return self._prefetch_qos

    @property
    def object_type(self):
        """Returns the type of object specified at object creation time"""
        return self._object_type

    def create(self):
        """Starts the worker and starts consuming messages"""
        Queue.create(self)
        self._channel.basic_qos(prefetch_count=self.prefetch)
        self._channel.basic_consume(self.callback, queue=self.queue_name)
        self._channel.start_consuming()

    def callback(self, channel, method, properties, body):
        """The Worker calls this function everytime a message is received on this end"""
        if(self.type==None or self.type==str):
            self._callback(body)
        else:
            try:
                if(type(body)==str):
                    data = bytearray(body, "utf-8")
                    body = bytes(data)
                _type = self.type
                msg = _type.FromString(body)
                self._callback(msg, self._callback_args)
            except:
                raise ValueError("Is the Message sent Protocol Buffers message or string?")
        self._channel.basic_ack(delivery_tag=method.delivery_tag)
