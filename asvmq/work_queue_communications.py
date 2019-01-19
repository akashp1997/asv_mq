# -*- coding: utf-8 -*-
'''
This module contains the Classes required to talk and send data via
the RabbitMQ server using Pika Client library on the AQMP 0-9-1.
The following module contains the Work Queue topologies and not the other ones.
'''

import pika
import asvprotobuf.std_pb2
from google.protobuf.json_format import MessageToJson



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
        """Initialises the channel create and also adds the logging
        publisher for sending message to logging systems"""
        Channel.create(self)
        self._channel.exchange_declare(exchange=LOG_EXCHANGE_NAME,\
         exchange_type="log")

    def publish(self, message):
        """Method for publishing the message to the MQ Broker"""
	log_message = asvprotobuf.std_pb2.Log()
        log_message.level = 0
        if not isinstance(message, self.type):
            raise ValueError("Please ensure that the message\
             passed to this method is of the same type as \
             defined during the Publisher declaration")
        if isinstance(message, str):
            log_message.name = "str"
        else:
            try:
                log_message.message = MessageToJson(message)
                message = message.SerializeToString()
            except:
                raise ValueError("Are you sure that the message \
                is Protocol Buffer message/string?")

        log_success = self._channel.basic_publish(exchange=LOG_EXCHANGE_NAME,\
         routing_key='', body=MessageToJson(log_message))
        if not log_success:
            raise RuntimeWarning("Cannot deliver message to logger")
        success = self._channel.basic_publish(exchange=self.exchange_name, \
         routing_key=self.topic, body=message)
        if not success:
	    raise pika.exceptions.ChannelError("Cannot deliver message to exchange")
class Subscriber(Channel):
    """Subscriber works on a callback function to process data
    and send it forward.
    To use it, create a new object using:
    asvmq.Subscriber(<topic_name>, <object_type>, <callback_func>,
    [<callback_args>], [<ttl>], [<hostname>], [<port>])
    and the program will go in an infinite loop to get data from the given topic name
    """
    def __init__(self, **kwargs):
        """Initialises the Consumer in RabbitMQ to receive messages"""
        topic_name = kwargs.get('topic_name')
        object_type = kwargs.get('object_type')
        callback = kwargs.get('callback')
        callback_args = kwargs.get('callback_args', '')
        ttl = kwargs.get('ttl', 1000)
        hostname = kwargs.get('hostname', 'localhost')
        port = kwargs.get('port', 5672)
        self._topic = topic_name
        self._object_type = object_type
        self._queue = None
        self._callback = callback
        self._callback_args = callback_args
        self._ttl = ttl
        Channel.__init__(self, exchange_name=DEFAULT_EXCHANGE_NAME,\
         exchange_type="topic", hostname=hostname, port=port)
    @property
    def type(self):
        """Returns the type of object to be strictly followed by the Publisher to send"""
        return self._object_type
    @property
    def ttl(self):
        """Returns the TTL parameter of the Queue"""
        return self._ttl
    @property
    def topic(self):
        """Returns the name of the topic as a variable"""
        return self._topic
    @property
    def queue_name(self):
        """Returns the Queue name if the queue exists"""
        if self._queue is not None:
            return self._queue.method.queue
        return None
    def __str__(self):
        """Returns the debug information of the Subscriber"""
        return "Subscriber on topic %s on %s:%d, of type %s" %\
         (self.topic, self.hostname, self.port, str(self.type))
    def create(self):
        """Creates a Temporary Queue for accessing Data from the exchange"""
        Channel.create(self)
        self._channel.exchange_declare(exchange=GRAPH_EXCHANGE_NAME,\
        exchange_type="fanout")
        self._queue = self._channel.queue_declare(arguments=\
        {"x-message-ttl": self.ttl}, exclusive=True)
        self._channel.queue_bind(exchange=self.exchange_name, \
        queue=self.queue_name, routing_key=self.topic)
        self._channel.basic_consume(self.callback, queue=self.queue_name)
        self._channel.start_consuming()
    def callback(self, channel, method, properties, body):
        """The Subscriber calls this function everytime
         a message is received on the other end"""
        del channel, properties
        if self.type is None or self.type == str:
            self._callback(body)
        else:
	     if isinstance(body, str):
                data = bytearray(body, "utf-8")
                body = bytes(data)
            _type = self.type
            if _type != str:
                try:
                    msg = _type.FromString(body)
		                except:
                    raise ValueError("Is the Message sent Protocol\
                    Buffers message or string?")
            self._channel.basic_ack(delivery_tag=method.delivery_tag)

            self._callback(msg, self._callback_args)

    