# -*- coding: utf-8 -*-
'''
This module contains the Classes required to talk and send data via
the RabbitMQ server using Pika Client library on the AQMP 0-9-1.
The following module contains the Topic topologies only.

You can use the Publisher object to send data and Subscriber object to receive it

'''

import uuid
import sys
import traceback
import pika
import asvprotobuf.std_pb2
from google.protobuf.json_format import MessageToJson

DEFAULT_EXCHANGE_NAME = "asvmq"
LOG_EXCHANGE_NAME = "logs"
GRAPH_EXCHANGE_NAME = "graph"

channel = None

#TODO: Create a subscriber for reading a particular level of logging and displaying on screen

class Channel:
    """Internal class for Using Common Functionalities of RabbitMQ and Pika"""
    def __init__(self, **kwargs):
        """Initialises a producer node for RabbitMQ.
        Base Class for the rest of the Communication Classes"""
        exchange_name = kwargs.get('exchange_name', DEFAULT_EXCHANGE_NAME)
        exchange_type = kwargs.get('exchange_type', 'direct')
        self._node_name = kwargs.get('node_name', 'node')+str(uuid.uuid4())
        hostname = kwargs.get('hostname', 'localhost')
        port = kwargs.get('port', 5672)
        self._parameters = pika.ConnectionParameters(hostname, port)
        self._exchange_name = exchange_name
        self._exchange_type = exchange_type
        self.create()

    @property
    def params(self):
        """Returns the parameters of the Blocking Connection"""
        return self._parameters

    @property
    def hostname(self):
        """Returns the hostname provided for the Broadcaster"""
        return self._parameters.host

    @property
    def port(self):
        """Returns the port provided for the Broadcaster"""
        return self._parameters.port

    @property
    def exchange_name(self):
        """Returns the topic name provided for the Broadcaster"""
        return self._exchange_name

    @property
    def exchange_type(self):
        """This method returns the exchange type"""
        return self._exchange_type

    @property
    def node_name(self):
        """Returns the name of the node that was used during the initialisation"""
        return self._node_name

    def __str__(self):
        """Returns the name of the exchange and the type, if called for"""
        return "Exchange %s is open on %s:%d and is of type %s" % \
        (self.exchange_name, self.hostname, self.port, self._exchange_type)

    def create(self):
        """Initiates the Blocking Connection and the Channel for the process"""
        global channel
        if channel is None:
            connection = pika.BlockingConnection(self.params)
            channel = connection.channel()
        channel.exchange_declare(exchange=self.exchange_name,\
         exchange_type=self.exchange_type)

class Publisher(Channel):
    """Class to use for publisher in Topic topology. Use exchange name as 'asvmq'
    To publish, first initialize the class in the following manner:
    obj = Publisher(<topic_name>,[<object_type>], [<hostname>], [<port>])
    and then publish the message of type as follows:
    obj.publish(object), where object=object_type defined
    """
    def __init__(self, **kwargs):
        topic_name = kwargs.get('topic_name')
        object_type = kwargs.get('object_type', str)
        hostname = kwargs.get('hostname', 'localhost')
        port = kwargs.get('port', 5672)
        node_name = kwargs.get('node_name', 'pub_%s' % \
        (str(object_type).split("\'")[1]))
        self._object_type = object_type
        self._topic = topic_name
        Channel.__init__(self, exchange_name=DEFAULT_EXCHANGE_NAME,\
         exchange_type="topic", hostname=hostname, port=port, node_name=node_name)

    @property
    def type(self):
        """Returns the type of object to be strictly followed by
        the Publisher to send"""
        return self._object_type

    @property
    def topic(self):
        """Returns the topic name specified during class creation"""
        return self._topic

    @property
    def node_name(self):
        """Returns the node name of the publisher"""
        return self._node_name

    def __str__(self):
        """Returns the debug information of the publisher"""
        return "Publisher on topic %s on %s:%d, of type %s" %\
         (self.topic, self.hostname, self.port, str(self.type))

    def create(self):
        global channel
        """Initialises the channel create and also adds the logging
        publisher for sending message to logging systems
        """
        Channel.create(self)
        channel.exchange_declare(exchange=LOG_EXCHANGE_NAME,\
         exchange_type="fanout")

    def publish(self, message):
        """Method for publishing the message to the MQ Broker and also send
        a message to log exchange for logging and monitoring"""
        global channel
        log_message = asvprotobuf.std_pb2.Log()
        log_message.level = 0
        message.header.sender = self.node_name
        if not isinstance(message, self.type):
            raise ValueError("Please ensure that the message\
passed to this method is of the same type as \
defined during the Publisher declaration")
        if isinstance(message, str):
            log_message.name = "str"
        else:
            try:
                log_message.name = str(type(message)).split("'")[1]
                log_message.message = MessageToJson(message).replace("\n", "")\
                .replace("\"", "'")
                message = message.SerializeToString()
            except:
                raise ValueError("Are you sure that the message \
                is Protocol Buffer message/string?")

        log_success = channel.basic_publish(exchange=LOG_EXCHANGE_NAME,\
         routing_key='', body=MessageToJson(log_message).replace("\n", "")\
         .replace("\'", "'"))
        if not log_success:
            log_warn("Cannot deliver message to logger")
        success = channel.basic_publish(exchange=self.exchange_name, \
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
        ttl = kwargs.get('ttl', 10)
        hostname = kwargs.get('hostname', 'localhost')
        port = kwargs.get('port', 5672)
        node_name = kwargs.get('node_name', 'sub_%s' % \
        (str(object_type).split("\'")[1]))
        self._topic = topic_name
        self._object_type = object_type
        self._queue = None
        self._last_timestamp = 0
        self._callback = callback
        self._callback_args = callback_args
        self._ttl = ttl
        Channel.__init__(self, exchange_name=DEFAULT_EXCHANGE_NAME,\
         exchange_type="topic", hostname=hostname, port=port, node_name=node_name)

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
        global channel
        """Creates a Temporary Queue for accessing Data from the exchange"""
        Channel.create(self)
        channel.exchange_declare(exchange=GRAPH_EXCHANGE_NAME,\
        exchange_type="fanout")
        self._queue = channel.queue_declare(arguments=\
        {"x-message-ttl": self.ttl}, exclusive=True)
        channel.queue_bind(exchange=self.exchange_name, \
        queue=self.queue_name, routing_key=self.topic)
        channel.basic_consume(self.callback, queue=self.queue_name)

    def callback(self, _channel, method, properties, body):
        """The Subscriber calls this function everytime
         a message is received on the other end and publishes a message
         to the graph exchange to form the barebones of graph"""
        del _channel, properties
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
            channel.basic_ack(delivery_tag=method.delivery_tag)
            graph_message = asvprotobuf.std_pb2.Graph()
            graph_message.sender = msg.header.sender
            graph_message.msg_type = str(self.type).split("\'")[1]
            graph_message.receiver = self._node_name
            curr_timestamp = msg.header.stamp
            if self._last_timestamp == 0:
                graph_message.freq = 0
            else:
                if curr_timestamp-self._last_timestamp != 0:
                    graph_message.freq = 1/(curr_timestamp-self._last_timestamp)
            self._last_timestamp = curr_timestamp
            if graph_message.freq < 0:
                graph_message.freq = 0
            graph_success = channel.basic_publish(exchange=GRAPH_EXCHANGE_NAME,\
             routing_key='', body=MessageToJson(graph_message).replace("\n", "")\
             .replace("\'", "'"))
            if not graph_success:
                log_warn("The messages cannot be sent to graph.")
            self._callback(msg, self._callback_args)

def spin(start=True):
    """This function will start the loop of Pika to start consuming"""
    global channel
    if channel is None:
        return
    if start:
        channel.start_consuming()
    else:
        channel.stop_consuming()

def _log(string, **kwargs):
    """This function is a base function used to send log messages
    to the RabbitMQ/ASVMQ logging system"""
    level = kwargs.pop("level", 0)
    global channel
    kwargs["exchange_name"] = LOG_EXCHANGE_NAME
    kwargs["exchange_type"] = "fanout"
    _channel = Channel(**kwargs)
    log_message = asvprotobuf.std_pb2.Log()
    log_message.level = level
    log_message.name = "str"
    log_message.message = string
    log_message = MessageToJson(log_message).replace("\n", "").replace("\'", "'")
    channel.basic_publish(exchange=LOG_EXCHANGE_NAME, \
    body=log_message, routing_key='')
    if level == 0:
        sys.stdout.write("\x1b[37m[INFO]%s\n\x1b[39m" % string)
    elif level == 1:
        sys.stdout.write("\x1b[33m[WARN]%s\n\x1b[39m" % string)
    elif level == 2:
        sys.stdout.write("\x1b[34m[DEBUG]%s\n\x1b[39m" % string)
    else:
        sys.stdout.write("\x1b[31m[FATAL]%s\n\x1b[39m" % string)

def log_info(string):
    """This function uses the _log function to send log messages at
    info level i.e at the user readable level(stdout)"""
    kwargs = {}
    kwargs["level"] = 0
    _log(string, **kwargs)

def log_warn(string):
    """This function uses the _log function to send log messages at
    warning level i.e at the exception that is not fatal"""
    kwargs = {}
    kwargs["level"] = 1
    _log(string, **kwargs)

def log_debug(string):
    """This function uses the _log function to send log messages at
    debug level i.e at the debugging purposes level"""
    kwargs = {}
    kwargs["level"] = 2
    _log(string, **kwargs)

def log_fatal(string):
    """This function uses the _log function to send log messages at
    fatal error level i.e at the irrecoverable exceptions"""
    raise Exception(string)

def init():
    """Initialises the exception handling of asvmq"""
    def excepthook(exctype, excvalue, exctb):
        err_traceback = traceback.format_exception(exctype, excvalue, exctb)
        _log("".join(err_traceback).strip(), level=3)
    sys.excepthook = excepthook
