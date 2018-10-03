# -*- coding: utf-8 -*-
'''
This module contains the Classes required to talk and send data via
the RabbitMQ server using Pika Client library on the AQMP 0-9-1.
The following module contains the Topic topologies and not the other ones.

You can use the Publisher object to send data and Subscriber object to receive it

'''

import pika

DEFAULT_EXCHANGE_NAME = "asvmq"

class Channel(object):
    """Internal class for Using Common Functionalities of RabbitMQ and Pika"""
    def __init__(self, exchange_name=DEFAULT_EXCHANGE_NAME, exchange_type="direct", hostname="localhost", port=5672):
        """Initialises a producer node for RabbitMQ.
        Base Class for the rest of the Communication Classes"""
        self._parameters = pika.ConnectionParameters(hostname, port)
        self._channel = None
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

    def close(self):
        """Destroys the channel only"""
        #Not safe to sse this method. Use del instead.
        if(self._channel==None):
            return
        if (self._channel.is_open==True):
            self._channel.close()

    def __del__(self):
        """Destroys the channel and closes the connection"""
        self.close()

    def __str__(self):
        """Returns the name of the exchange and the type, if called for"""
        return "Exchange %s is open on %s:%d and is of type %s" % (self.exchange_name, self.hostname, self.port, self._exchange_type)

    def create(self):
        """Initiates the Blocking Connection and the Channel for the process"""
        if (self._channel==None):
            connection = pika.BlockingConnection(self.params)
            self._channel = connection.channel()
        self._channel.exchange_declare(exchange=self.exchange_name, exchange_type=self.exchange_type)

class Publisher(Channel):
    """Class to use for publisher in Topic topology. Use exchange name as 'asvmq'
    To publish, first initialize the class in the following manner:
    obj = Publisher(<topic_name>,[<object_type>], [<hostname>], [<port>])
    and then publish the message of type as follows:
    obj.publish(object), where object=object_type defined
    """
    def __init__(self, topic_name, object_type=str, hostname="localhost", port=5672):
        self._object_type = object_type
        self._topic = topic_name
        Channel.__init__(self, exchange_name=DEFAULT_EXCHANGE_NAME, exchange_type="topic", hostname=hostname, port=port)

    @property
    def type(self):
        """Returns the type of object to be strictly followed by the Publisher to send"""
        return self._object_type

    @property
    def topic(self):
        """Returns the topic name specified during class creation"""
        return self._topic

    def __str__(self):
        """Returns the debug information of the publisher"""
        return "Publisher on topic %s on %s:%d, of type %s" % (self.topic, self.hostname, self.port, str(self.type))

    def close(self):
        """Destroys the object and deletes the exchange"""
        try:
            self._channel.exchange_delete(self.exchange_name, if_unused=True)
        except:
            pass
        Channel.close(self)

    def publish(self, message):
        """Method for publishing the message to the MQ Broker"""
        if(type(message)!=self.type):
            raise ValueError("Please ensure that the message passed to this method is of the same type as defined during the Publisher declaration")
        if(type(message)!=str):
            try:
                message = message.SerializeToString()
            except:
                raise ValueError("Are you sure that the message is Protocol Buffer message/string?")
        success = self._channel.basic_publish(exchange=self.exchange_name,routing_key=self.topic, body=message)
        if(not success):
            raise pika.exceptions.ChannelError("Cannot deliver message to exchange")

class Subscriber(Channel):
    """Subscriber works on a callback function to process data and send it forward.
    To use it, create a new object using:
    rospy.Subscriber(<topic_name>, <object_type>, <callback_func>, [<callback_args>], [<ttl>], [<hostname>], [<port>])
    and the program will go in an infinite loop to get data from the given topic name
    """
    def __init__(self, topic_name, object_type, callback, callback_args=None, ttl=100000, hostname="localhost", port=5672):
        """Initialises the Consumer in RabbitMQ to receive messages"""
        self._topic = topic_name
        self._object_type = object_type
        self._queue = None
        self._callback = callback
        self._callback_args = callback_args
        self._ttl = ttl
        Channel.__init__(self, exchange_name=DEFAULT_EXCHANGE_NAME, exchange_type="topic", hostname=hostname, port=port)

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
        if(self._queue!=None):
            return self._queue.method.queue

    def __str__(self):
        """Returns the debug information of the Subscriber"""
        return "Subscriber on topic %s on %s:%d, of type %s" % (self.topic, self.hostname, self.port, str(self.type))


    def create(self):
        """Creates a Temporary Queue for accessing Data from the exchange"""
        Channel.create(self)
        self._queue = self._channel.queue_declare(arguments={"x-message-ttl": self.ttl}, exclusive=True)
        self._channel.queue_bind(exchange=self.exchange_name, queue=self.queue_name, routing_key=self.topic)
        self._channel.basic_consume(self.callback, queue=self.queue_name)
        self._channel.start_consuming()

    def callback(self, channel, method, properties, body):
        """The Subscriber calls this function everytime a message is received on the other end"""
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
