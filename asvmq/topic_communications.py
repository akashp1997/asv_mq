# -*- coding: utf-8 -*-
import pika

class Channel(object):
    """Internal class for Using Common Functionalities of RabbitMQ and Pika"""
    def __init__(self, topic_name, exchange_type="direct", hostname="localhost", port=5672):
        """Initialises a producer node for RabbitMQ.
        Base Class for the rest of the Communication Classes"""
        self._parameters = pika.ConnectionParameters(hostname, port)
        self._connection = None
        self._channel = None
        self._name_exchange = topic_name
        self._exchange_type = exchange_type

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
    def topic(self):
        """Returns the topic name provided for the Broadcaster"""
        return self._name_exchange

    @property
    def exchange_type(self):
        """This method returns the exchange type"""
        return self._exchange_type

    def close(self):
        """Destroys the channel only"""
        #Not Safe to Use the method. Use del instead
        if(self._channel==None):
            return
        if (self._channel.is_open==True):
            self._channel.close()

    def __del__(self):
        """Destroys the channel and closes the connection"""
        self.close()

    def create(self):
        """Initiates the Blocking Connection and the Channel for the process"""
        if (self._channel==None):
            self._connection = pika.BlockingConnection(self.params)
            self._channel = self._connection.channel()
        if(self.topic==""):
            raise ValueError("The topic name cannot be None")
        self._channel.exchange_declare(exchange=self.topic, exchange_type=self.exchange_type)

class Publisher(Channel):
    def __init__(self, topic_name, type_object=str, hostname="localhost", port=5672):
        self._object_type = type_object
        Channel.__init__(self, topic_name, exchange_type="fanout", hostname=hostname, port=port)

    @property
    def type(self):
        """Returns the type of object to be strictly followed by the Publisher to send"""
        return self._object_type

    def close(self):
        """Destroys the object and deletes the exchange"""
        #Not Safe to Use the method. Use del instead
        self._channel.exchange_delete(self.topic)
        Channel.close(self)

    def __del__(self):
        """Destructor function for the Publisher"""
        self.close()

    def publish(self, message):
        """Method for publishing the message to the MQ Broker"""
        if(type(message)!=self.type):
            raise ValueError("Please ensure that the message passed to this method is of the same type as defined during the exchange declaration")
        if(type(message)!=str):
            try:
                message = message.SerializeToString().decode()
            except:
                raise ValueError("Are you sure that the message is Protocol Buffer message/string?")
        success = self._channel.basic_publish(exchange=self.topic,routing_key="", body=message)
        if(not success):
            raise pika.exceptions.ChannelError("Cannot deliver message to Exchange")

class Subscriber(Channel):
    def __init__(self, topic_name, type_object, callback, callback_args=None, ttl=1000, hostname="localhost", port=5672):
        """Initialises the Consumer in RabbitMQ to receive messages"""
        self._object_type = type_object
        self._queue = None
        self._callback = callback
        self._callback_args = callback_args
        self._ttl = ttl
        Channel.__init__(self, topic_name, exchange_type="fanout", hostname=hostname, port=port)

    @property
    def type(self):
        """Returns the type of object to be strictly followed by the Publisher to send"""
        return self._object_type

    @property
    def ttl(self):
        """Returns the TTL parameter of the Queue"""
        return self._ttl

    @property
    def queue_name(self):
        """Returns the Queue name if the queue exists"""
        if(self._queue!=None):
            return self._queue.method.queue

    def create(self):
        """Creates a Temporary Queue for accessing Data from the exchange"""
        Channel.create(self)
        self._queue = self._channel.queue_declare(exclusive=True, arguments={"x-message-ttl": self.ttl})
        self._channel.queue_bind(exchange=self.topic, queue=self.queue_name)
        self._channel.basic_consume(self.callback, queue=self.queue_name, no_ack=True)

    def callback(self, channel, method, properties, body):
        if(self.type==None or self.type==str):
            self._callback(body)
        else:
            try:
                obj = self.type().ParseFromString(Body)
                self._callback(obj, callback_args)
            except:
                raise ValueError("Is the Message sent Protocol Buffers message or string?")

    def __del__(self):
        """Destructor function for the Publisher"""
        self.close()
