# -*- coding: utf-8 -*-
import pika

class Publisher(object):
    def __init__(self, topic_name="", type_object=str, hostname="localhost", port=5672):
        self._parameters = pika.ConnectionParameters(hostname, port)
        self._connection = None
        self._channel = None
        self._name_exchange = topic_name
        self._object_type = type_object
        if (self._name_exchange):
            self._exchange_type = "fanout"
        else:
            self._exchange_type = "direct"

    def close(self):
        """Destroys the channel only"""
        if (self._channel!=None):
            self._channel.close()

    def __del__(self):
        """Destroys the channel and closes the connection"""
        self.close()

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

    @property
    def type(self):
        """Returns the type of object to be strictly followed by the Publisher to send"""
        return self._object_type

    def create(self):
        """Initiates the Blocking Connection and the Channel for the process"""
        if (self._channel==None):
            self._connection = pika.BlockingConnection(self._parameters)
            self._channel = self._connection.channel()
        self._channel.exchange_declare(exchange=self._name_exchange,exchange_type=self._exchange_type)

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
            print("Cannot deliver message to Exchange")
