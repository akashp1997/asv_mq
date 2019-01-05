# SRM Autonomous Surface Vehicle Messaging Queue API
------
The team SRMASV uses this repository as an API to transfer data from one process or device to another process or device.

------
### Installation
------
To install just type in the following command:
`pip install asvmq`

There are three type of topologies involved:
1. [x] Topic Topology
2. [ ] Work Queue Topology
3. [ ] Merge Topology


## Instructions for Use:
#### Publisher
The `publisher` objects sends data over RabbitMQ broker, acting as the sole messenger to send data over the `asvmq` exchange.

The message has to either be an instance of a protobuf message, or it must be a string to be sent over, otherwise the publisher will generate an error and not send your message altogether.

The syntax of Publisher is  asvmq.Publisher(**topic_name**,**object_type**,hostname, port)
Here, the required arguments are bolded.

If the parameter passed to the publish method is a protobuf message, you need not serialise the message, but instead pass the protobuf object to the method itself.

To use the `publisher` object, write the code in the following manner:
```
import asvmq
import asvprotobuf.*_pb2
pub = asvmq.Publisher("hello", asvprotobuf.sensor_pb2.Imu)
while True:
    pub.publish("Hello world!!")
```
