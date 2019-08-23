# net-kafka-repeater
A kafka message repeater with data transferred via the Internet (TCP/UDP).

## Overview

The repeater is desgined to tranfer messages to a remote location and rewrite the message to the kafka message queue.
The sender would subscribe to a topic and when a message is fetched from the queue, it will send it out to the receiver.
When the receive get a message from sender it will deliver it to the local kafka message queue.

## Data packet structure

FLAG(2 bytes) + SIZE(4 bytes) + ID(4 bytes) + DATA

`FLAG` specifies the message type: a authentication request or a valid data message
`ID`   is a random `uint32` to identify the message for confirmation from receiver(Not implemented yet)


## Security

After a sender established a connection to the receiver it would send a login request with plain text password carried. If the password does not match the receiver end password, the connection would be dropped. Without a authentication success the sender can not send messages to the receiver.

The connection is not secured with encryption by default.

## Reliability

The sender would maintain a outgoing connectin to the receiver, and when the connection is broken, it'll retry again later.

If `order` is true, the message received on receiver end would be delivered to the kafka message queue one by one.

A kafka message delivery failure would normal trigger a new delivery but no message loss is not guaranteed.

However, the 'ack' flag is for this purpose(Not implemented yet). The idea is the sender needs to receive delivery success ack message for each kafka message before sending out the next message, but the performance and efficiency could be lower.

## Networking

Two types of networking solutions are provided:

+ Tcp: a valid tcp connection is proper for a stable network link

+ Kcp: a udp based connection for fast reliable delivery on a unstable network link with extra traffic cost

## Configuration

Samples

sender.json
```
{
  "mode": "sender",
  "pidFile": "sender.pid",
  "logFile": "sender.log",
  "workDir": ".",
  "exe": "net-kafka-repeater",
  "relay": {
    "address": "0.0.0.0:2000",
    "password": "Testing",
    "ack": true,
    "order": true,
    "mode": "tcp"
  },
  "topic": {
    "topic": "xxxxx",
    "order": true,
    "to_channel": true,
    "params": {
      "bootstrap.servers": "127.0.0.1:9092",
      "broker.address.family": "v4",
      "group.id": "test",
      "session.timeout.ms": 6000,
      "auto.offset.reset": "earliest"
    }
  }
}
```

receiver.json
```
{
  "mode": "receiver",
  "pidFile": "receiver.pid",
  "logFile": "receiver.log",
  "workDir": ".",
  "exe": "net-kafka-repeater",
  "relay": {
    "address": "0.0.0.0:2000",
    "password": "xxxxxxxxxx",
    "ack": true,
    "mode": "tcp"
  },
  "topic": {
    "topic": "xxxxx",
    "order": true,
    "to_channel": true,
    "params": {
      "bootstrap.servers": "127.0.0.1:9092"
    }
  }
}

```

