#!/usr/bin/env python

from confluent_kafka import Consumer

# Set configuration parameters
config = {
    'bootstrap.servers': 'pkc-p11xm.us-east-1.aws.confluent.cloud:9092',
    'sasl.username':     '3FEGRT53MX7L2BFA',
    'sasl.password':     '6PclszbMQOGlGhrPdiu4WL4wJPxzm/KY1+NSzQDL69mZv98Rg8UDjQv4txI298A4',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms':   'PLAIN',
    'group.id':          'bdis_kafka',
    'auto.offset.reset': 'earliest'
}

# Create Consumer instance
consumer = Consumer(config)

# Subscribe to topic
topic = "bdis_kafka_cluster_01_topic_01"
consumer.subscribe([topic])

# Poll for new messages from Kafka and print them.
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            # Initial message consumption may take up to
            # `session.timeout.ms` for the consumer group to
            # rebalance and start consuming
            print("Waiting...")
        elif msg.error():
            print("ERROR: %s".format(msg.error()))
        else:
            # Extract the (optional) key and value, and print.
            print("Consumed event from topic {topic}: temperature = {key:12}, humidity = {value:12}".format(
                topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))
except KeyboardInterrupt:
    pass
finally:
    # Leave group and commit final offsets
    consumer.close()

