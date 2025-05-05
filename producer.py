#!/usr/bin/env python

from random import choice
from confluent_kafka import Producer
import time
import board
import adafruit_dht

# Initialize the dht device
dhtDevice = adafruit_dht.DHT22(board.D4, use_pulseio=False)

# Set configuration parameters
config = {
    'bootstrap.servers': 'pkc-p11xm.us-east-1.aws.confluent.cloud:9092',
    'sasl.username':     '3FEGRT53MX7L2BFA',
    'sasl.password':     '6PclszbMQOGlGhrPdiu4WL4wJPxzm/KY1+NSzQDL69mZv98Rg8UDjQv4txI298A4',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms':   'PLAIN',
    'acks':              'all'
}

# Optional per-message delivery callback (triggered by poll() or flush())
# when a message has been successfully delivered or permanently
# failed delivery (after retries).
def delivery_callback(err, msg):
    if err:
        print('ERROR: Message failed delivery: {}'.format(err))
    else:
        print("Produced event to topic {topic}: temperature = {key:12}, humidity = {value:12}".format(
            topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))


def main():
    topic = "bdis_kafka_cluster_01_topic_01"
    # Create Producer instance
    producer = Producer(config)
    while True:
      try:
        producer.produce(topic, str(dhtDevice.humidity), str(dhtDevice.temperature), callback=delivery_callback)
        # Block until the messages are sent.
        producer.poll(10000)
        producer.flush()
      except RuntimeError as error:
        print(error.args[0])
        time.sleep(2.0)
        continue
      except Exception as error:
        dhtDevice.exit()
        raise error  
      time.sleep(2.0)


main()
    
