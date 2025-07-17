from confluent_kafka import Consumer, KafkaException
import sys

conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'print-all-messages-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
}

consumer = Consumer(conf)
consumer.subscribe(['transactions-topic'])

try:
    print("Consuming messages from 'transactions-topic'...\n")
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())

        print(msg.value().decode('utf-8'))

except KeyboardInterrupt:
    print("Stopped by user")
finally:
    consumer.close()
