import argparse
import atexit
import json
import logging
import random
import time
import sys
from confluent_kafka import Producer
from datetime import datetime


logging.basicConfig(
    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.INFO,
    handlers=[
        logging.FileHandler("transaction_producer.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger()

TRANSACTION_TYPES = ['atm', 'wire', 'check', 'plp']
DIRECTION_TYPES = ['dep', 'credit']

# Kafka delivery callback
class ProducerCallback:
    def __init__(self, record, log_success=False):
        self.record = record
        self.log_success = log_success

    def __call__(self, err, msg):
        if err:
            logger.error(f'Error producing record: {self.record}')
        elif self.log_success:
            logger.info(f'Produced to topic={msg.topic()} partition={msg.partition()} offset={msg.offset()} | record={self.record}')


def main(args):
    logger.info(f'Starting transaction producer: Sending {args.count} messages')

    conf = {
        'bootstrap.servers': args.bootstrap_server,
        'linger.ms': 200,
        'client.id': 'txn-producer',
        'partitioner': 'murmur2_random'
    }

    producer = Producer(conf)
    atexit.register(lambda p=producer: p.flush())

    txn_id = 1
    for i in range(args.count):
        transaction_type = random.choice(TRANSACTION_TYPES)
        dep_or_withdraw = random.choice(DIRECTION_TYPES)
        base_amount = round(random.uniform(50, 1000), 2)
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        transaction = {
            "user_id": random.randint(10000000, 100000000),
            "transaction_amount": base_amount,
            "dep_or_withdraw": dep_or_withdraw,
            "timestamp_str": timestamp,
            "transaction_id": txn_id,
            "transaction_type": transaction_type
        }
        
        producer.produce(
            topic=args.topic,
            value=json.dumps(transaction),
            on_delivery=ProducerCallback(transaction, log_success=True)
        )

        txn_id += 1
        producer.poll(0)
        time.sleep(0.01)  

    producer.flush()
    logger.info("Finished producing messages.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--bootstrap-server', default='localhost:9092')
    parser.add_argument('--topic', default='transactions-topic')
    parser.add_argument('count', type=int, help='Number of messages to produce')
    args = parser.parse_args()

    main(args)

