#!/usr/bin/env python

from __future__ import print_function

import sys
import threading, logging, time
#import pydoop.hdfs as hdfs

from kafka import KafkaConsumer

# Kafka topic
#topic = sys.argv[1]
group = sys.argv[4]
topic = []
for value in sys.argv[1].split(","):
    topic.append(value)

server = []
for value in sys.argv[3].split(","):
    server.append(sys.argv[2]+":"+value)



class Consumer(threading.Thread):
    daemon = True

    def run(self):
        consumer = KafkaConsumer(bootstrap_servers=server,
                                 group_id= group,
                                 auto_offset_reset='earliest')
        consumer.subscribe(topic)

        for message in consumer:
            print (message)


def main():
    threads = [
        Consumer()
    ]

    for t in threads:
        t.start()

    time.sleep(100)

if __name__ == "__main__":

    if len(sys.argv) != 5:
        print("Usage: kafkaConsumer.py <topic descriptor> <topic list> <broker server> <broker port list> <group>", file=sys.stderr)
        exit(-1)

    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
        )
    main()