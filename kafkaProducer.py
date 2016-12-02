#!/usr/bin/env python

from __future__ import absolute_import, print_function

import sys
import threading, logging, time
import pydoop.hdfs as hdfs

from kafka import KafkaProducer, TopicPartition

from kafka.errors import KafkaTimeoutError

from collections import namedtuple


#filename
filename = sys.argv[1]
# Kafka topic
topic = sys.argv[2]
#kafka server
server = sys.argv[3]
ack = '1'
lingerms = 10
batchsize = 50000
retries = 10
async = True

record = namedtuple("record", ["key", "value"])

class Producer(threading.Thread):
    daemon = True

    def run(self):


        print('Initializing producer...')

        producer = KafkaProducer(bootstrap_servers=server, acks=ack, linger_ms=lingerms, batch_size=batchsize)

        with hdfs.open(filename) as reader:
            # Read all from line
            i = 0
            while True:
                for gpsline in reader:
                    i += 1
                    columns = gpsline.split(",")

                    item = record(key=columns[2], value=gpsline)

                    stop = False
                    while not stop:

                        #print(item.key)

                        try:
                            producer.send(topic + item.key, key=item.key, value=item.value)

                            #record_metadata = future.get(timeout=10)

                            #print(record_metadata)
                            #print(i)

                            stop = True
                        except KafkaTimeoutError:
                            print ("Couldn't find topic. I`ll retry after some period!")
                            time.sleep(1000)


                    # Every 16384 records, block until everything is
                    # okay. Only needs to be called if async is True
                    if (async is True) and (i % batchsize == 0):
                        print(i)
                        producer.flush()

def main():
    threads = [
        Producer()
    ]

    for t in threads:
        t.start()

    time.sleep(60000)

if __name__ == "__main__":

    if len(sys.argv) != 4:
        print("Usage: kafkaProducer.py <filename> <topic> <broker>", file=sys.stderr)
        exit(-1)

    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.WARNING
        )
    main()