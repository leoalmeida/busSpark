#!/usr/bin/env python

from __future__ import absolute_import, print_function

import sys
import threading, logging, time
import pydoop.hdfs as hdfs

from datetime import datetime
from kafka import KafkaProducer
from collections import defaultdict

class Producer(threading.Thread):
    daemon = True

    def run(self):
        print('Initializing producer...')
        print('---> server {0} '.format(server))

        producer = KafkaProducer(bootstrap_servers=server)
        with hdfs.open(filename) as reader:
            # Read all from line
            i = 0
            for gpsline in reader:
                columns = gpsline.split(",")
                if (not lines[columns[2]]):
                    continue
                else:
                    i += 1
                    if (not (i%2000)):
                        print(i)

                producer.send(topic + columns[2], key=columns[2], value=gpsline)

                # block until all async messages are sent
                if i%batchsize == 0:
                    producer.flush()

            print(i)
        print(" Fim: " + datetime.now().strftime('%d/%m/%Y %H:%M:%S'))
        producer.close()
        exit()

def hash(): return defaultdict(hash)

def main():
    threads = [
        Producer()
    ]

    for t in threads:
        t.start()

    time.sleep(60000)

if __name__ == "__main__":
    print(" Inicio: " + datetime.now().strftime('%d/%m/%Y %H:%M:%S'))

    if len(sys.argv) != 6:
        print("Usage: kafkaProducer.py <filename> <topic> <broker> <lines>", file=sys.stderr)
        exit(-1)

    filename = sys.argv[1]  # HDFS input filename
    topic = sys.argv[2]     # Kafka default topic part
    server = []             # kafka used servers
    for value in sys.argv[4].split(","):
        server.append(sys.argv[3] + ":" + value)

    lines = hash()          # requested lines
    for value in sys.argv[5].split(","):
        lines[value] = True

    # static variables
    ack = '1'
    lingerms = 20
    batchsize = 16384
    retries = 10
    async = True

    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.WARNING
    )
    main()