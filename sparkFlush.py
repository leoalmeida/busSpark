from __future__ import print_function

import sys
import findspark
import itertools
import json
findspark.init()

try:
    from pyspark import SparkConf, SparkContext

    from pyspark.sql import SQLContext
    from pyspark.streaming import StreamingContext
    from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType,StructField,StringType
    from pyspark.sql import Row
    from collections import OrderedDict
    from math import sqrt,pow

except ImportError as e:
    print("Can not import Spark Modules", e)
    sys.exit(1)

config = {
    "spark.app.name": "HDFSFlush",
    "spark.driver.cores": "1",
    "spark.master": "local",
    "spark.python.profile": "false",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.kryoserializer.buffer": "24m",
    "spark.kryo.referenceTracking": "true",
    "spark.ui.enabled": "true",
    "spark.executor.extraClassPath": "lib/sqlite-jdbc-3.15.1.jar",
    "spark.driver.extraClassPath": "lib/sqlite-jdbc-3.15.1.jar",
    "spark.jars": "lib/sqlite-jdbc-3.15.1.jar"
}

dbparams = {'url': "jdbc:sqlite:data/bus.db", 'properties': {"driver": "org.sqlite.JDBC"}, 'mode': "append"}

conf = SparkConf()
for key, value in config.iteritems():
    conf = conf.set(key, value)

spark = SparkSession.builder.config(conf=conf).getOrCreate()

if __name__ == "__main__":
    print("---> File Flush start")

    if len(sys.argv) != 3:
        print("Usage: sparkFlush.py <output> <topic>", file=sys.stderr)
        exit(-1)

    topic = "sparkstream" + sys.argv[2]
    output = sys.argv[1] + '/' + topic

    print('---> Topics {0} '.format(topic))
    print('---> URL {0}'.format(dbparams['url']))
    print('---> Mode {0}'.format(dbparams['mode']))
    print('---> Properties {0}'.format(dbparams['properties']))

    # Get static info from dbtable
    df = spark.read.jdbc(url=dbparams['url'], table=topic, properties=dbparams['properties'])
    df.write.json(output, mode='append')