from __future__ import print_function

import sys
import findspark
import itertools
import json
findspark.init()

try:
    import pandas as pd
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
    "spark.app.name": "TopicsProcess",
    "spark.driver.cores": "3",
    "spark.master": "local",
    "spark.executor.memory": "6g",
    "spark.driver.memory": "6g",
    "spark.driver.maxResultSize": "4g",
    "spark.kryoserializer.buffer.max": "1g",
    "spark.python.profile": "false",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.kryoserializer.buffer": "24m",
    "spark.kryo.referenceTracking": "true",
    "spark.ui.enabled": "true",
    "spark.executor.extraClassPath": "lib/sqlite-jdbc-3.15.1.jar",
    "spark.driver.extraClassPath": "lib/sqlite-jdbc-3.15.1.jar",
    "spark.jars": "lib/sqlite-jdbc-3.15.1.jar,lib/spark-streaming-kafka-0-8-assembly_2.10-2.0.2.jar"
}

dbparams = {'url': "jdbc:sqlite:data/bus.db", 'properties': {"driver": "org.sqlite.JDBC"}, 'mode': "append"}

conf = SparkConf()
for key, value in config.iteritems():
    conf = conf.set(key, value)

spark = SparkSession.builder.config(conf=conf).getOrCreate()
ssc = StreamingContext(spark.sparkContext, 60)
sqlContext = SQLContext(spark.sparkContext)

# The schema is encoded in a JSON string.
schemaString = "dtservidor,dtavl,idlinha,latitude,longitude,idavl,evento,idponto,sentido,shape_idx,shape_lat,shape_lon,shape_distance,linha"
schema = ["dtservidor","dtavl","idlinha","latitude","longitude","idavl","evento","idponto","sentido","shape_idx","shape_lat","shape_lon","shape_distance","linha"]
dfSchema = StructType(StructField(i, StringType(), True) for (i) in schemaString.split(","))

#-----------------------------------------------------------------

if len(sys.argv) != 5:
    print("Usage: spark_filesplit.py <zk> <topic>", file=sys.stderr)
    exit(-1)

brokers = []
for value in sys.argv[3].split(","):
    brokers.append(sys.argv[2]+":"+value)
brokers = ','.join(brokers)

lines = sys.argv[4]
topic = "sparkstream" + sys.argv[4]
# tolerancia = sys.argv[5]
output = sys.argv[1] + '/' + topic

#-----------------------------------------------------------------

# Get static info from dbtable
df = sqlContext.read.format("jdbc").option("url", dbparams['url']).option("dbtable", "rotas").load()
filteredGroup = df.filter(df.line_id == lines).rdd.collect()

def process((k, v)):
    value = {i: j.strip() for (i, j) in itertools.izip_longest(schemaString.split(","), v.split(","), fillvalue='0')}
    return value

def convert_to_row(d):
    return Row(**OrderedDict(d.items()))

def modify(point):
    min = 360
    indice = 0
    for rdd in filteredGroup:
        indice += 1

        distance = sqrt(pow(float(rdd.shape_pt_lat) - float(point.latitude), 2) + pow(
            float(rdd.shape_pt_lon) - float(point.longitude), 2))
        if distance < min:
            min = distance
            shape_distance = distance
            sentido = rdd.direction_id
            shape_idx = indice
            shape_lat = rdd.shape_pt_lat
            shape_lon = rdd.shape_pt_lon
            route_id = rdd.route_id

    return (point.dtservidor, point.dtavl, point.idlinha,point.latitude, point.longitude, point.idavl,point.evento, point.idponto, sentido, shape_idx, shape_lat, shape_lon, shape_distance, route_id)

def streamrdd_to_df(srdd):
    if not srdd.isEmpty():
        points = srdd.map(modify);
        print(points);
        newdf = spark.createDataFrame(points, schema);
        newdf.write.json(output, mode='append');
        # newdf.write.jdbc(url=dbparams['url'], table=topic, mode=dbparams['mode'], properties=dbparams['properties'])

def main():

    print('---> Topics {0} '.format(topic))
    print('---> params {0} '.format(brokers))
    print('---> URL {0}'.format(dbparams['url']))
    print('---> Mode {0}'.format(dbparams['mode']))
    print('---> Properties {0}'.format(dbparams['properties']))

    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})

    readingsPart = kvs.map(process)

    featuresRDD = readingsPart.map(convert_to_row)

    featuresRDD.foreachRDD(streamrdd_to_df)

    ssc.start()
    ssc.awaitTermination()

if __name__ == "__main__":
    print("---> Stream reading start")
    main()