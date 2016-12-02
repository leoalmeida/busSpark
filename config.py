from __future__ import print_function

import sys
import findspark
import itertools
import numpy as np
findspark.init()

try:
    from pyspark import SparkConf, SparkContext
    from pyspark.sql import SQLContext
    from pyspark.streaming import StreamingContext
    from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition

    from pyspark.sql import SparkSession
    from pyspark.sql.functions import *
    from pyspark.sql.types import *
    from pyspark.sql import Row
    from collections import OrderedDict
    from math import sqrt,pow

except ImportError as e:
    print("Can not import Spark Modules", e)
    sys.exit(1)

config = {
    "spark.app.name": "HDFTestes",
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
    "spark.jars": "lib/sqlite-jdbc-3.15.1.jar,lib/spark-streaming-kafka-assembly_2.10-1.6.3.jar"
}

dbparams = {'url': "jdbc:sqlite:data/bus.db", 'properties': {"driver": "org.sqlite.JDBC"}, 'mode': "append"}

conf = SparkConf()
for key, value in config.iteritems():
    conf = conf.set(key, value)

spark = SparkSession.builder.config(conf=conf).getOrCreate()
ssc = StreamingContext(spark.sparkContext, 1)
sqlContext = SQLContext(spark.sparkContext)

df = sqlContext.read.format("jdbc").option("url", dbparams['url']).option("dbtable", "rotas").load()

# The schema is encoded in a JSON string.
schemaString = "dtservidor,dtavl,idlinha,latitude,longitude,idavl,evento,idponto,sentido,shape_idx,shape_lat,shape_lon,shape_distance,linha"
LineSchema = Row(i for (i) in schemaString.split(","))

if len(sys.argv) != 5:
    print("Usage: spark_filesplit.py <zk> <topic>", file=sys.stderr)
    exit(-1)

kafkaParams = {"metadata.broker.list": sys.argv[1]}
topic = sys.argv[2]
tolerancia = sys.argv[3]
output = sys.argv[4] + '/' + sys.argv[2]

def process((k, v)):
    value = {i: j.strip() for (i, j) in itertools.izip_longest(schemaString.split(","), v.split(","), fillvalue='0')}
    return value

def convert_to_row(d):
    return Row(**OrderedDict(d.items()))

def streamrdd_to_df(srdd):
    if not srdd.isEmpty():
        points = srdd.collect()

        for point in points:
            min = 360
            indice = 0

            filteredGroup = df.filter(df.line_id == point.idlinha).rdd.collect()

            for rdd in filteredGroup:
                indice += 1

                distance = sqrt(pow(float(rdd.shape_pt_lat) - float(point.latitude), 2) + pow(float(rdd.shape_pt_lon) - float(point.longitude), 2))
                if distance < min:
                    shape_distance = distance
                    shape_idx = indice
                    sentido = rdd.direction_id
                    shape_lat = rdd.shape_pt_lat
                    shape_lon = rdd.shape_pt_lon
                    route_id = rdd.route_id

            newpoint = [{i: j for (i, j) in itertools.izip_longest(schemaString.split(","), [point.dtservidor, point.dtavl, point.idlinha, point.latitude, point.longitude, point.idavl, point.evento, point.idponto ,sentido, shape_idx,shape_lat,shape_lon,shape_distance, route_id], fillvalue='0')}]

            newdf = spark.createDataFrame(newpoint)

            newdf.write.json(output,mode='append')

            #newdf.write.jdbc(url=dbparams['url'], table=topic, mode=dbparams['mode'], properties=dbparams['properties'])

            if ((shape_distance * 10000) < tolerancia):
                newdf.write.json(output+"_clean", mode='append')
                #newdf.write.jdbc(url=dbparams['url'], table=topic+"_clean", mode=dbparams['mode'], properties=dbparams['properties'])

def main():

    print('---> Topics {0} '.format(topic))
    print('---> params {0} '.format(kafkaParams))
    print('---> URL {0}'.format(dbparams['url']))
    print('---> Mode {0}'.format(dbparams['mode']))
    print('---> Properties {0}'.format(dbparams['properties']))

    kvs = KafkaUtils.createDirectStream(ssc, [topic], kafkaParams)

    readingsPart = kvs.map(process)

    featuresRDD = readingsPart.map(convert_to_row)

    featuresRDD.foreachRDD(streamrdd_to_df)

    ssc.start()
    ssc.awaitTermination()

if __name__ == "__main__":
    print("---> Stream reading start")
    main()