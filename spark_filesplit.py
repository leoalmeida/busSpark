from __future__ import print_function

import sys
import findspark
findspark.init()

try:
    from pyspark import SparkConf, SparkContext
    from pyspark.sql import SQLContext, SparkSession
    from pyspark.sql.streaming import DataStreamReader, DataStreamWriter, StreamingQuery
    from pyspark.sql.types import *

except ImportError as e:
    print("Can not import Spark Modules", e)
    sys.exit(1)

config = {
    "spark.app.name": "HDFTestes",
    "spark.driver.cores": "3",
    "spark.master": "local",
    "spark.executor.memory": "10g",
    "spark.driver.memory": "10g",
    "spark.driver.maxResultSize": "8g",
    "spark.python.profile": "true",
    "spark.ui.enabled": "true",
    "spark.executor.extraClassPath": "lib/sqlite-jdbc-3.15.1.jar",
    "spark.driver.extraClassPath": "lib/sqlite-jdbc-3.15.1.jar",
    "spark.jars": "lib/sqlite-jdbc-3.15.1.jar"
}
conf = SparkConf()
for key, value in config.iteritems():
    conf = conf.set(key, value)

if __name__ == "__main__":

    if len(sys.argv) != 3:
        print("Usage: spart_filesplit.py <directory>", file=sys.stderr)
        exit(-1)

    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()

    textStream = spark.read.format("text").load(sys.argv[1])
    readings = textStream.rdd.map(lambda line: (line[0].split(",")[2], line[0].split(","))).groupByKey().map(lambda value: (value[0], list(value[1])))

    # print(readings.collect())

    # The schema is encoded in a string.
    schemaString = "line readings"
    #schemaString = "dtservidor dtavl idlinha longitude latitude idavl evento idponto"

    fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
    schema = StructType(fields)

    # Apply the schema to the RDD.
    schemaReadings = spark.createDataFrame(readings, schema)

    #schemaReadings.write.format("json").save(sys.argv[2])

    writer = schemaReadings.write.jdbc(url="jdbc:sqlite:data/bus.db", table="bus2", mode="overwrite", properties={"driver":"org.sqlite.JDBC"})

