import sys

try:
    from pyspark import SparkContext
    from pyspark import SparkConf
    import pymongo_spark
    pymongo_spark.activate()
    print ("Successfully imported Spark Modules")
except ImportError as e:
    print ("Can not import Spark Modules", e)
sys.exit(1)