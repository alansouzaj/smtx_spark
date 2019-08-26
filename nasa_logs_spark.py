# Extracting information from NASA HTTP logs

from pyspark import SparkConf, SparkContext


conf = (SparkConf()
         .setMaster("local")
         .setAppName("NASALogs")
         .set("spark.executor.memory", "2g"))
sc = SparkContext(conf = conf)

