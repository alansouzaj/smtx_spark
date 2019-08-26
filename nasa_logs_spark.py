# Extracting information from NASA HTTP logs

from pyspark import SparkConf, SparkContext


conf = (SparkConf()
         .setMaster("local")
         .setAppName("NASALogs"))
sc = SparkContext(conf = conf)


#Create line RDDs from log files
july_log = sc.textFile('access_log_Jul95').cache()
august_log = sc.textFile('access_log_Aug95').cache()

#Calculate number of host to each month
july_host_number = july.flatMap(lambda line: line.split(' ')[0]).distinct().count()
august_host_number = august.flatMap(lambda line: line.split(' ')[0]).distinct().count()
print('Number of hosts on July: %s' % july_host_number)
print('Number of hosts on August %s' % august_host_number)
