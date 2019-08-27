# Extracting information from NASA HTTP logs

from pyspark import SparkConf, SparkContext
from operator import add

conf = (SparkConf()
         .setMaster("local")
         .setAppName("NASALogs"))
sc = SparkContext(conf = conf)


# ########## Create line RDDs from log files ########################
july_log = sc.textFile('access_log_Jul95').cache()
august_log = sc.textFile('access_log_Aug95').cache()

# ########## Calculate number of host to each month #################
july_host_number = july_log.flatMap(lambda line: line.split(' ')[0]).distinct().count()
august_host_number = august_log.flatMap(lambda line: line.split(' ')[0]).distinct().count()
print('Number of hosts in July: %s' % july_host_number)
print('Number of hosts in August: %s' % august_host_number)



# ########## Calculate the number of 404 errors per month ###########
def response_code_404(line):
    try:
        code = line.split(' ')[-2]
        if code == '404':
            return True
    except:
        pass
    return False
    
july_404_errors = july_log.filter(response_code_404).cache()
august_404_errors = august_log.filter(lambda line: line.split(' ')[-2] == '404').cache()

print('Number of 404 errors in July: %s' % july_404_errors.count())
print('Number of 404 errors in August: %s' % august_404_errors.count())


# ############### N top endpoints causing 404 errors ################
def top_n_404_endpoints(RDD,n):
    endpoints = RDD.map(lambda line: line.split('"')[1].split(' ')[1])
    counts = endpoints.map(lambda endpoint: (endpoint, 1)).reduceByKey(add)
    top = counts.sortBy(lambda pair: -pair[1]).take(n)
    month = RDD.map(lambda line: line.split('[')[1].split(':')[0]).take(1)[0][3:11]
	
    print('\nTop %s endpoints causing 404 on %s:' %(n , month))
    for endpoint, count in top:
        print(endpoint, count)
        
    return top

top_n_404_endpoints(july_404_errors,5)
top_n_404_endpoints(august_404_errors,5)


# ############# Number of 404 errors by day ##########################

def num_404_by_day(RDD):
    days = RDD.map(lambda line: line.split('[')[1].split(':')[0])
    month = days.take(1)[0][3:11]
    counts = days.map(lambda day: (day, 1)).reduceByKey(add).sortBy(lambda pair: -pair[1]).collect()
    
    print('\nNumber of 404 errors by day on %s:' % month)
    for day, count in counts:
        print(day, count)
        
    return counts

num_404_by_day(july_404_errors)
num_404_by_day(august_404_errors)


# ######### Stop SparkContext ################
sc.stop()