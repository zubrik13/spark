from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TotalCustomerSpendings")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    customer = int(fields[0])
    spendings = float(fields[2])
    return (customer, spendings)

lines = sc.textFile("file:///SparkCourse/customer-orders.csv")
rdd = lines.map(parseLine)
totalsByCustomer = rdd.reduceByKey(lambda x,y: x + y)

flipped = totalsByCustomer.map(lambda x: (x[1], x[0]))
totalsByCustomerSorted = flipped.sortByKey()

results = totalsByCustomerSorted.collect()
for result in results:
    print("Customer No {}".format(result[1]) + " " +"spent {:.2f}$ overall".format(result[0]))

