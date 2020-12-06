import sys
import re
from operator import add

from pyspark import SparkContext

def map_phase(x):
    x = re.sub('--', ' ', x)
    x = re.sub("'", '', x)
    return re.sub('[?!@#$\'",.;:()]', '', x).lower()


if __name__ == "__main__":
	if len(sys.argv) < 4:
            print >> sys.stderr, "Usage: wordcount <master> <inputfile> <outputfile>"
            exit(-1)
	sc = SparkContext(sys.argv[1], "python_wordcount_sorted in bigdataprogrammiing")
	numTokyo = sc.accumulator(0)

	def removeTokyo(x):
		global numTokyo
		if x == "Tokyo":
			numTokyo += 1
			return False
		else:
			return True

	lines = sc.textFile(sys.argv[2],2)
	print(lines.getNumPartitions()) # print the number of partitions
	filtered = lines.filter(removeTokyo)
	outRDD = filtered.map(lambda x: (map_phase(x), 1)).reduceByKey(lambda x, y: x + y)
	outRDD = outRDD.sortBy(lambda x: x[1])
	outRDD.saveAsTextFile(sys.argv[3])
	print("numbers of Tokyo: " + numTokyo) # print the number of Tokyo












