from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext
from graphframes import *
import json
import time
import sys
import os

# # Only needed for spark-submit
os.environ["PYSPARK_SUBMIT_ARGS"] = ( "--packages graphframes:graphframes:0.6.0-spark2.3-s_2.11")
start_time = time.time()

sc = SparkContext(master='local[*]', appName='scObject')
sc.setLogLevel("ERROR")

# Path to the file
input_path = sys.argv[1]
input_file = sc.textFile(input_path)
edges = input_file.map(lambda line: line.split(" ")).map(lambda x:(x[0],x[1]))

#Reading i vertices
nodes_i = edges.keys().collect()
#Reading j vertices
nodes_j = edges.values().collect()

#All of the vertices/nodes
nodes_i.extend(nodes_j)
print("Total number of unique vertices:",len(set(nodes_i)))

#All of the edges =
edges_distinct =  edges.distinct().collect()

print("Number of distinct edges:",len(edges_distinct))
edges_bidirectional = edges_distinct.copy()
for i in edges_distinct:
  if (i[1],i[0]) in edges_distinct:
    continue
  else:
    edges_bidirectional.append((i[1], i[0]))

print("After adding the opposite edges:",len(edges_bidirectional))

# Making it a RDD
edges_bidirectional = sc.parallelize(edges_bidirectional)

# Graph Frames code -
# Need this for GraphFrames
spark = SQLContext(sc)

# Need to create edges and vertices dataframe
edges_DF = spark.createDataFrame(edges_bidirectional, ["src", "dst"])
vertices_DF = edges_DF.select("src").union(edges_DF.select("dst")).distinct().withColumnRenamed('src', 'id')


# Let's create a graph from the edges and vertices we have above
g = GraphFrame(vertices_DF,edges_DF)

# Just a test
# This is the degree of each node
vertexDegree = g.degrees

# Running the label propagation algorithm
LPA_result = g.labelPropagation(maxIter=5)
print(LPA_result)
# result.select("id", "label").show(5)

# print("This is groupBy:")
result_groupBy = LPA_result.groupBy('label').count()
result_groupBy_order = result_groupBy.orderBy('count', ascending=False)

print("Number of labels:",result_groupBy.count())
all_labels = result_groupBy_order.select('label').collect()

# Converting to a RDD
label_node_rdd = LPA_result.select(['label', 'id']).rdd
label_node_rdd = label_node_rdd.map(lambda x:(x[0],x[1]))

# Grouping on labels (Communities)
label_groupBy = label_node_rdd.groupByKey().collect()
label_groupBy_dict = {}

for pair in label_groupBy:
    if pair[0] in label_groupBy_dict:
        label_groupBy_dict[pair[0]].append(list(pair[1]))
    else:
        label_groupBy_dict[pair[0]] = list(pair[1])
    # .mapValues(dict).collectAsMap()
print("****Label****|*****Nodes**** ")
print("Number of keys:",len(label_groupBy_dict.keys()))

# Storing in a dictionary with key as length of community
len_dict = {}
for label in label_groupBy_dict.keys():
    new_key = len(label_groupBy_dict[label])
    if new_key in len_dict:
        len_dict[new_key].append(sorted(label_groupBy_dict[label]))
    else:
        len_dict[new_key] = [sorted(label_groupBy_dict[label])]

# Writing to disk
print("The largest community is:",max(len_dict.keys()))
outfile = sys.argv[2]
print("Writing to file:")
with open(outfile, 'w') as writeFile:
     for key in sorted(len_dict.keys()):
        sorted_op = sorted(len_dict[key])
        for item in sorted_op:
            # test = ', '.join(sorted(item))
            test = ', '.join(["'" + i + "'" for i in item])
            writeFile.write(test + "\n")

print("Total time:",time.time()-start_time)