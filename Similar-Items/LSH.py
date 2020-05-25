from pyspark import SparkContext
from pyspark import SparkConf
import json
import time
import sys
from itertools import islice
from collections import Counter, defaultdict
from itertools import combinations
import random
import numpy as np

start_time = time.time()
# conf = SparkConf().set('spark.driver.host','127.0.0.1')

sc = SparkContext(master='local[*]', appName='scObject')
sc.setLogLevel("ERROR")

# Path to the file
# Train File
input_file = "/Users/vegi/Documents/USC_Masters/INF553/Homeworks/Homework-3/data/yelp_train.csv"
# input_file = sys.argv[1]
print("This is input file:",input_file)

# Reading the raw data as a text file
raw_data = sc.textFile(input_file)

# Converting text file
rdd_data = raw_data.map(lambda x: x.split(","))
header = rdd_data.first()  # extract header

print("The header is:", header)

# Method 1 to get csv without header
rdd_data = rdd_data.mapPartitionsWithIndex(lambda idx, it: islice(it, 1, None) if idx == 0 else it).map(
    lambda x: (x[0], x[1]))

unique_users = rdd_data.map(lambda x: (x[0], 1)).keys().distinct().collect()

unique_business = rdd_data.map(lambda x: (x[1], 1)).keys().distinct().collect()

print("Number of users:", len(unique_users))
print("Number of business:", len(unique_business))

user_ids_map = {k: v for v, k in enumerate(unique_users)}
business_ids_map = {k: v for v, k in enumerate(unique_business)}

# Create the characteristic matrix!
characteristic_matrix = rdd_data.map(lambda x: (x[1], [user_ids_map[x[0]]])).reduceByKey(lambda x, y: x + y)

char_matrix = {}
for i in characteristic_matrix.collect():
    char_matrix[i[0]] = i[1]


# characteristic_matrix_reverse = rdd_data.map(lambda x: (x[0],[business_ids_map[x[1]]])).reduceByKey(lambda x,y: x+y)

def map_function(x):
    # Compute min hash values for each business
    min_hash_vals = []
    for i in range(len(all_primes)):
        min_vals = []
        for val in x[1]:
            min_vals.append(((all_primes[i] * val + 7)) % num_rows)
        min_val = min(min_vals)
        min_hash_vals.append(min_val)
    return (x[0], min_hash_vals)


######Generating Hash Functions
num_rows = len(unique_users)

# Set all values here
num_functions = 24
num_bands = 12
num_rows_per_band = int(num_functions / num_bands)
print("Number of functions in signature:", num_functions)
print("Number of bands:", num_bands)
print("Number of rows per band:", num_rows_per_band)
print("b*r value:", (num_bands * num_rows_per_band))

all_primes = [2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71, 73, 77, 79, 83]

#Create the signature matrx
signature_matrix = characteristic_matrix.map(lambda x: map_function(x)).collect()

for i in signature_matrix[0:1]:
    print(i)
    print(len(i[1]))

print(len(signature_matrix))

# Implementing LSH
num_buckets = 500
dict_buckets = {}

for bid in signature_matrix:
    # print("This is the bid:",bid[0])
    band_id = 0
    for group_keys in range(num_rows_per_band, num_functions + 1, num_rows_per_band):
        if band_id not in dict_buckets:
            dict_buckets[band_id] = defaultdict(dict)
        # Obtaining hash_bucket
        rows = ''.join([str(i) for i in bid[1][group_keys - num_rows_per_band:group_keys]])

        if rows in dict_buckets[band_id]:
            dict_buckets[band_id][rows].append(bid[0])
        else:
            dict_buckets[band_id][rows] = [bid[0]]
        band_id += 1

for k in dict_buckets.keys():
    for key, val in dict_buckets[k].items():
        dict_buckets[k][key] = sorted(val)

# Pure Jaccard Similarity
def pure_calc_jac(cand):
    a = set(char_matrix[cand[0]])
    b = set(char_matrix[cand[1]])

    #Obtain Intersections and Union
    numIntersections = len(a.intersection(b))
    numUnions = len(set(a.union(b)))

    #Jaccard Similarity
    jaccSim = float(numIntersections / numUnions)

    return jaccSim


pure_jacc = []

for band in dict_buckets.keys():
    # print("This is band:",band)
    for bucket in dict_buckets[band].keys():
        if len(dict_buckets[band][bucket]) >= 2:
            #Generate candidate pairs for items in each bucket
            for cand in sorted(list(combinations(sorted(dict_buckets[band][bucket]), 2))):
                #Skip if we already stored candidate
                if cand in pure_jacc:
                    continue
                #Calculate Jaccard Similarity and store only if >=0.5
                jac_sim = pure_calc_jac(cand)
                if jac_sim >= 0.5:
                    pure_jacc.append((cand, jac_sim))

print("This is the output:", len(set(pure_jacc)))
print("This is the output:", len(pure_jacc))

for i in list(set(pure_jacc))[0:5]:
    print(i)

final_rdd = sc.parallelize(set(pure_jacc)).map(lambda x: (x[0][0], x[0][1]))
# print(final_rdd.take(5))

groud_truth_file = "/Users/vegi/Documents/USC_Masters/INF553/Homeworks/Homework-3/data/pure_jaccard_similarity.csv"
ground = sc.textFile(groud_truth_file).map(lambda x: x.split(",")).map(lambda x: (x[0], x[1]))

tp = final_rdd.intersection(ground)
# print("This is tp:",tp.collect())

precision = len(tp.collect()) / len(final_rdd.collect())
print("This is precision:", precision)

recall = len(tp.collect()) / len(ground.collect())
print("This is recall:", recall)

print("Total time:", time.time() - start_time)

outFilePath = '/Users/vegi/Documents/USC_Masters/INF553/Homeworks/Homework-3/OP/HW3_1.txt'
# outFilePath = sys.argv[2]
print("This is output file:",outFilePath)

outFile = open(outFilePath, "w")

outFile.write('business_id_1,business_id_2, similarity\n')
op = sorted(set(pure_jacc))
for line in op:
  # write line to output file
  outFile.write(str(line[0][0])+','+str(line[0][1])+','+str(line[1]))
  outFile.write("\n")
outFile.close()