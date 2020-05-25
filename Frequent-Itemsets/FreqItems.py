from pyspark import SparkContext
from pyspark import SparkConf
import json
import time
import sys
from itertools import islice
from collections import Counter,defaultdict
from itertools import combinations
import random

start_time = time.time()

sc = SparkContext(master='local[*]', appName='scObject')
sc.setLogLevel("ERROR")

# Problem 2
#Command line parameters
filter_threshold = int(sys.argv[1])
inp_support = int(sys.argv[2])
input_file = sys.argv[3]
outfile = sys.argv[4]

print("Filter threshold is:",filter_threshold)
print("Support is:",inp_support)
print("Input file is:",input_file)
print("Output file is:",outfile)

#Number of buckets to use in phase1
num_buckets = 10000
#Input variables
# Path to the file
#Input File
input_file = "/Users/vegi/Documents/USC_Masters/INF553/Homeworks/Homework-2/ta_feng_all_months_merged.csv"
#
#
inp_support = 50
filter_threshold = 20
#
# outfile = "/Users/vegi/Documents/USC_Masters/INF553/Homeworks/Homework-2/submit_final/result2.txt"
# Reading the raw data as a text file
raw_data = sc.textFile(input_file)

#Converting text file
rdd_data = raw_data.map(lambda x: x.split(","))
header = rdd_data.first() #extract header

print("The header is:",header)

#data = rdd_file.filter(lambda row: row != header)
rdd_data = rdd_data.mapPartitionsWithIndex(lambda idx, it: islice(it, 1, None) if idx == 0 else it)

final_rdd = rdd_data.map(lambda x:(x[0]+'-'+x[1],x[5]))
#final_rdd = final_rdd.partitionBy(5)
num_partitions = final_rdd.getNumPartitions()

#Do we really need to group by? look at alternatives.
data_groupby = final_rdd.groupByKey().filter(lambda x: len(x[1])>filter_threshold)
#Creating a sorted set of the items (Example: if a user reviewed the same business multiple times)

data_groupby_unique = data_groupby.map(lambda x: (x[0],sorted(set(x[1]))))

#Custom function to count number of elements per partition
def count_items(iterator):
    yield sum(1 for _ in iterator)

#Number of items/partition
num_buckets_partition = data_groupby_unique.mapPartitions(count_items).collect()
print("Number of items in each paritition:",num_buckets_partition)
total_num_buckets = sum(num_buckets_partition)

################################### PLAYING AROUND WITH MAPPARTITIONS ###################

def count_in_a_partition(idx, iterator):
#def count_in_a_partition(iterator):
    all_baskets = {}
    for i in iterator:
        #Only using baskets with number of items>filter
        all_baskets[i[0]] = i[1]

    # Dictionary to stores buckets and count
    items_buckets_count = {}
    # Look for a better alternate with range!
    items_buckets_count = {k: {} for k in range(2, 25)}

    def hash_pair(items_in_basket, i):
        for item in items_in_basket.keys():
            # Hashing to a bucket
            hash_key = int(sum([int(it) for it in item]))
            hash_val = hash(hash_key)
            bucket = hash_val % num_buckets
            if bucket in items_buckets_count[i]:
                items_buckets_count[i][bucket] += 1
            else:
                items_buckets_count[i].update({bucket: 1})

    singleton_count = {}
    # This stores the final output!
    freq_items = {}
    # Iterating over each of the baskets
    for k, val in all_baskets.items():
        # Get the count of every singleton
        for single in val:
            if single in singleton_count:
                singleton_count[single] += 1
            else:
                singleton_count[single] = 1
        # Start with pairs and then extend to maximum i
        i = 2
        items_basket = {k: 1 for k in combinations(val, i)}

        # Calling the hash function
        hash_pair(items_basket, i)

    freq_items = {}
    freq_items["1"] = {k: 1 for k, v in singleton_count.items() if
                           v >= (num_buckets_partition[idx] * inp_support) / total_num_buckets}

    #Obtaining frequent buckets
    for k in sorted(items_buckets_count.keys()):
        items_buckets_count[k] = {k: 1 for k, v in items_buckets_count[k].items() if
                   v >= (num_buckets_partition[idx] * inp_support) / total_num_buckets}

    #Count all possible candidates
    count_all_candidates = {k:{} for k in range(2,len(freq_items["1"].keys()))}

    for i in range(2,len(freq_items["1"].keys())):

        #Exit the loop if itemsets of size i-1 is empty
        if freq_items[str(i-1)] == {}:
            break

        if i==2:
            # All possible items of size i from the singletons
            all_items_i_from_singletons = {k: 1 for k in combinations(sorted(freq_items["1"].keys()), i)}
            for k, val in all_baskets.items():
            #Obtain all possible itemsets of size i in the basket
                items_basket = {k:1 for k in combinations(val, i)}
                #for pair in items_basket:
                for pair in items_basket.keys():
                #Check if all items in the pair are there in the singletons
                #if pair in all_items_i_from_singletons:
                    if pair in all_items_i_from_singletons.keys():
                        hash_key = int(sum([int(i) for i in pair]))
                        hash_val = hash(hash_key)
                        bucket = hash_val % num_buckets
                        #If bucket is frequent
                        if bucket in items_buckets_count[i]:
                            #Update count
                            if pair in count_all_candidates[i]:
                                count_all_candidates[i][pair] += 1
                            else:
                                count_all_candidates[i].update({pair:1})

            freq_items[str(i)] = {k: 1 for k in sorted(count_all_candidates[i].keys()) if
                                   count_all_candidates[i][k] >= (num_buckets_partition[idx] * inp_support)/ total_num_buckets}
            #Exit if no frequent items
            if freq_items[str(i)] == {}:
                del freq_items[str(i)]
                break
            #Creating singletons from frequent itemsets
            items_use_next_iter = {j: {} for i in freq_items[str(i)].keys() for j in i}

        elif i>2:
            print("I'm at i:",i)
            # All possible items of size i
            all_items_i_from_singletons = {k: 1 for k in combinations(sorted(items_use_next_iter.keys()), i)}
            use_all_items_i_from_singletons = {}
            for cand in all_items_i_from_singletons.keys():
                cand_subsets = list(combinations(sorted(cand), i - 1))
                if set(cand_subsets).issubset(freq_items[str(i - 1)].keys()):
                    # use_all_items_i_from_singletons.append(cand)
                    use_all_items_i_from_singletons[cand] = 1

            all_items_i_from_singletons = use_all_items_i_from_singletons.copy()
            #print("Updated number of itemsets to look at are:", len(all_items_i_from_singletons))
            if len(all_items_i_from_singletons) == 0:
                break
            for k, val in all_baskets.items():
                #Skip basket if none of the candidates are present in it!
                flag_basket = 0
                for cand_item in all_items_i_from_singletons:
                    if  set(cand_item).issubset(val):
                        flag_basket = 1
                if flag_basket == 0:
                    continue
                items_basket = {k:1 for k in combinations(val, i)}
                #for pair in items_basket:
                for pair in items_basket.keys():
                #Check if all items in the pair are there in the singletons
                #if pair in all_items_i_from_singletons:
                    if pair in all_items_i_from_singletons.keys():
                        if pair in count_all_candidates[i]:
                            count_all_candidates[i][pair] += 1
                        else:
                            count_all_candidates[i].update({pair:1})

            freq_items[str(i)] = {k: 1 for k in sorted(count_all_candidates[i].keys()) if
                                   count_all_candidates[i][k] >= (num_buckets_partition[idx] * inp_support)/ total_num_buckets}
            # Exit if no frequent items
            if freq_items[str(i)] == {}:
                del freq_items[str(i)]
                break
            # Creating singletons from frequent itemsets
            items_use_next_iter = {j: {} for i in freq_items[str(i)].keys() for j in i}

    print("This is frequent items:", freq_items)
    print("Node {0} is done!".format(idx))
    return idx, freq_items

op_phase1 = data_groupby_unique.mapPartitionsWithIndex(count_in_a_partition).collect()
#op_phase1_dict = {}
print(op_phase1)

print("Fininshed executing MAP")
cdict = {}
total_elem = len(op_phase1)

for i in range(1,total_elem,2):
    for key,val in op_phase1[i].items():
        if key in cdict:
            cdict[key].update(val)
        else:
            cdict[key] = val
print(cdict)

#Moving on to phase 2

def final_count_in_a_partition(idx, iterator):
    all_baskets = {}
    for i in iterator:
        # Only using baskets with number of items>filter
        all_baskets[i[0]] = i[1]

    #Counting singleton and pairs in each basket
    singleton_count = {}
    candidate_items_count = {str(k):{} for k in range(2,len(cdict.keys())+1)}
    for key,vals in all_baskets.items():
        for val in vals:
            if val in cdict['1']:
                if val in singleton_count:
                    singleton_count[val] += 1
                else:
                    singleton_count[val] = 1
        for itemset_size in range(2,len(cdict.keys())+1):
            pair_basket = list(combinations(vals, itemset_size))
            for pair in cdict[str(itemset_size)]:
                if pair in pair_basket:
                    if pair in candidate_items_count[str(itemset_size)]:
                        candidate_items_count[str(itemset_size)][pair] += 1
                    else:
                        candidate_items_count[str(itemset_size)].update({pair:1})
    candidate_items_count['1'] = singleton_count
    return idx,candidate_items_count

op_phase2 = data_groupby_unique.mapPartitionsWithIndex(final_count_in_a_partition).collect()
print(op_phase2)
print("End of Map 2")

cdict_2 = {}
total_elem = len(op_phase2)
for i in range(1,total_elem,2):
    for key,val in op_phase2[i].items():
        if key in cdict_2:
            cdict_2[key] = dict(Counter(cdict_2[key]) + Counter(val))
        else:
            cdict_2[key] = val

for key in cdict_2.keys():
   cdict_2[key] = {k: cdict_2[key][k] for k in sorted(cdict_2[key].keys()) if cdict_2[key][k] >= inp_support}
print(cdict_2)

for key in sorted(cdict_2.keys()):
   if cdict_2[key] == {}:
       continue
   print("Frequent items of size:",key)
   print("Number of items are:",len(cdict_2[key].keys()))
   for k,v in cdict_2[key].items():
       print("{0} : Frequency {1}".format(k,v))

#Writing ouptut to disk
outFile = open(outfile, "w")
outFile.write('Candidates:\n')

print("Candidates:")

for k1 in cdict.keys():
    str_write = ''
    for item in sorted(cdict[k1].keys()):
        if k1 == '1':
            str_write += "('" + item + "'),"
        else:
            str_write += str(item) + ','
    print(str_write[:-1])
    outFile.write(str_write[:-1]+'\n\n')

outFile.write('Frequent Itemsets:\n')

print("Frequent Itemsets:")

for k1 in sorted(cdict_2.keys()):
    str_write = ''
    for item in sorted(cdict_2[k1].keys()):
        if k1 == '1':
            str_write += "('"+item+"'),"
        else:
            str_write+= str(item)+','
    print(str_write[:-1])
    outFile.write(str_write[:-1]+'\n\n')
outFile.close()

print("Total Execution Time:",time.time()-start_time)