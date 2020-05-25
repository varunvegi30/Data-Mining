from pyspark import SparkContext
from pyspark import SparkConf
import json
import time
import sys
from math import sqrt
from itertools import islice
import xgboost as xgb
import numpy as np
from collections import defaultdict

start_time = time.time()

sc = SparkContext(master='local[*]', appName='scObject')
sc.setLogLevel("ERROR")

############################################# READING THE TRAINING DATA #############################################
# Path to the file
# Train Dataset
folder_path = sys.argv[1]
print("This is folder path:", folder_path)
input_file = folder_path + "yelp_train.csv"

# Reading the raw data as a text file
raw_data = sc.textFile(input_file)

# Converting text file
rdd_data = raw_data.map(lambda x: x.split(","))

header = rdd_data.first()  # extract header

# To get csv without header
rdd_data = rdd_data.mapPartitionsWithIndex(lambda idx, it: islice(it, 1, None) if idx == 0 else it)
val_file = sys.argv[2]
print("This is val path:", val_file)

# Reading the validation data:
raw_data_val = sc.textFile(val_file)

# Converting text file
rdd_data_val = raw_data_val.map(lambda x: x.split(","))

header_val = raw_data_val.first()  # extract header

# To get csv without header
rdd_data_val = rdd_data_val.mapPartitionsWithIndex(lambda idx, it: islice(it, 1, None) if idx == 0 else it)
############################################# END OF READING THE VALIDATION DATA #############################################
############################################# DICTIONARIES WE NEED #############################################
uid_groupby = rdd_data.map(lambda x: (x[0], 1)).groupByKey()  # (uid, 1)
bid_groupby = rdd_data.map(lambda x: (x[1], 1)).groupByKey()  # (bid, 1)

unique_bids = bid_groupby.keys().collect()
unique_uids = uid_groupby.keys().collect()

unique_bids_map = {k: i for i, k in enumerate(unique_bids)}
unique_uids_map = {k: i for i, k in enumerate(unique_uids)}

max_bid = max(unique_bids_map.values())
max_uid = max(unique_uids_map.values())


# Function for map partition
def map_partition(idx, itr):
    part_op = {}
    for it in itr:
        part_op[it[0]] = {}
        part_op[it[0]]['avg_rating'] = it[1]
        part_op[it[0]]['n_review'] = it[2]

    return idx, part_op


############################################# READING THE USER DATA #############################################
user_json_path = folder_path + "user.json"

# Reading the raw data as a text file
raw_data = sc.textFile(user_json_path)

# Converting to a RDD of dictionaries
user_dataset = raw_data.map(json.loads).map(lambda x: (x['user_id'], float(x['average_stars']), int(x['review_count'])))

map_part_op = user_dataset.mapPartitionsWithIndex(map_partition).collect()

user_data_dict = {}
for i in range(1, len(map_part_op), 2):
    user_data_dict.update(map_part_op[i])
############################################# END OF READING THE USER DATA #############################################
############################################# READING THE BUSINESS DATA #############################################
business_json_path = folder_path + "business.json"

# Reading the raw data as a text file
raw_data = sc.textFile(business_json_path)

# Created a dictionary ---> {uid:{avg_rating:rating, n_review:n_reviews.....}}

business_dataset = raw_data.map(json.loads).map(lambda x: (x['business_id'], x['stars'], x['review_count']))

map_part_op = business_dataset.mapPartitionsWithIndex(map_partition).collect()

business_data_dict = {}
for i in range(1, len(map_part_op), 2):
    business_data_dict.update(map_part_op[i])


############################################# END OF READING THE BUSINESS DATA #############################################
############################################# FORMING THE TRAIN AND TEST DATA #############################################
def map_func_train_part(idx, itr):
    part_op = []
    global unique_bids_map, unique_uids_map
    for x in itr:
        if x[1] in unique_bids_map:
            bid_map = unique_bids_map[x[1]]
        else:
            global max_bid
            max_bid += 1
            unique_bids_map[x[1]] = max_bid
            bid_map = unique_bids_map[x[1]]

        if x[1] in business_data_dict:
            avg_rating_business = business_data_dict[x[1]]['avg_rating']
            n_review_business = business_data_dict[x[1]]['n_review']
        else:
            avg_rating_business = 0
            n_review_business = 0

        if x[0] in unique_uids_map:
            uid_map = unique_uids_map[x[0]]
        else:
            global max_uid
            max_uid += 1
            unique_uids_map[x[0]] = max_uid
            uid_map = unique_uids_map[x[0]]

        if x[0] in user_data_dict:
            avg_rating = user_data_dict[x[0]]['avg_rating']
            n_reviews = user_data_dict[x[0]]['n_review']
        else:
            avg_rating = 0.0
            n_reviews = 0
        # part_op.append(([uid_map, bid_map, avg_rating, n_reviews, avg_rating_business, n_review_business], float(x[2])))
        part_op.append(([uid_map, bid_map, avg_rating, n_reviews, avg_rating_business, n_review_business], float(x[2])))
        # part_op.append(([uid_map, bid_map, avg_rating, n_reviews, avg_rating_business, n_review_business]))
    return idx, part_op


def map_func_test_part(idx, itr, name='Train'):
    part_op = []
    global unique_bids_map, unique_uids_map
    for x in itr:
        if x[1] in unique_bids_map:
            bid_map = unique_bids_map[x[1]]
        else:
            global max_bid
            max_bid += 1
            unique_bids_map[x[1]] = max_bid
            bid_map = unique_bids_map[x[1]]

        if x[1] in business_data_dict:
            avg_rating_business = business_data_dict[x[1]]['avg_rating']
            n_review_business = business_data_dict[x[1]]['n_review']
        else:
            avg_rating_business = 0
            n_review_business = 0

        if x[0] in unique_uids_map:
            uid_map = unique_uids_map[x[0]]
        else:
            global max_uid
            max_uid += 1
            unique_uids_map[x[0]] = max_uid
            uid_map = unique_uids_map[x[0]]

        if x[0] in user_data_dict:
            avg_rating = user_data_dict[x[0]]['avg_rating']
            n_reviews = user_data_dict[x[0]]['n_review']
        else:
            avg_rating = 0.0
            n_reviews = 0

        part_op.append(([uid_map, bid_map, avg_rating, n_reviews, avg_rating_business, n_review_business], 0))

    return idx, part_op


train_data_collect = rdd_data.mapPartitionsWithIndex(map_func_train_part).collect()
test_data_collect = rdd_data_val.mapPartitionsWithIndex(map_func_test_part).collect()

x_train = []
y_train = []
for i in range(1, len(train_data_collect), 2):
    for pair in train_data_collect[i]:
        x_train.append(pair[0])
        y_train.append(pair[1])

x_test = []
y_test = []
for i in range(1, len(test_data_collect), 2):
    for pair in test_data_collect[i]:
        x_test.append(pair[0])
#        y_test.append(pair[1])

# X_train and Y_Train and numpy array
x_train_np = np.array(x_train)
y_train_np = np.array(y_train)
print(x_train_np.shape)
print(y_train_np.shape)

# X_test and Y_test and numpy array
x_test_np = np.array(x_test)
y_test_np = np.array(y_test)
print(x_test_np.shape)
print(y_test_np.shape)

# xg_reg = xgb.XGBRegressor(objective ='reg:linear', colsample_bytree = 0.3, learning_rate = 0.1,
#                max_depth = 5, n_estimators = 100)

xg_reg = xgb.XGBRegressor(objective='reg:linear', colsample_bytree=0.1, learning_rate=0.4,
                          max_depth=5, n_estimators=100)

xg_reg.fit(x_train_np, y_train_np)

preds = xg_reg.predict(x_test_np)
# print(preds)

op_file = sys.argv[3]
print("This is output file:", op_file)

outFile = open(op_file, "w")

outFile.write('user_id, business_id, prediction\n')

count = 0
for i, pair in enumerate(rdd_data_val.collect()):
    outFile.write(str(pair[0]) + ',' + str(pair[1]) + ',' + str(preds[i]))
    outFile.write("\n")
    count += 1

print("Wrote:", count)

# sums = 0
# count = 0
# for i in range(len(preds)):
#    sums = sums+((preds[i]-y_test[i])**2)
#    count+=1

# Mean square error and root mean square error
# print("Root Mean Squared Error:", sqrt(sums/count))
# print("Count is:{0}".format(count))
print("Total time:", time.time() - start_time)