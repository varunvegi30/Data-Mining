from pyspark import SparkContext
from pyspark import SparkConf
import time
from itertools import islice
# from statistics import mean
from math import sqrt
import math
import json
import xgboost as xgb
import numpy as np
import sys

start_time = time.time()

sc = SparkContext(master='local[*]', appName='scObject')
sc.setLogLevel("ERROR")

############################################# READING THE TRAINING DATA #############################################
# Path to the file
# Train Dataset
folder_path = sys.argv[1]
input_file = folder_path + "yelp_train.csv"
# input_file = "/Users/vegi/Documents/USC_Masters/INF553/Homeworks/Homework-3/data_2/yelp_train.csv"

# Reading the raw data as a text file
raw_data = sc.textFile(input_file)

# Converting text file
rdd_data = raw_data.map(lambda x: x.split(","))

header = rdd_data.first()  # extract header

# To get csv without header
rdd_data = rdd_data.mapPartitionsWithIndex(lambda idx, it: islice(it, 1, None) if idx == 0 else it)
############################################# END OF READING THE TRAINING DATA #############################################
############################################# READING THE VALIDATION DATA #############################################
# Validation Dataset
val_file = "/Homework-3/data_2/yelp_val.csv"
val_file = sys.argv[2]

# Reading the validation data:
raw_data_val = sc.textFile(val_file)

# Converting text file
rdd_data_val = raw_data_val.map(lambda x: x.split(","))

header_val = raw_data_val.first()  # extract header

# To get csv without header
rdd_data_val = rdd_data_val.mapPartitionsWithIndex(lambda idx, it: islice(it, 1, None) if idx == 0 else it)
############################################# END OF READING THE VALIDATION DATA #############################################
############################################# DICTIONARIES WE NEED #############################################
# This is the format we used - (bid , (uid, rating))
# Creating a dictionary ---> {bid:{uid:rating, uid:rating.....}}
bid_uids_rating = {}
bid_uids_rating = rdd_data.map(lambda x: (x[1], (x[0], float(x[2])))).groupByKey().mapValues(dict).collectAsMap()

# Getting the average rating for each business!
bid_uids_rating_mean = {}
for key in bid_uids_rating.keys():
    bid_uids_rating_mean[key] = sum(bid_uids_rating[key].values()) / len(bid_uids_rating[key].values())

# This is the format we used - (uid , (bid, rating))
# Creating a dictionary ---> {uid:{bid:rating, bid:rating.....}}
uid_bid_rating = {}
uid_bid_rating = rdd_data.map(lambda x: (x[0], (x[1], float(x[2])))).groupByKey().mapValues(dict).collectAsMap()

############################################# THIS IS WHERE THE CODE STARTS #############################################
correlation_dict = {}
intersection_size = {}


# Function calculate correlation between businesses
def calc_corr(user_intesection, bid_main, id, users_bid_main_dict, users_id_dict):
    u_ratings = []
    v_ratings = []
    # Rating made by common users
    for user in user_intesection:
        u_ratings.append(users_bid_main_dict[user])
        v_ratings.append(users_id_dict[user])

    # Average rating of the business (Using all user ratings)
    mean_u = bid_uids_rating_mean[bid_main]
    mean_v = bid_uids_rating_mean[id]

    # Correlation Calculation
    numerator = sum([((users_bid_main_dict[x] - mean_u) * (users_id_dict[x] - mean_v)) for x in user_intesection])
    denominator_a = sqrt(sum([((users_bid_main_dict[x] - mean_u) ** 2) for x in user_intesection]))
    denominator_b = sqrt(sum([((users_id_dict[x] - mean_v) ** 2) for x in user_intesection]))

    # Set correlation to zero if denominator is zero
    if denominator_a == 0.0 or denominator_b == 0.0:
        corr = 0
        correlation_dict[(bid_main, id)] = corr
        return corr

    else:
        corr = numerator / (denominator_a * denominator_b)
        if corr >= 0:
            correlation_dict[(bid_main, id)] = corr
        return corr


# Calcuting new rating
def calc_new_rating(correlation_items, business_check):
    businesses_use = correlation_items.keys()

    # Values being used
    # correlation_items -> Get the correlation between the two items
    # business_check[bu] -> Check the rating given by the pertinent user

    num = sum([correlation_items[bu] * business_check[bu] for bu in businesses_use])
    denom = sum([abs(correlation_items[bu]) for bu in businesses_use])

    if denom == 0:
        new_rating = 0.0

    else:
        new_rating = float(num / denom)
        # Negative and greater than 5 case
        if new_rating < 0.0: new_rating = 0
        if new_rating > 5.0: new_rating = 5.0

    return new_rating


def get_new_rating(x):
    bid_main = x[1]
    uid = x[0]

    # Cold start business
    if bid_main not in bid_uids_rating:
        return ((x[0], x[1]), 4.0)

    # Cold start user
    if uid not in uid_bid_rating:
        # Business average if business exists!
        bus_avg = bid_uids_rating_mean.get(bid_main, 4.0)
        return ((x[0], x[1]), bus_avg)

    # Businesses rated by this user!
    business_check = uid_bid_rating[uid]

    # Users that rated this business!
    users_bid_main = bid_uids_rating[bid_main].keys()

    # Users that rated this business(dictionary)
    users_bid_main_dict = bid_uids_rating[bid_main]  # {user:rating, ..}
    correlation_items = []
    for business in business_check:

        # We need the correlations between the businesses!
        if (bid_main, business) in correlation_dict:
            corr = correlation_dict[(bid_main, business)]
            correlation_items.append((corr, business))
            continue
        elif (business, bid_main) in correlation_dict:
            corr = correlation_dict[(business, bid_main)]
            correlation_items.append((corr, business))
            continue
        if business == bid_main: continue

        # The intersection between the two businesses!
        user_intersection = set(bid_uids_rating[business].keys()) & set(users_bid_main)

        # Only if the length of the intersection between the businesses is greater than or equal to 2!
        if len(user_intersection) >= 25:
            global intersection_size
            intersection_size[(uid, bid_main)] = user_intersection
            # Users that rated this business(dictionary)
            users_id_dict = bid_uids_rating[business]  # {user:rating, ..}
            corr = calc_corr(user_intersection, bid_main, business, users_bid_main_dict, users_id_dict)
            if corr >= 0.2:
                correlation_items.append((corr, business))

        # correlation_items.append((corr, business))

    # Sort the correlation items!
    if len(correlation_items) > 0:
        correlation_items = sorted(correlation_items, reverse=True)[0:30]
        correlation_items_dict = {x[1]: x[0] for x in correlation_items}  # Converting to a dictionary

        # Calculating New Rate
        rating = calc_new_rating(correlation_items_dict, business_check)

        return (x, rating)
    else:
        # Business average if business exists!
        bus_avg = bid_uids_rating_mean.get(bid_main, 3.5)
        return (x, bus_avg)


def get_new_rating_partition(idx, itr):
    op_partition = {}
    user_intersection_size = {}
    for x in itr:
        bid_main = x[1]
        uid = x[0]

        op_partition[(uid, bid_main)] = {}
        # Cold start business
        if bid_main not in bid_uids_rating:
            op_partition[(uid, bid_main)]['Rating'] = 4.0
            op_partition[(uid, bid_main)]['Neighbors'] = 0
            continue
            # op_partition.append((x[0],x[1]),4.0)

        # Cold start user
        if uid not in uid_bid_rating:
            # Business average if business exists!
            bus_avg = bid_uids_rating_mean.get(bid_main, 4.0)
            op_partition[(uid, bid_main)]['Rating'] = bus_avg
            op_partition[(uid, bid_main)]['Neighbors'] = 0
            continue
            # op_partition.append((x[0], x[1]), bus_avg)

        # Businesses rated by this user!
        business_check = uid_bid_rating[uid]

        # Users that rated this business!
        users_bid_main = bid_uids_rating[bid_main].keys()

        # Users that rated this business(dictionary)
        users_bid_main_dict = bid_uids_rating[bid_main]  # {user:rating, ..}
        correlation_items = []
        for business in business_check:

            # We need the correlations between the businesses!
            if (bid_main, business) in correlation_dict:
                corr = correlation_dict[(bid_main, business)]
                correlation_items.append((corr, business))
                continue
            elif (business, bid_main) in correlation_dict:
                corr = correlation_dict[(business, bid_main)]
                correlation_items.append((corr, business))
                continue
            if business == bid_main: continue

            # The intersection between the two businesses!
            user_intersection = set(bid_uids_rating[business].keys()) & set(users_bid_main)

            # Only if the length of the intersection between the businesses is greater than or equal to 2!
            if len(user_intersection) >= 25:
                # user_intersection_size[(uid,bid_main)] = user_intersection
                # Users that rated this business(dictionary)
                users_id_dict = bid_uids_rating[business]  # {user:rating, ..}
                corr = calc_corr(user_intersection, bid_main, business, users_bid_main_dict, users_id_dict)
                if corr >= 0.2:
                    correlation_items.append((corr, business))

            # correlation_items.append((corr, business))

        # Sort the correlation items!
        if len(correlation_items) > 0:
            op_partition[(uid, bid_main)]['Neighbors'] = len(correlation_items)
            correlation_items = sorted(correlation_items, reverse=True)[0:30]
            correlation_items_dict = {x[1]: x[0] for x in correlation_items}  # Converting to a dictionary

            # Calculating New Rate
            rating = calc_new_rating(correlation_items_dict, business_check)
            op_partition[(uid, bid_main)]['Rating'] = rating
            # op_partition.append(x,rating)
        else:
            # Business average if business exists!
            op_partition[(uid, bid_main)]['Neighbors'] = len(correlation_items)
            bus_avg = bid_uids_rating_mean.get(bid_main, 3.5)
            op_partition[(uid, bid_main)]['Rating'] = bus_avg
            # op_partition.append(x, bus_avg)

    return idx, op_partition


# Needed to caclulate
# actual_ratings = rdd_data_val.map(lambda x: ((x[0],x[1]), float(x[2]))).collectAsMap() # {(uid, bid): rating}

# Format we need validation set in
# rdd_data_val = rdd_data_val_orig.map(lambda x:(x[0],(x[1],float(x[2]))))
test_data = rdd_data_val.map(lambda x: (x[0], x[1]))

# We need the keys!
test_data_keys = test_data.collect()
# Calculating RMSE
print("Calling explicit MAP Function!")
# new_ratings = test_data.map(lambda x:get_new_rating(x))
map_partition_op = test_data.mapPartitionsWithIndex(get_new_rating_partition).collect()
model1_dict = {}
new_pred_1 = {}
for i in range(1, len(map_partition_op), 2):
    model1_dict.update(map_partition_op[i])
    for k in (map_partition_op[i].keys()):
        new_pred_1[k] = map_partition_op[i][k]['Rating']

# sums = 0
# for k in new_pred_1.keys():
#    sums = sums + ((new_pred_1[k] - actual_ratings[k]) ** 2)

# print("Root Mean Squared Error(Approach 1):", sqrt(sums/len(new_pred_1.keys())))
print("Total time(Approach 1):", time.time() - start_time)
################################################## APPROACH 2 ##################################################
############################################# DICTIONARIES WE NEED #############################################
uid_groupby = rdd_data.map(lambda x: (x[0], 1)).groupByKey()  # (uid, 1)
bid_groupby = rdd_data.map(lambda x: (x[1], 1)).groupByKey()  # (bid, 1)

unique_bids = list(bid_uids_rating.keys())
unique_uids = list(uid_bid_rating.keys())

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
        part_op.append(([uid_map, bid_map, avg_rating, n_reviews, avg_rating_business, n_review_business], float(x[2])))
    return idx, part_op


def map_func_test_part(idx, itr):
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
        # y_test.append(pair[1])

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

# D Matrix - Do we need this?
# data_dmatrix = xgb.DMatrix(data=x_train_np,label=y_train_np)

# xg_reg = xgb.XGBRegressor(objective ='reg:linear', colsample_bytree = 0.3, learning_rate = 0.1,
#                 max_depth = 5, n_estimators = 100)

# xg_reg = xgb.XGBRegressor(objective ='reg:linear', colsample_bytree = 0.3, learning_rate = 0.1,
#                max_depth = 5, n_estimators = 100)

xg_reg = xgb.XGBRegressor(objective='reg:linear', colsample_bytree=0.1, learning_rate=0.4,
                          max_depth=5, n_estimators=100)

xg_reg.fit(x_train_np, y_train_np)

preds = xg_reg.predict(x_test_np)

new_pred_2 = {}
for i, key in enumerate(test_data_keys):
    new_pred_2[key] = preds[i]

# sums = 0
# count = 0
# for i in range(len(preds)):
#    sums = sums+((preds[i]-y_test[i])**2)
#    count+=1

# RMSE_2 = sqrt(sums/count)
# Mean square error and root mean square error
# print("Root Mean Squared Error(Approach 2):",sqrt(sums/count))
print("Total time(Approach 2):", time.time() - start_time)
########################################## HYBRID CALCULATION ##########################################
a = 0.9
n = 50

hybrid_rating = {}
for i, key in enumerate(test_data_keys):
    if model1_dict[key]['Neighbors'] > n:
        alpha = a
    else:
        alpha = 1 - a
        temp_rating = new_pred_1[key] * alpha + new_pred_2[key] * (1 - alpha)
        hybrid_rating[key] = temp_rating

# sums = 0
# count = 0
# for i,k in enumerate(test_data_keys):
#    sums = sums + ((hybrid_rating[k] - actual_ratings[k]) ** 2)
#    count += 1
# RMSE_Hybrid = sqrt(sums / count)
# if RMSE_Hybrid<0.99:
#    print("Alphas is:{0} and Number of neighbors:{1}".format(a, n))
#    print("Root Mean Squared Error(Hybrid):", sqrt(sums / count))
#    print("This is count:{0}".format(count))


op_file = sys.argv[3]
print("This is output file:", op_file)

outFile = open(op_file, "w")

outFile.write('user_id, business_id, prediction\n')

count = 0
for i, pair in enumerate(rdd_data_val.collect()):
    outFile.write(str(pair[0]) + ',' + str(pair[1]) + ',' + str(preds[i]))
    outFile.write("\n")
    count += 1

print("Total time(Final):", time.time() - start_time)