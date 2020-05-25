from pyspark import SparkContext
from pyspark import SparkConf
import time
from itertools import islice
from math import sqrt
import math
import sys

start_time = time.time()

sc = SparkContext(master='local[*]', appName='scObject')
sc.setLogLevel("ERROR")

# Path to the file
#Train Dataset
# input_file = "/Users/vegi/Documents/USC_Masters/INF553/Homeworks/Homework-3/data/yelp_train.csv"
input_file = sys.argv[1]
#Validation Dataset
# val_file = "/Users/vegi/Documents/USC_Masters/INF553/Homeworks/Homework-3/data/yelp_val.csv"
val_file = sys.argv[2]

# Reading the raw data as a text file
raw_data = sc.textFile(input_file)

#Converting text file
rdd_data = raw_data.map(lambda x: x.split(","))

header = rdd_data.first() #extract header

#To get csv without header
rdd_data = rdd_data.mapPartitionsWithIndex(lambda idx, it: islice(it, 1, None) if idx == 0 else it)

#This is the format we used - (bid , (uid, rating))
#Creating a dictionary ---> {bid:{uid:rating, uid:rating.....}}
bid_uids_rating = {}
bid_uids_rating = rdd_data.map(lambda x:(x[1],(x[0],float(x[2])))).groupByKey().mapValues(dict).collectAsMap()

#Getting the average rating for each business!
bid_uids_rating_mean = {}
for key in bid_uids_rating.keys():
    bid_uids_rating_mean[key] = sum(bid_uids_rating[key].values())/len(bid_uids_rating[key].values())

#This is the format we used - (uid , (bid, rating))
#Creating a dictionary ---> {uid:{bid:rating, bid:rating.....}}
uid_bid_rating = {}
uid_bid_rating = rdd_data.map(lambda x:(x[0],(x[1],float(x[2])))).groupByKey().mapValues(dict).collectAsMap()

############################################# THIS IS WHERE THE CODE STARTS #############################################
correlation_dict = {}

#Function calculate correlation between businesses
def calc_corr(user_intesection, bid_main, id,users_bid_main_dict,users_id_dict):
    u_ratings = []
    v_ratings = []
    #Rating made by common users
    for user in user_intesection:
        u_ratings.append(users_bid_main_dict[user])
        v_ratings.append(users_id_dict[user])

    #Average rating of the business (Using all user ratings)
    mean_u = bid_uids_rating_mean[bid_main]
    mean_v = bid_uids_rating_mean[id]

    #Correlation Calculation
    numerator = sum([((users_bid_main_dict[x] - mean_u) * (users_id_dict[x] - mean_v)) for x in user_intesection])
    denominator_a = sqrt(sum([((users_bid_main_dict[x] - mean_u) ** 2) for x in user_intesection]))
    denominator_b = sqrt(sum([((users_id_dict[x] - mean_v) ** 2) for x in user_intesection]))

    #Set correlation to zero if denominator is zero
    if denominator_a == 0.0 or denominator_b == 0.0:
        corr = 0
        correlation_dict[(bid_main, id)] = corr
        return corr

    else:
        corr = numerator / (denominator_a * denominator_b)
        if corr >= 0:
            correlation_dict[(bid_main,id)] = corr
        return corr

#Calcuting new rating
def calc_new_rating(correlation_items,business_check):

    businesses_use = correlation_items.keys()

    # Values being used
    # correlation_items -> Get the correlation between the two items
    # business_check[bu] -> Check the rating given by the pertinent user

    num = sum([correlation_items[bu] * business_check[bu] for bu in businesses_use])
    denom = sum([abs(correlation_items[bu]) for bu in businesses_use])

    if denom==0:
        new_rating = 0.0

    else:
        new_rating = float(num / denom)
        #Negative and greater than 5 case
        if new_rating<0.0: new_rating = 0
        if new_rating>5.0: new_rating = 5.0

    return new_rating

def get_new_rating(x):
    bid_main = x[1]
    uid = x[0]

    #Cold start business
    if bid_main not in bid_uids_rating:
        return ((x[0],x[1]),4.0)

    # Cold start user
    if uid not in uid_bid_rating:
        #Business average if business exists!
        bus_avg = bid_uids_rating_mean.get(bid_main, 4.0)
        return ((x[0], x[1]), bus_avg)

    #Businesses rated by this user!
    business_check = uid_bid_rating[uid]

    #Users that rated this business!
    users_bid_main = bid_uids_rating[bid_main].keys()

    # Users that rated this business(dictionary)
    users_bid_main_dict = bid_uids_rating[bid_main]  # {user:rating, ..}
    correlation_items = []
    for business in business_check:

        #We need the correlations between the businesses!
        if (bid_main,business) in correlation_dict:
            corr = correlation_dict[(bid_main,business)]
            correlation_items.append((corr, business))
            continue
        elif (business, bid_main) in correlation_dict:
            corr = correlation_dict[(business, bid_main)]
            correlation_items.append((corr, business))
            continue
        if business == bid_main: continue

        #The intersection between the two businesses!
        user_intersection = set(bid_uids_rating[business].keys()) & set(users_bid_main)

        # Only if the length of the intersection between the businesses is greater than or equal to 2!
        if len(user_intersection)>=25:
            # Users that rated this business(dictionary)
            users_id_dict = bid_uids_rating[business]  # {user:rating, ..}
            corr = calc_corr(user_intersection, bid_main, business,users_bid_main_dict,users_id_dict)
            if corr>=0.2:
                correlation_items.append((corr, business))

        # correlation_items.append((corr, business))

    #Sort the correlation items!
    if len(correlation_items)>0:
        correlation_items = sorted(correlation_items, reverse=True)[0:30]
        correlation_items_dict = {x[1]: x[0] for x in correlation_items} #Converting to a dictionary

        #Calculating New Rate
        rating = calc_new_rating(correlation_items_dict,business_check)

        return (x,rating)
    else:
        # Business average if business exists!
        bus_avg = bid_uids_rating_mean.get(bid_main, 3.5)
        return (x, bus_avg)

#Reading the validation data:
raw_data_val = sc.textFile(val_file)

# Extract header
header_val = raw_data_val.first()

# print("This is the validation header:",header_val)
rdd_data_val = raw_data_val.map(lambda x: x.split(","))

# print("Validation data:",rdd_data_val.take(3))

#Removing the header
rdd_data_val_orig = rdd_data_val.mapPartitionsWithIndex(lambda idx, it: islice(it, 1, None) if idx == 0 else it)

#Needed to caclulate
#actual_ratings = rdd_data_val_orig.map(lambda x: ((x[0],x[1]), float(x[2]))).collectAsMap() # {(uid, bid): rating}

# Format we need validation set in
test_data = rdd_data_val_orig.map(lambda x:(x[0],x[1]))
#Calculating RMSE
print("Calling explicit MAP Function!")
new_ratings = test_data.map(lambda x:get_new_rating(x))

# outFilePath = '/Users/vegi/Documents/USC_Masters/INF553/Homeworks/Homework-3/OP/HW3_2_1.txt'
outFilePath = sys.argv[3]
print("This is output file:",outFilePath)

outFile = open(outFilePath, "w")

outFile.write('user_id,business_id, prediction\n')
for line in new_ratings.collect():
    outFile.write(str(line[0][0]) + ',' + str(line[0][1]) + ',' + str(line[1]))
    outFile.write("\n")
outFile.close()
print("Total time:",time.time()-start_time)