import time
import sys
import numpy as np
from sklearn.cluster import KMeans
import random
from collections import Counter
from itertools import combinations

#Setting the seed
random.seed(16)

start_time = time.time()

input_file = "/Users/vegi/Documents/USC_Masters/INF553/Homeworks/Homework-6/data/hw6_clustering.txt"
outfile = "HW6_op.txt" #Writing ouptut to disk
# input_file = sys.argv[1]
# num_clusters = sys.argv[2]
# outfile = sys.argv[3]

lines = []
#Attempting to read the file
with open (input_file,'r') as fp:
    line = fp.readline()
    num_observations = 0
    while(line):
        if line.split(',')!=[]:
            num_observations+=1
            line_split = line.split(',')
            line_split[-1] = line_split[-1].strip("\n")
            conv_values = []
            conv_values.append(int(line_split[0]))
            conv_values.append(int(line_split[1]))
            for i in range(2, len(line_split)):
                conv_values.append(float(line_split[i]))
            lines.append(conv_values)
            line = fp.readline()

predicted_cluster = {}
# Global CS dictionary
main_CS_dict = {}
# You know why we need this!
CS_Map = {}

#Number of clusters in ground truth!
num_clusters = 10
new_cluster = num_clusters+1

#Need to extract elements for k_means
#We need only [2:] of each line for this
co_ordinates = [line[2:] for line in lines]

# Step 1. Load 20% of the data randomly.
# Need 20% of samples for K-Means
# Divinding the data into 5 chunnks!
len_data = len(co_ordinates)
chunk_size = int(len_data/5)

# Step 1. Load 20% of the data randomly.
# Need 20% of samples for K-Means
k_means_co_ordinates = co_ordinates[:chunk_size]

# Updating remaining co-ordinates
co_ordinates = co_ordinates[chunk_size:]

# Step 2. Run K-Means (e.g., from sklearn) with a large K (e.g., 5 times of the number of the input clusters)
# Creating the k-means model
kmeans = KMeans(n_clusters=5*num_clusters,random_state=22).fit(k_means_co_ordinates)
labels = kmeans.labels_

# Creating a cluster counter:
cluster_counter = dict(Counter(labels))
# Step 3. In the K-Means result from Step 2, move all the clusters that contain only one point to RS (outliers).
# All clusters with only one element need to be moved to Retained Set
single_elem_cluster = [k for k,v in cluster_counter.items() if v == 1]

RS = []
for k in single_elem_cluster:
    #Obtaining index of this point
    idx = np.where(labels == k)[0]
    # Stores actual points
    RS.append(k_means_co_ordinates[idx[0]])

# New k-means co-ordinates
# Need to get rid of the indices which were making clusters standalone!
k_means_co_ordinates = [ele for idx,ele in enumerate(k_means_co_ordinates) if ele not in RS]

# Step 4. Run K-Means again to cluster the rest of the data points with K = the number of input clusters.
# Running k-means again
kmeans_new = KMeans(n_clusters=num_clusters, random_state=16).fit(k_means_co_ordinates)
labels = kmeans_new.labels_

# Creating a dictionary with cluster info -
# The following format is used - {cluster number: list of indices}
clusters_dict = {}

for id,label in enumerate(labels):
    if label in clusters_dict:
        clusters_dict[label].append(id)
    else:
        clusters_dict[label] = [id]

# Step 5. Use the K-Means result from Step 4 to generate the DS clusters (i.e., discard their points and generate statistics).
DS_dict = {}
for k in clusters_dict:
    sums = np.array([0]*len(k_means_co_ordinates[0]))
    sums_square = np.array([0]*len(k_means_co_ordinates[0]))
    count = 0
    for id in clusters_dict[k]:
        predicted_cluster[tuple(k_means_co_ordinates[id])] = k
        sums = (sums)+np.array(k_means_co_ordinates[id])
        sums_square = (sums_square) + (np.array(k_means_co_ordinates[id])**2)
        count+=1
    #This needs to be done outside the loop
    num = len(clusters_dict[k])
    # Calculating centroid and variance before hand!
    centroid = sums / num
    variance = (sums_square / num) - ((sums / num) ** 2)
    DS_dict[k] = {
        'num':num,
        'SUM':list(sums),
        'SUMSQ':list(sums_square),
        'centroid':list(centroid),
        'std':list(variance**0.5)
    }

# Step 6. Run K-Means on the points in the RS with a large K (e.g., 5 times of the number of the input clusters) to generate CS
# (clusters with more than one points) and RS (clusters with only one point).
# Creating the k-means model

amp_k = 5*num_clusters
skip_flag = 1
CS_dict = {}

# We want to run k-means only if we have sufficient points
if len(RS) < amp_k:
    skip_flag = 0

if skip_flag!=0:
    print("I'm in the skip flag section")
    #MAKE THE CHANGE HERE
    kmeans_CS = KMeans(n_clusters=amp_k, random_state=16).fit(RS)
    labels_CS = kmeans_CS.labels_

    # Creating a cluster counter:
    CS_cluster_counter = dict(Counter(labels_CS))

    # This is the actual retained set
    for k in sorted(CS_cluster_counter.keys()):
        if CS_cluster_counter[k]>1:
            print("Label:{0} ==> Count:{1}".format(k,CS_cluster_counter[k]))

            # Obtaining inidices of the points of this cluster
            indices = np.where(labels_CS == k)[0]

            sums = [0] * len(RS[0])
            sums_square = [0] * len(RS[0])

            # Go over each observation in the cluster
            for id in indices:
                predicted_cluster[tuple(RS[id])] = k
                sums = np.array(sums) + np.array(RS[id])
                sums_square = np.array(sums_square) + (np.array(RS[id]) ** 2)

            # This needs to done outside the loop
            num = len(indices)
            centroid = sums / num
            variance = (sums_square / num) - ((sums / num) ** 2)
            CS_dict[new_cluster] = {
                'num': num,
                'SUM': list(sums),
                'SUMSQ': list(sums_square),
                'centroid': list(centroid),
                'std': list(variance ** 0.5)
            }
            new_cluster+=1

def get_deets():
    num_DS_points = sum([DS_dict[clus]['num'] for clus in DS_dict])
    num_clusters_CS = len(main_CS_dict.keys())
    if num_clusters_CS==0:
        num_CS_points = 0
    else:
        num_CS_points = sum([main_CS_dict[clus]['num'] for clus in main_CS_dict])
    return num_DS_points,num_clusters_CS,num_CS_points

round_op = []
num_DS_points,num_clusters_CS,num_CS_points = get_deets()
round_op.append([num_DS_points,num_clusters_CS,num_CS_points,len(RS)])

# num_discard_points = sum([DS_dict[clus]['num'] for clus in DS_dict])
# print(num_discard_points)

# Setting the threshold
dimension = len(lines[0][2:])
# print("This is dimension:",dimension)
threshold = 2*(dimension**0.5)
# print("This is threshold:",threshold)

# main_CS_dict_label = 0
def check_md(dic,point):
    min_dist = 999999999
    min_dist_cluster = 999

    # Compute Mahalanobis distance with each cluster in DS/CS
    for k in dic:
        centroid = np.array(dic[k]['centroid'])
        std = np.array(dic[k]['std'])

        # Compute Mahalanobis Distance
        mahalanobis_distance = np.sqrt(np.sum(np.square((point - centroid) / std)))

        # Getting the minimum distance!
        if mahalanobis_distance < min_dist:
            min_dist = mahalanobis_distance
            min_dist_cluster = k

    return min_dist,min_dist_cluster

def update_dict(dic_cluster,point):
    num = dic_cluster['num']
    dic_cluster['num']+=1

    sums = np.array(dic_cluster['SUM'].copy())
    sums_square = np.array(dic_cluster['SUMSQ'])

    sums = sums + point
    sums_square = sums_square + (point ** 2)

    centroid = sums / num
    variance = (sums_square / num) - ((sums / num) ** 2)

    dic_cluster['SUM'] = list(sums)
    dic_cluster['SUMSQ'] =  list(sums_square)
    dic_cluster['centroid'] = list(centroid)
    dic_cluster['std'] = list(variance ** 0.5)

    return dic_cluster

def merge_clusters(CS_dict):
    global new_cluster
    count = 0
    for clus1 in CS_dict:
        if len(main_CS_dict.keys())==0:
            main_CS_dict[new_cluster] = CS_dict[clus1]
            for pt in CS_Map[clus1]:
                predicted_cluster[tuple(pt)] = new_cluster
            new_cluster+=1
            continue
        min_dist = 999999999
        min_dist_cluster = 999
        point = np.array(CS_dict[clus1]['centroid'])
        for clus2 in main_CS_dict:
            # if clus1!=clus2:
            centroid = np.array(main_CS_dict[clus2]['centroid'])
            std = np.array(main_CS_dict[clus2]['std'])

            # Compute Mahalanobis Distance
            mahalanobis_distance = np.sqrt(np.sum(np.square((point - centroid) / std)))

            # Getting the minimum distance!
            if mahalanobis_distance < min_dist:
                min_dist = mahalanobis_distance
                min_dist_cluster = clus2

        # Checking if the minimum distance is less than threshold, then we need to add to CS for that particular cluster
        if min_dist < threshold:
            new_num = main_CS_dict[min_dist_cluster]['num']+CS_dict[clus1]['num']
            new_sum = np.array(main_CS_dict[min_dist_cluster]['SUM'])+ np.array(CS_dict[clus1]['SUM'])
            new_sumsq = np.array(main_CS_dict[min_dist_cluster]['SUMSQ'])+ np.array(CS_dict[clus1]['SUMSQ'])

            centroid = new_sum / new_num
            variance = (new_sumsq / new_num) - ((new_sum / new_num) ** 2)

            main_CS_dict[min_dist_cluster]['num'] = new_num
            main_CS_dict[min_dist_cluster]['SUM'] = list(new_sum)
            main_CS_dict[min_dist_cluster]['SUMSQ'] = list(new_sumsq)
            main_CS_dict[min_dist_cluster]['centroid'] = list(centroid)
            main_CS_dict[min_dist_cluster]['std'] = list(variance ** 0.5)

            # Updating the points in the cluster with the cluster number we just merged to
            for pt in CS_Map[clus1]:
                predicted_cluster[tuple(pt)] = min_dist_cluster

        #Add as a seperate cluster
        else:
            main_CS_dict[new_cluster] = CS_dict[clus1]
            for pt in CS_Map[clus1]:
                predicted_cluster[tuple(pt)] = new_cluster
            new_cluster += 1

    return

count = 0
count_CS = 0

while(len(co_ordinates)!=0):
    # Need 20% of samples for K-Means
    curr_chunk = co_ordinates[:chunk_size]

    # Updating remaining co-ordinates
    co_ordinates = co_ordinates[chunk_size:]
    # if len(co_ordinates)<=100:
    #     curr_chunk.extend(co_ordinates)
    #     co_ordinates = []

    print("Remaining elements:", len(co_ordinates))

    # For each point
    for i,point in enumerate(curr_chunk):
        # Converting point to a numpy array
        point = np.array(point)
        # Compute Mahalanobis distance with each cluster in DS
        min_dist_DS, min_dist_cluster_DS = check_md(DS_dict,point)

        # Checking if the minimum distance is less than threshold, then we need to add to DS for that particular cluster
        if min_dist_DS < threshold:
            #We have to update the deets of this dict
            cluster_dict = DS_dict[min_dist_cluster_DS].copy()
            updated_dict = update_dict(cluster_dict,point)
            DS_dict[min_dist_cluster_DS] = updated_dict

            #Updating predicted cluster
            predicted_cluster[tuple(list(point))] = min_dist_cluster_DS
            continue

        # Compute Mahalanobis distance with each cluster in CS
        min_dist_CS, min_dist_cluster_CS = check_md(main_CS_dict, point)

        # Checking if the minimum distance is less than threshold, then we need to add to CS for that particular cluster
        if min_dist_CS < threshold:
            #We have to update the deets of this dict
            cluster_dict = main_CS_dict[min_dist_cluster_CS].copy()
            updated_dict = update_dict(cluster_dict, point)
            main_CS_dict[min_dist_cluster_CS] = updated_dict
            count_CS += 1
            predicted_cluster[tuple(list(point))] = min_dist_cluster_CS
            continue

        # THE RS case!
        RS.append(point)

        # If we have insufficient points in RS, don't run k-means
        if len(RS)<amp_k:
            continue

        else:

            RS_kmeans = KMeans(n_clusters=num_clusters, random_state=16).fit(RS)
            RS_labels = RS_kmeans.labels_

            RS_cluster_counter = Counter(RS_labels)
            indices_delete = []
            count = 0
            new_RS = []
            # This is the actual retained set
            for k in sorted(RS_cluster_counter.keys()):
                if RS_cluster_counter[k] > 1:
                    count+=RS_cluster_counter[k]

                    # Obtaining inidices of the points of this cluster
                    indices = np.where(RS_labels == k)[0]
                    #Indices to drop from RS
                    indices_delete.extend(indices)

                    sums = np.array([0] * len(co_ordinates[0]))
                    sums_square = np.array([0] * len(co_ordinates[0]))

                    # Go over each observation in the cluster
                    for id in indices:
                        # This is CS_mapping
                        if k not in CS_Map:
                            CS_Map[k] = [RS[id]]
                        else:
                            CS_Map[k].append(RS[id])

                        if type(RS[id])==list:
                            RS[id] = np.array(RS[id])

                        sums = sums + RS[id]
                        sums_square = sums_square + (RS[id] ** 2)
                        sums_square = sums_square + (RS[id] ** 2)

                    # This needs to done outside the loop
                    num = len(indices)
                    centroid = sums / num
                    variance = (sums_square / num) - ((sums / num) ** 2)
                    CS_dict[k] = {
                        'num': num,
                        'SUM': list(sums),
                        'SUMSQ': list(sums_square),
                        'centroid': list(centroid),
                        'std': list(variance ** 0.5)
                    }

                    # new_cluster += 1
                else:
                    indices = np.where(RS_labels == k)[0]
                    new_RS.append(list(RS[indices[0]]))
            #Updating RS
            RS = new_RS.copy()
            # print(RS)
            #Merging clusters
            merge_clusters(CS_dict)

    num_DS_points, num_clusters_CS, num_CS_points = get_deets()

    # Need to store the deets of each round
    round_op.append([num_DS_points, num_clusters_CS, num_CS_points, len(RS)])

print("This is the number of points predicted(right after the loop):",len(predicted_cluster.keys()))
print("This is the number of points in RS:",len(RS))
print("This is the number of lines:",len(lines))

# If any points are left in the RS, then add them to the outliers
if len(RS)!=0:
    for pt in RS:
        predicted_cluster[tuple(pt)] = -1


# Forming the op
op_dict = DS_dict.copy()
max_clus = max(DS_dict.keys())+1
# This is code after all chunks have been read
for clus in main_CS_dict:
    min_dist = 999999999
    min_dist_cluster = 999
    point = np.array(main_CS_dict[clus]['centroid'])
    for clus2 in DS_dict:
        # if clus1!=clus2:
        centroid = np.array(DS_dict[clus2]['centroid'])
        std = np.array(DS_dict[clus2]['std'])

        # Compute Mahalanobis Distance
        mahalanobis_distance = np.sqrt(np.sum(np.square((point - centroid) / std)))

        # Getting the minimum distance!
        if mahalanobis_distance < min_dist:
            min_dist = mahalanobis_distance
            min_dist_cluster = clus2

    # Checking if the minimum distance is less than threshold, then we need to add to CS for that particular cluster
    if min_dist < threshold:
        new_num = DS_dict[min_dist_cluster]['num'] + main_CS_dict[clus]['num']
        new_sum = np.array(DS_dict[min_dist_cluster]['SUM']) + np.array(main_CS_dict[clus]['SUM'])
        new_sumsq = np.array(DS_dict[min_dist_cluster]['SUMSQ']) + np.array(main_CS_dict[clus]['SUMSQ'])

        centroid = new_sum / new_num
        variance = (new_sumsq / new_num) - ((new_sum / new_num) ** 2)

        DS_dict[min_dist_cluster]['num'] = new_num
        DS_dict[min_dist_cluster]['SUM'] = list(new_sum)
        DS_dict[min_dist_cluster]['SUMSQ'] = list(new_sumsq)
        DS_dict[min_dist_cluster]['centroid'] = list(centroid)
        DS_dict[min_dist_cluster]['std'] = list(variance ** 0.5)

        #We need to change the deets of all the points in the CS cluster that was merged
        keys_change = list(predicted_cluster.keys())[list(predicted_cluster.values()).index(clus)]

        for cl in keys_change:
            predicted_cluster[cl] = min_dist_cluster
    else:
        # We need to change the deets of all the points in the CS cluster that was merged
        change_pts = []
        for pt in predicted_cluster:
            if predicted_cluster[pt] == clus:
                change_pts.append(pt)

        for cl in change_pts:
            predicted_cluster[cl] = -1

        op_dict[max_clus] = main_CS_dict[clus]
        max_clus+=1

true_labels = []
predicted_labels = []
for line in lines:
    true_labels.append(line[1])
    key = line[2:]
    predicted_labels.append(predicted_cluster[tuple(key)])

print("This is the number of points predicted:",len(predicted_cluster.keys()))
print("True number of points:",len(lines))

test_lines = [tuple(line) for line in lines]
extra = set(predicted_cluster.keys())-set(test_lines)
print(len(extra))

print("Diff:",len(lines)-len(predicted_cluster.keys()))


from sklearn.metrics import normalized_mutual_info_score
score = normalized_mutual_info_score(true_labels,predicted_labels)

print("This is accuracy:",score)
outFile = open(outfile, "w")
outFile.write('The intermediate results:\n')
for id,op in enumerate(round_op):
    temp_key = [str(i) for i in op]
    str_op = ','.join(temp_key)
    outFile.write("Round "+str(id+1)+": "+str_op+"\n")

outFile.write('\n')
outFile.write('The clustering results:\n')
for id,line in enumerate(lines):
    key = line[2:]
    outFile.write(str(id)+","+str(predicted_cluster[tuple(key)])+"\n")

print("Total time:",time.time()-start_time)

print("Additional test:")

# Creating ground truth dictionary
ground_truth = {}
for line in lines:
    if line[1] in ground_truth:
        ground_truth[line[1]].append(line[2:])
    else:
        ground_truth[line[1]] = [line[2:]]

# for clus in ground_truth.keys():
#     clus_data = ground_truth[clus]
#     sums = [0]*len(clus_data[0])
#     for pt in clus_data:
#         sums+=np.array(pt)
#     true_centroid = sums/len(clus_data)
#     for k in op_dict:
#         pred_centroid = (op_dict[k]['SUM']/np.array(op_dict[k]['num'])



