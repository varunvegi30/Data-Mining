# Data-Mining
Tech Stack : Python (Pandas, NumPy, PySpark), Spark, XGBoost, Scala

Examples of data mining projects I worked on during my time at USC - including finding frequent itemsets, Locality Sensitive Hashing to find similar items, recommendation systems, Community Detection in large graphs, Streaming Algorithms (Bloom Filtering, Flajolet Martin Algorithm) and BFR algorithm for clustering on large datasets.

## Finding Frequent Itemsets
In this project, I implemented SON Algorithm on top of the Apache Spark Framework. The goal was find all the possible combinations of the frequent itemsets in any given input file within the required time. I made use of the Ta Feng grocery dataset that can be found here.

Code can be executed using the following format – ./spark-submit FreqItems.py <filter threshold> <support> <input_file_path> <output_file_path> 

## Finding Similar Users using Jaccard Similarity and Locality Sensitive Hashing
In this project, I implemented the Locality Sensitive Hashing algorithm with Jaccard similarity. I focused on the “0 or 1” ratings rather than the actual ratings/stars from the users. Specifically, if a user has rated a business, the user’s contribution in the characteristic matrix is 1. If the user hasn’t rated the business, the contribution is 0. I used Jaccard Similarity of >= 0.5 to identify similar businesses.

I achieved a __precision of 1.0 and recall of  0.972__

Code can be executed using the following – ./spark-submit LSH.py <input_file_name> <output_file_name> 

## Recommendation System to Predict User Ratings on Unseen Businesses on the yelp dataset
In this project, I built a recommendation engine in three phases –

### Case 1: Item-based CF recommendation system with Pearson Similarity

Code can be executed using the following command – ./spark-submit Item_Based_CF.py <train_file_name> <test_file_name><output_file_name>
### Case 2: Model Based Recommendation system using XGBRegressor (a regressor based on decision trees).

Code can be executed using the following command – ./spark-submit Item_Based.py <folder_path> <test_file_name><output_file_name
### Case 3: Hybrid Recommendation System – where I made use of weighted model

##### 𝑓𝑖𝑛𝑎𝑙𝑠𝑐𝑜𝑟𝑒=𝛼×𝑠𝑐𝑜𝑟𝑒𝑖𝑡𝑒𝑚_𝑏𝑎𝑠𝑒𝑑 +(1−𝛼)×𝑠𝑐𝑜𝑟𝑒𝑚𝑜𝑑𝑒𝑙_𝑏𝑎𝑠𝑒𝑑

Code can be executed using the following command – ./spark-submit Hybrid.py <folder_path> <test_file_name><output_file_name>

__Achieved the lowest RMSE of 0.98 with the Hybrid Recommendation System__

## Community Detection in Large Graphs
In this project, I explored the spark GraphFrames library as well as implemented the Girvan-Newman algorithm using the Spark Framework to detect communities in graphs. There are two scripts in this folder –

### LPA.py
Implemented the Label Propagation Algorithm (LPA) which was proposed by Raghavan, Albert, and Kumara in 2007. It is an iterative community detection solution whereby information “flows” through the graph based on underlying edge structure. Used the graphframes library to implement the same.

Code can be executed using the following command – spark-submit --packages graphframes:graphframes:0.6.0-spark2.3-s_2.11 LPA.py <input_file_path> <community_output_file_path> 

### GirvanNewman.py
I implemented the Girvan-Newman algorithm to detect the communities in the network graph from scratch. First, I calculated the betweenness of each edge in the original graph. Then I calculated the correct number of communities by calculating the modularity at different community sizes.

Code can be executed using the following command – spark-submit GirvanNewman.py <input_file_path> <betweenness_output_file_path> <community_output_file_path> 

I also implemented both the tasks in scala.

## Streaming Algorithms	
In this project, I implemented three streaming algorithms –

### Bloom Filtering – To check if a given user_id has been seen before in a continuous stream of data

Code can be executed using the following command – python BloomFiltering.py <input_filename> stream_size num_of_asks <output_filename> 

### Flajolet Martin Algorithm – To estimate number of users within a window in a continuous data stream.

Code can be executed using the following command – python FM.py <input_filename> stream_size num_of_asks <output_filename> 

### Fixed Size sampling – In this project, I assumed that the memory can only save 100 users, so we need to use the fixed size sampling method to only keep part of the users as a sample in the streaming. 

Code can be executed using the following command – python Sampling.py <input_filename> stream_size num_of_asks <output_filename> 

## Bradley-Fayyad-Reina (BFR) algorithm for clustering on large datasets

Achieved an accuracy of __~99%__ on a synthetic test dataset with approximately 200,000 observations.

Code can be executed using the following command – python3 BFR.py <input_file> <n_cluster> <output_file> 


_If you need access to the datasets I used, contact me._














