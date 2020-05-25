from pyspark import SparkContext
from pyspark import SparkConf
from graphframes import *
from collections import defaultdict
import json
import time
import sys
import os


# # Only needed for spark-submit
# os.environ["PYSPARK_SUBMIT_ARGS"] = ( "--packages graphframes:graphframes:0.6.0-spark2.3-s_2.11")
start_time = time.time()

sc = SparkContext(master='local[*]', appName='scObject')
sc.setLogLevel("ERROR")

# Path to the file
input_path = sys.argv[1]

#Test files
# input_path = '/Users/vegi/Documents/USC_Masters/INF553/Homeworks/Homework-4/slides_graph.txt'
# input_path = '/Users/vegi/Documents/USC_Masters/INF553/Homeworks/Homework-4/YT_Graph.txt'

input_file = sc.textFile(input_path)
edges = input_file.map(lambda line: line.split(" ")).map(lambda x:(x[0],x[1]))

#Reading i vertices
nodes_i = edges.keys().collect()
#Reading j vertices
nodes_j = edges.values().collect()

#All of the vertices/nodes
nodes_i.extend(nodes_j)
vertices = sorted(list(set(nodes_i.copy())))
print("Total number of unique vertices:",len(vertices))

#Creating the tree dictionary
tree_dictionary = {}
for i in edges.collect():
    if i[0] in tree_dictionary:
        tree_dictionary[i[0]].append(i[1])
        #Add the reverse edge too
        if i[1] in tree_dictionary:
            tree_dictionary[i[1]].append(i[0])
        else:
            tree_dictionary[i[1]] = [i[0]]
    else:
        tree_dictionary[i[0]] = [i[1]]
        if i[1] in tree_dictionary:
            tree_dictionary[i[1]].append(i[0])
        else:
            tree_dictionary[i[1]] = [i[0]]

updated_tree = tree_dictionary.copy()

#Creating a degree dictionary
degree_dictionary = {k:len(v) for k,v in tree_dictionary.items()}

#print(len(tree_dictionary.keys()))

def map_part_func(idx,itr):

    # function to determine level of
    # each node starting from x using BFS
    def findLevels(graph, x):
        # dictionary to store depth of each node
        level = {}

        # Dictionary to store nodes in each level
        level_dict = {}

        # dictionary to store whether a node has been visited or not
        visited = {k: False for k in vertices}

        # Queue for BFS
        queue = []

        # Starting with the start node
        queue.append(x)

        # root starts at 0
        level[x] = 0

        level_dict[0] = [x]  # Storing root in level 0
        visited[x] = True  # Changing visited status

        # Until queue is empty!
        while (len(queue) != 0):
            # get the first element of queue
            x = queue.pop(0)

            # traverse neighbors of node x
            for n in (graph[x]):
                # n is neighbor
                # if b is not marked already
                if (not visited[n]):
                    # enqueue b in queue
                    queue.append(n)
                    # level of b is level of x + 1
                    level[n] = level[x] + 1
                    if (level[x] + 1) in level_dict:
                        level_dict[level[x] + 1].append(n)
                    else:
                        level_dict[level[x] + 1] = [n]
                    # Visited n!
                    visited[n] = True

        return level, level_dict

    # Function to find the number of paths to each node from the root
    def num_paths(level_dict, max_level):
        num_path_dict = {}
        # Iterating level by level
        for lvl in range(0, max_level + 1):
            # Set number of paths to 1 for node
            for node in level_dict[lvl]:
                if lvl == 0:
                    num_path_dict[node] = 1
                else:
                    # Iterate over edges for the node
                    for edge in updated_tree[node]:
                        # If the destination of the edge is in level-1 increment count
                        if edge in level_dict[lvl - 1]:
                            if node not in num_path_dict:
                                # num_path_dict[node] = 1
                                # Setting it to 1 will cause an error!
                                # Here the initial value is set to the number of ways the parent can be reached
                                num_path_dict[node] = num_path_dict[edge]
                            else:
                                if edge in num_path_dict:
                                    # Increasing count by the number of ways a parent can be reached
                                    num_path_dict[node] += num_path_dict[edge]
        return num_path_dict

    edge_val_dict = {}

    # Function to calculate the sum at each node and edge value
    def root_sum(num_path_dict, max_level, start_node):

        # The first dictionart
        # Every node gets a value 1
        node_val = {}
        for node in num_path_dict.keys():
            if node == start_node:
                # Set it to 0 for the root
                node_val[node] = 0
                continue
            node_val[node] = 1

        # Iterate over all the nodes from max_level-1
        for lvl in range(max_level - 1, -1, -1):
            for node in level_dict[lvl]:
                # Increase the sum only if there is a path to a node in the level below
                for node_below in level_dict[lvl + 1]:
                    if node_below in updated_tree[node]:
                        #################################################
                        # Might have to multiply with a factor, take a look
                        #################################################
                        # int_step[node] = int_step[node] + int_step[node_below]
                        # The value we add is -
                        # Number of ways we can reach a child from a parent : num_path_dict[node]
                        # Contribution the child can make to the edge : node_val[node_below]/num_path_dict[node_below]
                        # This part is probably confusing!
                        add_val = num_path_dict[node] * (node_val[node_below] / num_path_dict[node_below])
                        node_val[node] = node_val[node] + add_val
                        # Just sorting the key
                        if (node) < (node_below):
                            key = (node, node_below)
                        else:
                            key = (node_below, node)
                        if key in edge_val_dict:
                            edge_val_dict[key] += add_val
                        else:
                            edge_val_dict[key] = add_val

        # return the root node value
        return node_val[start_node]
    # print("Im in map partition")
    edge_val_dict = {}
    node_sum_dict = {}
    for pair in itr:
        node = pair[0]
        level, level_dict = findLevels(updated_tree, node)

        # Obtaining the maximum level
        max_level = max(level.values())
        num_path_dict = num_paths(level_dict, max_level)

        # This is the method which does edge calculation
        node_sum = root_sum(num_path_dict, max_level, node)
        # We don't really need this value, it's just for a sanity check
        node_sum_dict[node] = node_sum

    part_op = edge_val_dict.copy()
    # for k in part_op.keys():
    #     part_op[k] = part_op[k] / 2

    return idx,part_op

sorted_vertices = sorted(vertices)
# Iterating over all vertices
#print("Sorted vertices:",sorted_vertices)

vertices_RDD = sc.parallelize(vertices)

num_partitions =  vertices_RDD.getNumPartitions()
vertices_RDD = vertices_RDD.map(lambda x: (x,1))
# vertices_RDD = vertices_RDD.partitionBy(2)
test_op = vertices_RDD.mapPartitionsWithIndex(map_part_func).collect()

edge_betweenness_dict = {}
from collections import Counter

total_elem = len(test_op)
for i in range(1,total_elem,2):
    edge_betweenness_dict = dict(Counter(test_op[i])+Counter(edge_betweenness_dict))

edge_betweenness_dict = {k:v/2 for k,v in edge_betweenness_dict.items()}
sorted_op = sorted(edge_betweenness_dict.items(), key=lambda x: (-x[1],x[0]))

outfile = sys.argv[2]

with open(outfile, 'w') as writeFile:
    for k in sorted_op:
        writeFile.write(str(k[0])+" "+str(k[1])+"\n")


print("************************************************************** THIS IS THE MODULARITY PART **************************************************************")
#Function to form graph dictionary
def form_graph_dict(edge_updated):
    tree_dictionary_updated = {}
    for i in edge_updated:
        if i[0] in tree_dictionary_updated:
            tree_dictionary_updated[i[0]].append(i[1])
            # Add the reverse edge too
            if i[1] in tree_dictionary_updated:
                tree_dictionary_updated[i[1]].append(i[0])
            else:
                tree_dictionary_updated[i[1]] = [i[0]]
        else:
            tree_dictionary_updated[i[0]] = [i[1]]
            if i[1] in tree_dictionary_updated:
                tree_dictionary_updated[i[1]].append(i[0])
            else:
                tree_dictionary_updated[i[1]] = [i[0]]
    return tree_dictionary_updated

#Function which will return each component
def connected_components(G,vertices):
    seen = set()
    for v in G:
        if v not in seen:
            c = set(BFS(G, v))
            yield c
            seen.update(c)

def BFS(graph, x):
    # G_adj = graph
    seen = set()
    nextlevel = {x}
    while nextlevel:
        thislevel = nextlevel
        nextlevel = set()
        for v in thislevel:
            if v not in seen:
                yield v
                seen.add(v)
                nextlevel.update(graph[v])

# Function to calculate modularity
def calc_modularity(component, graph):
    sums = 0
    for node_1 in component:
        for node_2 in component:
            # 1 if an edge exists between the two nodes
            if node_1 in tree_dictionary[node_2]:
                a_ij = 1
            else:
                a_ij = 0
            k_i = len(tree_dictionary[node_1])
            k_j = len(tree_dictionary[node_2])
            degrees_prod = k_i*k_j
            num_edges_double = 2 * num_edges
            sums += (a_ij - (degrees_prod / num_edges_double))

    return sums

# Function to calculate modularity
def calc_modularity_2(components, graph):
    group_sum = 0
    for component in components:
        for node_1 in component:
            for node_2 in component:
            # 1 if an edge exists between the two nodes
                if node_1==node_2:
                   a_ij= 0
                else:
                    a_ij = (node_2 in tree_dictionary[node_1])

                s_um = a_ij - ((len(tree_dictionary[node_1])*len(tree_dictionary[node_2]))/(2*num_edges))
                group_sum = group_sum + s_um
            # k_i = len(tree_dictionary[node_1])
            # k_j = len(tree_dictionary[node_2])
            # degrees_prod = k_i*k_j
            # num_edges_double = 2 * num_edges
            # sums += (a_ij - (degrees_prod / num_edges_double))

    return group_sum/(2*num_edges)


# Sorting each edge
edges = edges.map(lambda x:tuple(sorted(x)))

# Updated tree!
updated_tree = tree_dictionary.copy()
#Do we need this?
component_size = []
# To store edge modularity values
modularity_values_dict = {}
#Dummy max value
max_modularity = -1

edge_updated = edges.collect()
num_edges = edges.count()
# for i in range(num_edges):
components = []
test_op = {}
while (len(components)!=len(sorted_vertices)):
    test_op = vertices_RDD.mapPartitionsWithIndex(map_part_func).collect()

    edge_betweenness_dict = {}
    from collections import Counter

    total_elem = len(test_op)
    for i in range(1, total_elem, 2):
        edge_betweenness_dict = dict(Counter(test_op[i]) + Counter(edge_betweenness_dict))

    edge_betweenness_dict = {k: v / 2 for k, v in edge_betweenness_dict.items()}
    sorted_op = sorted(edge_betweenness_dict.items(), key=lambda x: (-x[1], x[0]))

    #Edge we're deleting -
    #print("Number of edges:",len(sorted_op))
    #print("Print:",sorted_op[0:5])
    from operator import itemgetter

    #print("Max Betweenness:",max(sorted_op, key=itemgetter(1))[1])
    #print("Deleting edge:",sorted_op[0][0])
    edge_updated.remove(sorted_op[0][0])

    #Updating the tree
    updated_tree = form_graph_dict(edge_updated)
    # updated_tree[sorted_op[0][0][0]].remove(sorted_op[0][0][1])
    # updated_tree[sorted_op[0][0][1]].remove(sorted_op[0][0][0])
    for v in sorted_vertices:
        if v not in updated_tree:
            updated_tree[v] = []

    #Obtaining the list of components
    components = list(connected_components(updated_tree, sorted_vertices))
    sum_modularity = 0
    # print("Components are:", components)
    #print("Number of components:",len(components))
    component_size.append(len(components))

    sum_modularity = 0
    for component in components:
        sum_modularity += calc_modularity(component, updated_tree)
    modularity = sum_modularity / (2 * num_edges)
    #print("Modularity:", modularity)

    if len(components) in modularity_values_dict:
        modularity_values_dict[len(components)].append(modularity)
    else:
        modularity_values_dict[len(components)] = [modularity]

    if modularity>=max_modularity:
        max_modularity = modularity
        max_mod_components = components
        best_graph = updated_tree.copy()

    #print("****************************************************************************************************************************")

print("************************************************************** BEST CASE **************************************************************")
print("Best case:")
print("Modularity is:",max_modularity)
#print("Components are:",max_mod_components)
print("Number of components:",len(max_mod_components))
#print("This is the graph:",best_graph)
print("************************************************************** BEST CASE **************************************************************")

# Writing output!

out_file = sys.argv[3]


op_dict = {}
for l in max_mod_components:
    if len(l) in op_dict:
        op_dict[len(l)].append(sorted(l))
    else:
        op_dict[len(l)] = [sorted(l)]

op_dict = {k:sorted(v) for k,v in op_dict.items()}

with open(out_file,'w') as writeFile:
    print("In write")
    for k in sorted(list(op_dict.keys())):
        for v in op_dict[k]:
            test = ', '.join(["'" + i + "'" for i in v])
            writeFile.write(test + "\n")