import random
import sys
from statistics import median,mean

#class BlackBox:

#    def ask(self, file, num):
#        lines = open(file,'r').readlines()
#        users = [0 for i in range(num)]
#        for i in range(num):
#            users[i] = lines[random.randint(0, len(lines) - 1)].rstrip("\n")
#        return users
#This is how we read the data!
from blackbox import BlackBox
bx = BlackBox()

users_fn = sys.argv[1]
stream_size = int(sys.argv[2])
num_asks = int(sys.argv[3])
print("This is user FN:",users_fn)
print("This is stream size:",stream_size)
print("This is number of asks:",num_asks)

#Task 2.1 -- Bloom Filter
filter_bit_array = [0]*69997

import binascii


import math
def isPrime(n):
    # 3 cases we need to look at
    if (n <= 1):
        return False
    if (n <= 3):
        return True

    # middle five numbers in below loop
    if (n % 2 == 0 or n % 3 == 0):
        return False

    for i in range(5, int(math.sqrt(n) + 1), 6):
        if (n % i == 0 or n % (i + 2) == 0):
            return False

    return True

def next_prime(x):
    not_prime = True
    while(not_prime):
        is_prime = isPrime(x)
        if(is_prime == True):
            break
        x+=1
    return x



# Using the next prime 
num_rows = next_prime(int(stream_size*2.8))

#num_rows = stream_size*3
all_primes = [2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71, 73, 77, 79, 83]
def myhash(s):
    global filter_bit_array
    result = []
    for num in all_primes:
        result.append((num * s + 7) % num_rows)

    return result

#We're trying 8 functions per group
res_range = [i  for i in range(len(all_primes)+1) if i%8==0]
avg_vals = []
op_list = []

sum_estimates = 0
sum_ground_truth = 0

for _ in range(30):
    users = bx.ask(users_fn, 300)
    max_zeros = [0]*len(all_primes)
    mean_groups = []
    estimates_ask = 0
    ground_truth_ask = 0
    for user in users:
        #Let's get the hash value!
        # Converting it into a numerical value
        num_val = int(binascii.hexlify(user.encode('utf8')), 16)

        #Obtaining the hash values
        result = myhash(num_val)
        #Obntaining the binary for each hash value
        result_bin = [bin(i) for i in result]

        # To find the least significant 1
        # reversing all the binary values
        # finding the number of zeros
        num_zeros = [i[::-1].find('1') for i in result_bin]

        #Go through each r we have here and check if any crosses the max r we have
        for i,r in enumerate(num_zeros):
            if r>max_zeros[i]:
                max_zeros[i] = r


    #After we have max r for each ask lets compute the averages!
    #Obtainging 2^R for each max_zeros
    power_r = [(2**r) for r in max_zeros]

    #Iterating over the range of indices:
    mean_groups_2 =[]
    for i in range(1,len(res_range)):
        id_range = (res_range[i-1],res_range[i])
        mean_groups.append(int(mean(power_r[id_range[0]:id_range[1]])))

    estimates_ask = int(median(mean_groups))
    ground_truth_ask = len(set(users))

    sum_estimates += estimates_ask
    sum_ground_truth += ground_truth_ask
    op_list.append((_, ground_truth_ask, estimates_ask))

#    print("This is the actual number of unique users:",ground_truth_ask)
#    print("This is the predicted number of unique users:",estimates_ask)
#    print("This is the result for ask:", estimates_ask / ground_truth_ask)

#    print("****************************************")

print("This is the result:",sum_estimates/sum_ground_truth)


out_file = sys.argv[4]
with open(out_file,'w') as writeFile:
    print("In write")
    writeFile.write("Time,Ground Truth,Estimation\n")
    for pair in op_list:
        test = str(pair[0])+','+str(pair[1])+','+str(pair[2])
        writeFile.write(test + "\n")
