import random
import sys


#class BlackBox:

#    def ask(self, file, num):
#        lines = open(file, 'r').readlines()
#        users = [0 for i in range(num)]
#        for i in range(num):
#            users[i] = lines[random.randint(0, len(lines) - 1)].rstrip("\n")
#        return users


# This is how we read the data!
from blackbox import BlackBox
bx = BlackBox()

users_fn = sys.argv[1]
stream_size = int(sys.argv[2])
num_asks = int(sys.argv[3])
print("This is user FN:", users_fn)
print("This is stream size:", stream_size)
print("This is number of asks:", num_asks)

# Task 2.1 -- Bloom Filter
filter_bit_array = [0] * 69997

import binascii

num_rows = 69997
all_primes = [2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71, 73, 77, 79, 83]


def myhash(s):
    global filter_bit_array
    result = []
    test_primes = [2, 3, 5]
    for num in test_primes:
        result.append((num * s + 7) % num_rows)

    return result


# For updating the bit array
def update_bit(indices):
    global filter_bit_array
    for id in indices:
        filter_bit_array[id] = 1


# Using S to populate filter_bit_array
actual_stream = []
unique_users_sanity = []
unique_users_algo = []
op_list = []

not_unique = []
for _ in range(num_asks):
    # Obtain the users
    # print("This is ask:",_+1)
    FP_count = 0
    TN_count = 0
    users = bx.ask(users_fn, stream_size)
    for user in users:
        # Just keeping tabs on the actual stream
        actual_stream.append(user)

        # Converting it into a numerical value
        num_val = int(binascii.hexlify(user.encode('utf8')), 16)

        # Obtaining the hash values
        result = myhash(num_val)
        # print(result)
        # The indices returned by hash functions
        flag = 0
        skip_other_check = 0
        for i in result:
            if filter_bit_array[i] == 0:
                flag += 1
                # unique_users_algo.append(result)
                # update_bit(i)
                # filter_bit_array[i] = 1

        # All bits are 0, definitely a TN
        if flag == len(result):
            TN_count+=1
            skip_other_check = 1
            unique_users_algo.append(result)
            update_bit(result)


        if flag<=len(result) and skip_other_check:
            if user in unique_users_sanity:
                # print("OO LALA")
                TN_count += 1
                unique_users_algo.append(result)
                update_bit(result)

        # Keeping tabs on users pointed out as FP
        # The algorithm says that this is not a new user, but it is!
        if flag == 0:
            # Algorithm says it's not a new user, but it is
            # We haven't seen this unique user before
            if user not in unique_users_sanity:
                # print("FP for user:", user)
                # print("The unique user is:")
                FP_count += 1

        # This is a sanity check stream
        if user not in unique_users_sanity:
            unique_users_sanity.append(user)

    # FPR = FP_count / 100
    if FP_count+TN_count == 0:
        FPR = 0.0
    else:
        FPR = FP_count / (FP_count + TN_count)
#    print("False Positive Rate:", FPR)
#    print("FP:{0},TN:{1}".format(FP_count,TN_count))
#    print("**********************************************")
    op_list.append((_, FPR))

print("Number of unique users:(actual)", len(unique_users_sanity))
print("Number of unique users(algo):", len(unique_users_algo))
print("Difference is:", len(unique_users_sanity) - len(unique_users_algo))

out_file = sys.argv[4]
with open(out_file, 'w') as writeFile:
    # print("In write")
    writeFile.write("Time,FPR\n")
    for pair in op_list:
        test = str(pair[0]) + ',' + str(pair[1])
        writeFile.write(test + "\n")

