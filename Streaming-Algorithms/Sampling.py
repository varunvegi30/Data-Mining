import random
import sys

random.seed(553)

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


import binascii


# For each line, the first column is the sequence number (starting from 1)
# of the latest user in the entire streaming, then the 1th user (with index 0 in your list),
# 21th user, 41th user, 61th user and 81th user in your reservoir.
reservoir = []
out_file = sys.argv[4]
with open(out_file,'w') as writeFile:
    print("In write")
    writeFile.write("seqnum,0_id,20_id,40_id,60_id,80_id\n")
    
    total_count = 0
    n = 100
    for _ in range(num_asks):
        # print("This is ask:",_+1)
        # Obtain the users
        # print("This is ask:",_+1)
        users = bx.ask(users_fn, 100)

        if _ == 0:
            reservoir.extend(users)
            #print("This is reservoir:", reservoir)
            writeFile.write(str(n)+","+reservoir[0]+","+reservoir[20]+","+reservoir[40]+","+reservoir[60]+","+reservoir[80]+"\n")
            continue
            
        
        count = 0
        for user in users:
            n = n+1
            # prob_keep = 100/len(actual_stream)
            prob_keep = len(reservoir)
            #This is random number we're selecting
            rand_num = random.randint(0, 100000)
            
            #Assume you want to simulate a probability of p / q. Any random number, random.randint(any integer) % q will
            #give you a value between 0 and q - 1.(q numbers)

            # 0 to p - 1: p values: If we want prob p / q then the number will have to be in this range.
            # p to q - 1: (q - p) values
            # Hence doing random.randint() % q < p will simulate a prob of p / q.
            
            if rand_num%n<prob_keep:
                count+=1
                number_replace = random.randint(0, 100000) % 100
                reservoir[number_replace] = user
        total_count+=count
        # print("This is reservoir:",reservoir)
        writeFile.write(str(n)+","+reservoir[0]+","+reservoir[20]+","+reservoir[40]+","+reservoir[60]+","+reservoir[80]+"\n")
            
print("Replaced:",total_count)

