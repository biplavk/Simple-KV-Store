import string
import random
from random import seed
from random import randint

# initializing size of string
N = 1024 * 1024
M = 127
# using random.choices()
# generating random strings

# seed random number generator
seed(0)
# generate some integers
keySet = set()
#preLoad  = open("./10K_PreLoad","w")
#f = open("data/4MB_PUT", "w")
#fd = open("data/GET", "w")
# preLoadCount = 0;
# while (preLoadCount < 100):
#     key   = ''.join(random.choices(string.ascii_uppercase +  string.digits, k = M))
#     value = ''.join(random.choices(string.ascii_uppercase +  string.digits, k = N))
#     keySet.add(key)
#     line1 = "PUT " + str(key)+" " + str(value) +"\n"
#     preLoad.write(line1)
#     preLoadCount = preLoadCount +1
f = open("TESTDATA", "w")
LoadCount = 0
putCounter = 0
getCounter = 0
prefixCounter = 0
while (LoadCount < 1000):
    key   = ''.join(random.choices(string.ascii_uppercase +  string.digits, k = M))
    value = ''.join(random.choices(string.ascii_uppercase +  string.digits, k = N))
    keySet.add(key)
    num = randint(0, 2)
    p = randint(0, 10)
    line1 = ""
    if num == 0 :

        if p % 2 == 0 :
            key = random.sample(keySet,1)[0]
        line1 = "PUT " + str(key)+" " + str(value) +"\n"
        putCounter = putCounter +1
    if num == 1 :
        getCounter = getCounter +1
        if p % 2 == 0 :
            key = random.sample(keySet,1)[0]
        line1 = "GET " + str(key)+"\n"
    if num == 2 :
        prefixCounter = prefixCounter +1
        if p % 2 == 0 :
            key = random.sample(keySet,1)[0]
        s = str(key)[0:10]
        line1 = "PREFIX " + s + "\n"
    f.write(line1)
    LoadCount = LoadCount +1
print("Number of PUT " + str(putCounter) + "\n")
print("Number of GET " + str(getCounter) + "\n")
print("Number of PREFIX " + str(prefixCounter) + "\n")
