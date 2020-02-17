#!/bin/sh
rm -rf *.txt
echo "Folder Cleanup Done"
echo "Generating workload"
#python3 generateTestData.py
echo "Workload Generated"
sleep 1
./kvserver   &
#server_start=$(date +%s%N)
#echo "Preloading 10K Key Value Pairs."
#./kvclient "./10K_PreLoad"
#echo "10K Key Value Pairs Loaded."
sleep 3
a=0
echo "Test Started"
client_start=$(date +%s%N)
while [ $a -lt 10 ]
do
    # Print the values
    path="./TESTDATA"
    ./kvclient "${path}" &
    # increment the value
    sleep 3
    a=`expr $a + 1`
done
#echo "Server Runtime :" $((($(date +%s%N) - $server_start))) " ns"
sleep 3
pkill -f "kvserver"
echo "Test End"
