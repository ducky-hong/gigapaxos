#!/bin/bash

for server in "18.222.196.156" "18.216.90.37" "18.217.42.36"
do
	scp -i /Users/ducky/Downloads/dhong-umass-us-east-2.pem ./build/libs/gigapaxos-1.0.08-all.jar ubuntu@$server:~
	# scp -i /Users/ducky/Downloads/dhong-umass-us-east-2.pem ./conf/gigapaxos.properties ubuntu@$server:~
done
