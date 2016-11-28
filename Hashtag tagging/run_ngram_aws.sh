#!/bin/bash

aws emr create-cluster --name "ngramcount uzair" --ami-version 2.4.11 --service-role EMR_DefaultRole --ec2-attributes InstanceProfile=EMR_EC2_DefaultRole --log-uri s3://uzair.log-uri.ngramcount --enable-debugging --instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=c1.medium InstanceGroupType=CORE,InstanceCount=4,InstanceType=c1.medium --steps Type=CUSTOM_JAR,Jar=s3://uzair.fastcode/18645-proj3-0.1-latest.jar,Args=["-input","s3://yantweets10m/tweets10m.txt","-output","s3://uzair.output/ngram100m","-program","ngramcount","-n","5"] --auto-terminate
