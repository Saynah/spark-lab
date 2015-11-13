#!/usr/bin/env bash


time hive -f reddit_data.sql --hiveconf fs.s3n.awsAccessKeyId=$AWS_ACCESS_KEY_ID --hiveconf fs.s3n.awsSecretAccessKey=$AWS_SECRET_ACCESS_KEY --hiveconf mapreduce.application.classpath=$HADOOP_HOME/share/hadoop/mapreduce/*:$HADOOP_HOME/share/hadoop/mapreduce/lib/*:$HADOOP_HOME/share/hadoop/tools/lib/* --hiveconf fs.s3n.impl=org.apache.hadoop.fs.s3native.NativeS3FileSystem
