#!/usr/bin/env bash

hdfs dfs -rm -r /user/spark-output1
hdfs dfs -rm -r /user/spark-output2
hdfs dfs -rm -r /user/spark-output3

time spark-submit --master spark://$(hostname):7077 --executor-memory 14500M --driver-memory 14500M --packages com.databricks:spark-csv_2.10:1.1.0 reddit_data.py
