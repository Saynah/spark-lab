#!/usr/bin/env bash

hdfs dfs -rm -r /spark-output

time spark-submit --master spark://$(hostname):7077 --executor-memory 6100M --driver-memory 6100M --packages com.databricks:spark-csv_2.10:1.1.0 reddit_data.py
