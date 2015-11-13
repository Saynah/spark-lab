#!/usr/bin/env bash

hdfs dfs -rm -r /spark-output

spark-submit --master spark://ip-172-31-4-108:7077 --executor-memory 6100M --driver-memory 6100M --packages com.databricks:spark-csv_2.10:1.1.0 reddit_data.py
