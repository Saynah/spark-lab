from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *

sc = SparkContext()
sqlContext = SQLContext(sc)

fields = [StructField("archived", BooleanType(), True),
	  StructField("author", StringType(), True),
          StructField("author_flair_css_class", StringType(), True),
          StructField("body", StringType(), True),
          StructField("controversiality", LongType(), True),
          StructField("created_utc", StringType(), True),
          StructField("day", LongType(), True),
          StructField("distinguished", StringType(), True),
          StructField("downs", LongType(), True),
          StructField("edited", StringType(), True),
          StructField("gilded", LongType(), True),
          StructField("id", StringType(), True),
          StructField("link_id", StringType(), True),
          StructField("month", LongType(), True),
          StructField("name", StringType(), True),
          StructField("parent_id", StringType(), True),
          StructField("retrieved_on", LongType(), True),
          StructField("score", LongType(), True),
          StructField("score_hidden", BooleanType(), True),
          StructField("subreddit", StringType(), True),
          StructField("subreddit_id", StringType(), True),
          StructField("ups", LongType(), True),
          StructField("year", LongType(), True)]
rawDF = sqlContext.read.json("s3n://reddit-comments/2007", StructType(fields))

rawDF.registerTempTable("rc")

unique_authors_per_subreddit_df = sqlContext.sql("""
    SELECT subreddit, COUNT(DISTINCT author) as author_cnt
    FROM rc
    GROUP BY subreddit
    ORDER BY author_cnt DESC
    """)

unique_authors_per_subreddit_df.rdd.saveAsTextFile("hdfs://52.33.233.229:9000/spark-output")
