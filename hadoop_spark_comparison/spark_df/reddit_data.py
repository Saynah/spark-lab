from pyspark import SparkContext, StorageLevel
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
rawDF = sqlContext.read.json("s3n://reddit-comments/2008", StructType(fields)).persist(StorageLevel.MEMORY_AND_DISK_SER)

rawDF.registerTempTable("rc")

unique_authors_per_subreddit_df = sqlContext.sql("""
    SELECT subreddit, COUNT(DISTINCT author) as author_cnt
    FROM rc
    GROUP BY subreddit
    ORDER BY author_cnt DESC, subreddit
    """)

#gilded_comments_per_subreddit_df = sqlContext.sql("""
#    SELECT subreddit, COUNT(gilded) as gilded_cnt
#    FROM rc
#    WHERE gilded > 0
#    GROUP BY subreddit
#    ORDER BY gilded_cnt DESC, subreddit
#    """)

#unique_subreddits_per_author_df = sqlContext.sql("""
#    SELECT author, COUNT(DISTINCT subreddit) as subreddit_cnt
#    FROM rc
#    GROUP BY author
#    ORDER BY subreddit_cnt DESC, author
#    """)

unique_authors_per_subreddit_df.rdd.coalesce(1).saveAsTextFile("/user/spark-output1")
#gilded_comments_per_subreddit_df.rdd.coalesce(1).saveAsTextFile("/user/spark-output2")
#unique_subreddits_per_author_df.rdd.coalesce(1).saveAsTextFile("/user/spark-output3")

