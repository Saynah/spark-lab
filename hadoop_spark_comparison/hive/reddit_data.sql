DROP TABLE reddit_data;

CREATE EXTERNAL TABLE IF NOT EXISTS reddit_data ( value STRING )
LOCATION 's3n://reddit-comments/2008/';

DESCRIBE reddit_data;


INSERT OVERWRITE DIRECTORY '/user/hive-output1'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ' '
LINES TERMINATED BY '\n'
SELECT b.subreddit, COUNT(DISTINCT b.author) AS author_cnt
FROM reddit_data a 
  LATERAL VIEW json_tuple(a.value, 'author', 'subreddit') b 
	AS author, subreddit 
GROUP BY b.subreddit 
ORDER BY author_cnt DESC;

INSERT OVERWRITE DIRECTORY '/user/hive-output2'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ' '
LINES TERMINATED BY '\n'
SELECT b.author, COUNT(DISTINCT b.subreddit) AS subreddit_cnt
FROM reddit_data a 
  LATERAL VIEW json_tuple(a.value, 'author', 'subreddit') b 
	AS author, subreddit 
GROUP BY b.author 
ORDER BY subreddit_cnt DESC;
