DROP TABLE reddit_data;

CREATE EXTERNAL TABLE IF NOT EXISTS reddit_data ( value STRING )
LOCATION 's3n://reddit-comments/2007/';

DESCRIBE reddit_data;


INSERT OVERWRITE DIRECTORY '/user/hive-output'
SELECT *
FROM reddit_data;
