REGISTER 'string_manip.py' using jython as str_func;

reddit_data = LOAD 's3n://reddit-comments/2008/' 
	      USING JsonLoader(); 

DESCRIBE reddit_data;
/*
price_modified = FOREACH price GENERATE str_func.conv_to_30min(time) AS time, 
					price AS price, 
					volume AS volume;

grpd = GROUP price_modified BY (time);

compressed = FOREACH grpd GENERATE group AS time, 
				   AVG(price_modified.price), 
				   SUM(price_modified.volume);

STORE compressed INTO 'price_data_full_pig';
*/
