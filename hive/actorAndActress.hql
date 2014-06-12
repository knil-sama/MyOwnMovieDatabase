CREATE TABLE IF NOT EXISTS actor_tmp(
	name STRING,
	movie_title STRING,
	year_release INT,
	role STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '/home/knil/data_csv/actors.csv' OVERWRITE INTO TABLE actor_tmp;

CREATE TABLE IF NOT EXISTS actor_table(
	name STRING,
	movie_title STRING,
	role STRING)
PARTITIONED BY (year_release INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
stored as orc
tblproperties ("orc.compress"="SNAPPY");

INSERT OVERWRITE TABLE actor_table PARTITION(year_release) SELECT name, movie_title, role, year_release FROM actor_tmp WHERE not(movie_title="" or year_release is null);

CREATE TABLE IF NOT EXISTS actor_bucket_table(
	name STRING,
	movie_title STRING,
	role STRING)
PARTITIONED BY (year_release INT)
clustered by (movie_title) into 64 buckets
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
stored as orc
tblproperties ("orc.compress"="SNAPPY");

set hive.enforce.bucketing=true;
INSERT OVERWRITE TABLE actor_bucket_table PARTITION(year_release) SELECT name, movie_title, role, year_release FROM actor_table WHERE not(movie_title="" or year_release is null);

CREATE TABLE IF NOT EXISTS actress_tmp(
	name STRING,
	movie_title STRING,
	year_release INT,
	role STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '/home/knil/data_csv/actresses.csv' OVERWRITE INTO TABLE actress_tmp;

CREATE TABLE IF NOT EXISTS actress_table(
	name STRING,
	movie_title STRING,
	role STRING)
PARTITIONED BY (year_release int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
stored as orc
tblproperties ("orc.compress"="SNAPPY");

INSERT OVERWRITE TABLE actress_table PARTITION(year_release) SELECT name,movie_title, role, year_release FROM actress_tmp WHERE not(movie_title="" or year_release is null);

CREATE TABLE IF NOT EXISTS actress_bucket_table(
	name STRING,
	movie_title STRING,
	role STRING)
PARTITIONED BY (year_release INT)
clustered by (movie_title) into 64 buckets
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
stored as orc
tblproperties ("orc.compress"="SNAPPY");


set hive.enforce.bucketing=true;
INSERT OVERWRITE TABLE actress_bucket_table PARTITION(year_release) SELECT name, movie_title, role, year_release FROM actress_table;

