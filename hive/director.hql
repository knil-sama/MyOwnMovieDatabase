create table IF NOT EXISTS director_tmp(
	name string,
	movie_title string,
	year_release int,
	extra string,
	role string
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '|'
stored as textfile;

LOAD DATA LOCAL INPATH '/home/knil/data_csv/directors.csv' OVERWRITE INTO TABLE director_tmp;

create table IF NOT EXISTS director_bucket_table(
	name string,
	movie_title string,
	extra string,
	role string
)
PARTITIONED BY (year_release int)
clustered by (movie_title) into 64 buckets
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
stored as orc tblproperties("orc.compress"="SNAPPY");

set hive.enforce.bucketing=true;
INSERT OVERWRITE TABLE director_bucket_table PARTITION(year_release) SELECT name,movie_title, extra,role, year_release FROM director_table;
