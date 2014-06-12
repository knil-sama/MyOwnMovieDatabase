create table IF NOT EXISTS genre_tmp(movie_title string,year_release int, genre string)
row format delimited fields terminated by '|'
stored as textfile;

LOAD DATA LOCAL INPATH '/home/knil/data_csv/genres.csv' OVERWRITE INTO TABLE genre_tmp;

create table IF NOT EXISTS genre_table(movie_title string, genre string)
partitioned by(year_release int)
row format delimited fields terminated by '|'                                  
stored as orc
tblproperties ("orc.compress"="SNAPPY");

INSERT OVERWRITE TABLE genre_table PARTITION(year_release) SELECT movie_title, genre, year_release FROM genre_tmp WHERE not(movie_title="" or year_release is null or genre=""); 

create table IF NOT EXISTS genre_bucket_table(movie_title string, genre string)
partitioned by(year_release int)
clustered by (movie_title) into 64 buckets
row format delimited fields terminated by '|'                                  
stored as orc
tblproperties ("orc.compress"="SNAPPY");

set hive.enforce.bucketing=true;
INSERT OVERWRITE TABLE genre_bucket_table PARTITION(year_release) SELECT movie_title, genre, year_release FROM genre_table WHERE not(movie_title="" or year_release is null or genre=""); 



