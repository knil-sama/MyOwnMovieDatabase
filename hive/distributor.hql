create table IF NOT EXISTS distributor_tmp(
movie_title string, 
year_production int, 
serie string, 
distributor string, 
country_code string, 
year_release int, 
region string, 
media string)
row format delimited fields terminated by '|'                                  
stored as textfile;

LOAD DATA LOCAL INPATH '/home/knil/data_csv/distributors.csv' OVERWRITE INTO TABLE distributor_tmp;

create table IF NOT EXISTS distributor_table(
movie_title string,
year_release int,
region string, 
year_production int, 
serie string, 
distributor string, 
country_code string, 
media string
)
row format delimited fields terminated by '|'                                  
stored as orc
tblproperties ("orc.compress"="SNAPPY");

INSERT OVERWRITE table distributor_table SELECT movie_title,year_release, region,year_production, serie, distributor, country_code, media
FROM distributor_tmp where region IN ("USA", "France", "Japan") and
movie_title!="" and distributor !="" and not year_release is null and region!="" and media!="";

INSERT INTO table distributor_table SELECT movie_title,year_release, "World",year_production, serie, distributor, country_code, media
FROM distributor_tmp where region NOT IN ("USA", "France", "Japan") and
movie_title!="" and distributor !="" and not year_release is null and region!="" and media!="";

create table IF NOT EXISTS distributor_partition_table(
movie_title string, 
year_production int, 
serie string, 
distributor string, 
country_code string, 
media string
)
partitioned by (year_release int,region string)
row format delimited fields terminated by '|'                                  
stored as orc
tblproperties ("orc.compress"="SNAPPY");

INSERT OVERWRITE table distributor_partition_table PARTITION(year_release, region) SELECT movie_title,year_production, serie, distributor, country_code, media, year_release,region
FROM distributor_table where movie_title!="" and distributor !="" and not year_release is null and region!="" and media!=""; 

create table IF NOT EXISTS distributor_bucket_table(
movie_title string, 
year_production int, 
serie string, 
distributor string, 
country_code string, 
media string
)
partitioned by (year_release int,region string)
clustered by (movie_title) into 64 buckets
row format delimited fields terminated by '|'                                  
stored as orc
tblproperties ("orc.compress"="SNAPPY");

set hive.enforce.bucketing=true;
INSERT OVERWRITE table distributor_bucket_table PARTITION(year_release, region) SELECT movie_title,year_production, serie, distributor, country_code, media, year_release,region
FROM distributor_partition_table;
