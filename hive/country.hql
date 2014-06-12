create table IF NOT EXISTS country_tmp(movie_title string, episode string, year_release int, region string) 
row format delimited fields terminated by '|'
stored as textfile;

LOAD DATA LOCAL INPATH '/home/knil/data_csv/countries.csv' OVERWRITE INTO TABLE country_tmp;

create table IF NOT EXISTS country_table(movie_title string, episode string)
partitioned by (year_release int, region string) 
row format delimited fields terminated by '|'  
stored as orc
tblproperties ("orc.compress"="SNAPPY");
	
insert into table country_table partition(year_release, region) SELECT movie_title, episode, year_release, region
from country_tmp where region IN ("USA", "France", "Japan")
and movie_title!='' and not year_release is null and region!='';

insert into table country_table
partition(year_release,region) 
select movie_title, episode, year_release, "World" 
from country_tmp where region NOT IN ("USA", "France", "Japan")
and movie_title!='' and not year_release is null and region!='';

c
