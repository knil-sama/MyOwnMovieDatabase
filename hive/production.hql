create table IF NOT EXISTS production_tmp(
movie_title string, 
year_production int, 
serie string, 
company_name string, 
country_code string)
row format delimited fields terminated by '|'                                  
stored as textfile;

LOAD DATA LOCAL INPATH '/home/knil/data_csv/production-companies.csv' OVERWRITE INTO TABLE production_tmp;

create table IF NOT EXISTS production_table(
movie_title string,
year_production int,
serie string,
company_name string,
country_code string)
stored as orc
tblproperties ("orc.compress"="SNAPPY");

insert into table production_table 
select movie_title, year_production, serie, company_name, country_code  
from production_tmp where 
movie_title!='' and company_name!='';


