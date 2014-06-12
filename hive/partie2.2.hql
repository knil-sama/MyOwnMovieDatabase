CREATE VIEW actorAndActress AS 
SELECT name,movie_title 
FROM actor_bucket_table 
WHERE (year_release >= 1970 and year_release <= 2010)
UNION ALL
SELECT name,movie_title 
FROM actress_bucket_table
WHERE (year_release >= 1970 and year_release <= 2010);

CREATE VIEW join_distributor_director AS
SELECT distributors.year_release, region,media, director.name directorName,director.movie_title 
FROM distributor_bucket_table as distributors 
JOIN director_bucket_table AS director
ON (director.movie_title=distributors.movie_title AND (distributors.year_release >= 1970 and distributors.year_release <= 2010) AND  (director.year_release >= 1970 AND director.year_release <= 2010));

//resultat

SELECT year_release,region, media, directorName, number_movies, (number_movies/total_movies)*100 
FROM(
	SELECT year_release, region, media, directorName,number_movies, rank() over(PARTITION BY year_release, region, media ORDER BY number_movies desc) rank,sum(number_movies) OVER(PARTITION BY year_release, region, media) as total_movies
	FROM(
		SELECT join_distributor_director.year_release, region,media, directorName, count(join_distributor_director.movie_title) as number_movies 
		FROM actor_bucket_table	actor	
		INNER JOIN join_distributor_director		
		ON (actor.movie_title=join_distributor_director.movie_title and actor.year_release>=1970 and actor.year_release <= 2010)
		group by join_distributor_director.year_release, region,media,directorName
		UNION ALL
		SELECT join_distributor_director.year_release, region,media, directorName, count(join_distributor_director.movie_title) as number_movies 
		FROM actress_bucket_table actress
		INNER JOIN join_distributor_director		
		ON (actress.movie_title=join_distributor_director.movie_title and actress.year_release>=1970 and actress.year_release >= 2010)
		group by join_distributor_director.year_release, region,media,directorName
	)joined_table
)complete_table where rank < 6
SORT BY year_release,region,media, number_movies desc LIMIT 100;

create table IF NOT EXISTS actorAndActress_table(
movie_title string,
name string)
row format delimited fields terminated by '|'                                  
stored as orc
tblproperties ("orc.compress"="SNAPPY");

INSERT OVERWRITE table actorAndActress_table SELECT movie_title, name
FROM actor_table where year_release >= 1970 and year_release<=1970;
INSERT OVERWRITE table actorAndActress_table SELECT movie_title, name
FROM actor_table where year_release >= 1970 and year_release<=1970;

create table IF NOT EXISTS directorAndDistributor_table(
media string,
name_director string,
movie_title string)
PARTITIONED BY (year_release int,
region string)
row format delimited fields terminated by '|'                                  
stored as orc
tblproperties ("orc.compress"="SNAPPY");

select year_release,region, media, name_director, number_movies, (number_movies/total_movies)*100 from(
        select year_release, region, media, name_director, name,number_movies, rank() over(PARTITION BY year_release, region, media,name_director,name ORDER BY number_movies desc) rank,sum(number_movies) OVER(PARTITION BY year_release, region, media) as total_movies 
from(select year_release, region, media, name_director, name,count(aaa.movie_title) number_movies from actorAndActress_table aaa 
join directorAndDistributor_table dad 
on aaa.movie_title=dad.movie_title 
group by year_release, region, media, name_director, name
)joined_table)complete_table where rank < 6
SORT BY year_release,region,media, number_movies desc LIMIT 100;
