//good, attention pour égalité du rank on se retrouve avec plus que 5 valeurs

CREATE VIEW IF NOT EXISTS join_distributor_genre AS
SELECT distributor.year_release, region,media, genre, count(distributor.movie_title) as number_movies 
FROM distributor_partition_table as distributor 
INNER join genre_table as genres
on (genres.movie_title=distributor.movie_title and (distributor.year_release >= 1970 and distributor.year_release <= 2010) and  (genres.year_release >= 1970 and genres.year_release <= 2010))
group by distributor.year_release, region,media,genre;

select year_release,region, media, genre, number_movies, (number_movies/total_movies)*100 from(
	select year_release, region, media, genre,number_movies, rank() over(PARTITION BY year_release, region, media ORDER BY number_movies desc) rank,sum(number_movies) OVER(PARTITION BY year_release, region, media) as total_movies 
from join_distributor_genre	
)complete_table where rank < 6
SORT BY year_release,region,media, number_movies desc
LIMIT 100;

//bucket version
CREATE VIEW IF NOT EXISTS join_distributor_genre_bucket AS
SELECT distributor.year_release, region,media, genre, count(distributor.movie_title) as number_movies 
FROM distributor_bucket_table as distributor 
INNER join genre_bucket_table as genres
on (genres.movie_title=distributor.movie_title and (distributor.year_release >= 1970 and distributor.year_release <= 2010) and  (genres.year_release >= 1970 and genres.year_release <= 2010))
group by distributor.year_release, region,media,genre;

select year_release,region, media, genre, number_movies, (number_movies/total_movies)*100 from(
        select year_release, region, media, genre,number_movies, rank() over(PARTITION BY year_release, region, media ORDER BY number_movies desc) rank,sum(number_movies) OVER(PARTITION BY year_release, region, media) as total_movies 
from join_distributor_genre_bucket
)complete_table where rank < 6
SORT BY year_release,region,media, number_movies desc
LIMIT 100;

-------------------------------

Production Company

-------------------------------
//
CREATE VIEW IF NOT EXISTS join_distributor_production AS
SELECT year_release, region,media, company_name, count(distributor.movie_title) AS number_movies 
FROM distributor_table AS distributor
INNER join production_table AS productions
ON (productions.movie_title=distributor.movie_title AND (distributor.year_release >= 1970 and distributor.year_release <= 2010))
group by year_release, region,media,company_name;

select year_release,region, media, company_name, number_movies, (number_movies/total_movies)*100 from(
	select year_release, region, media, company_name,number_movies, rank() over(PARTITION BY year_release, region, media ORDER BY number_movies desc) rank,sum(number_movies) OVER(PARTITION BY year_release, region, media) as total_movies
	from join_distributor_production 
)complete_table where rank < 6
SORT BY year_release,region,media, number_movies desc
LIMIT 100;

---------------------------------
Distribution
---------------------------------

select year_release,region, media, distributor, number_movies, (number_movies/total_movies)*100 from(
	select year_release, region, media,distributor ,number_movies, rank() over(PARTITION BY year_release, region ORDER BY number_movies desc) rank,sum(number_movies) OVER(PARTITION BY year_release, region) as total_movies from (
		select year_release, region,media, distributor, count(movie_title) as number_movies from
			distributor_table 
			where (year_release >= 1970 and year_release <= 1970) 
			group by year_release, region,media,distributor)tmp_table
)complete_table where rank < 6
SORT BY year_release,region, number_movies desc
LIMIT 100;

//bucket version
select year_release,region, media, distributor, number_movies, (number_movies/total_movies)*100 from(
	select year_release, region, media,distributor ,number_movies, rank() over(PARTITION BY year_release, region ORDER BY number_movies desc) rank,sum(number_movies) OVER(PARTITION BY year_release, region) as total_movies from (
		select year_release, region,media, distributor, count(movie_title) as number_movies from
			distributor_bucket_table 
			where (year_release >= 1970 and year_release <= 1970) 
			group by year_release, region,media,distributor)tmp_table
)complete_table where rank < 6
SORT BY year_release,region, number_movies desc
LIMIT 100;
