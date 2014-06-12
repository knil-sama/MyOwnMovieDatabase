//acteurs
select year_release, region, media, avg(duration_career) from

select director_table.year_release, region, name, actor_table.name as nameActor from
	actorAndActress_table  
	JOIN director_table
	ON(actorAndActress.movie_title=director_table.movie_title and director_table.year_release >= 1970 and director_table.year_release <= 1980) 
	join distributor_table 
	ON(distributor_table.movie_title=director_table.movie_title and distributor_table.year_release >= 1970 and distributor_table.year_release <= 1980) 
limit 100;

//actrice

//director
