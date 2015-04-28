/*
* 	Hadoop class homework 3b, Winter 2013
*	John Soper
*/
-- Command Line Run: pig -x local hadoop_hmwk3b_jsoper.pig

weights_raw = load './data/movie-weights/imdb-weights.tsv' using
	 PigStorage('\t') AS (movie:chararray, year:int, rank:float);

weights_grouped = GROUP weights_raw BY year;

weights_cleaned = FOREACH weights_grouped  {
	aa = ORDER weights_raw BY rank DESC;
	aaa = LIMIT aa 1;
 	GENERATE FLATTEN(aaa.movie) AS movie, FLATTEN(aaa.rank) AS rank,
		 FLATTEN(aaa.year) AS year;  
} 

movies_raw = LOAD './data/movie/imdb.tsv' USING
	 PigStorage('\t') AS (actor:chararray, movie:chararray, year:int); 

movies_grouped = GROUP movies_raw BY (movie, year);

movies_cleaned = FOREACH movies_grouped GENERATE group.movie AS movie,
  	group.year AS year, movies_raw.actor AS actor;

movies_weights_join = JOIN weights_cleaned BY (movie, year) LEFT OUTER, movies_cleaned BY (movie, year);

movies_weights_actor = FOREACH movies_weights_join GENERATE
			weights_cleaned::movie, 
			weights_cleaned::year AS year,
			weights_cleaned::rank,
			movies_cleaned::actor;

movies_weights_actor_ordered = ORDER movies_weights_actor by year;

--DUMP movies_weights_actor_ordered;  
-- TODO RMF support
store movies_weights_actor_ordered into './hadoop_hmwk3b_jsoper_results' using PigStorage();
