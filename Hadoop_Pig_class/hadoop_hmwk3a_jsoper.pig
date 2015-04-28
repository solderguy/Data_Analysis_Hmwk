/*
*       Hadoop class homework 3a, Winter 2013
*       John Soper
*/
-- Command Line Run:  pig -x local hadoop_hmwk3a_jsoper.pig


movies = load './data/movie/imdb.tsv' using PigStorage()
	as (actor:chararray, movie:chararray, year:int);

--dump movies;

movies = foreach movies generate actor,movie;
--dump movies;


movies_group = group movies by actor;
--dump movies_group;


movies_count = foreach movies_group generate COUNT(movies) as count, group as actor;
--dump movies_count;


movies_sorted = order movies_count by count DESC;
--dump movies_sorted;



--RMF /tmp/hadoop/pig/movie/ative-actors;

store movies_sorted into './hadoop_hmwk3a_jsoper_results' using PigStorage();

