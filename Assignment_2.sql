--q1
CREATE TABLE query1 AS (
	SELECT name, COUNT(*) AS moviecount
	FROM genres, hasagenre
	WHERE genres.genreid=hasagenre.genreid
	GROUP BY name
);

--q2
CREATE TABLE query2 AS (
	SELECT name, AVG(rating) AS rating
	FROM ratings, genres, hasagenre
	WHERE genres.genreid=hasagenre.genreid AND hasagenre.movieid=ratings.movieid
	GROUP BY name
);

--q3
CREATE TABLE query3 AS (
	SELECT title, COUNT(rating) AS countofratings
	FROM movies, ratings
	WHERE movies.movieid=ratings.movieid
	GROUP BY title
	HAVING COUNT(rating)>=10
);

--q4
CREATE TABLE query4 AS (
	SELECT movies.movieid, title
	FROM movies, hasagenre, genres
	WHERE movies.movieid=hasagenre.movieid AND hasagenre.genreid=genres.genreid AND name='Comedy'
);

--q5
CREATE TABLE query5 AS (
	SELECT title, AVG(rating) AS average
	FROM movies, ratings
	WHERE movies.movieid=ratings.movieid
	GROUP BY movies.movieid
);

--q6
CREATE TABLE query6 AS (
	SELECT AVG(rating) AS average
	FROM ratings, hasagenre, genres
	WHERE ratings.movieid=hasagenre.movieid AND hasagenre.genreid=genres.genreid AND name='Comedy'
);

--q7
CREATE TABLE query7 AS (
	SELECT AVG(rating) AS average
	FROM ratings, hasagenre, genres
	WHERE ratings.movieid=hasagenre.movieid AND hasagenre.genreid=genres.genreid AND name='Comedy' AND hasagenre.movieid IN (
		SELECT hasagenre.movieid
		FROM hasagenre, genres
		WHERE hasagenre.genreid=genres.genreid AND name='Romance'
	)
);

--q8
CREATE TABLE query8 AS (
	SELECT AVG(rating) AS average
	FROM ratings, hasagenre, genres
	WHERE ratings.movieid=hasagenre.movieid AND hasagenre.genreid=genres.genreid AND name='Romance' AND hasagenre.movieid NOT IN (
		SELECT hasagenre.movieid
		FROM hasagenre, genres
		WHERE hasagenre.genreid=genres.genreid AND name='Comedy'
	)
);

--q9
CREATE TABLE query9 AS (
	SELECT movieid, rating
	FROM ratings
	WHERE userid=:v1
);
	
--q10

-- create a table of each movie and its average rating
CREATE TABLE query10 AS (
	SELECT movieid, AVG(rating) AS avgrating
	FROM ratings
	GROUP BY movieid
);

-- create a similarity table between each two movies in the DB based on their average rating
CREATE VIEW similarity AS (
	SELECT m1.movieid AS movieid1, m2.movieid AS movieid2, (1-(abs(m1.avgrating - m2.avgrating)/5)) AS sim
	FROM query10 m1, query10 m2
	WHERE m1.movieid!=m2.movieid
);

-- create a table of all movies rated by user Ua
CREATE TABLE user_ua AS (
	SELECT movieid, rating
	FROM ratings
	WHERE userid=:v1
);

-- create a view of all movies that aren't rated by user Ua and their predicted rating
CREATE VIEW prediction AS (
	SELECT s.movieid1 AS candidate, 
	CASE SUM(sim) WHEN 0.0 THEN 0.0
		ELSE SUM(sim*u.rating)/SUM(sim)
		END
	AS predictedrating
	FROM similarity s, user_ua u
	WHERE s.movieid2=u.movieid AND s.movieid1 NOT IN (
		SELECT movieid FROM user_ua
	)
	GROUP BY s.movieid1
);

-- recommend a movie from the DB if its predicted rating is above 3.9
CREATE TABLE recommendation AS(
	SELECT title
	FROM movies, prediction
	WHERE movies.movieid=prediction.candidate AND prediction.predictedrating>3.9
);