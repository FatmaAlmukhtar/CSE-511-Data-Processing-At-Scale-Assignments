CREATE TABLE movies (
	movieid INT PRIMARY KEY, 
	title TEXT NOT NULL
);

CREATE TABLE genres (
	genreid INT PRIMARY KEY,
	name TEXT NOT NULL UNIQUE
);

CREATE TABLE hasagenre (
	movieid INT,
	genreid INT,
	PRIMARY KEY (movieid, genreid),
	FOREIGN KEY (movieid) REFERENCES movies,
	FOREIGN KEY (genreid) REFERENCES genres
);

CREATE TABLE users (
	userid INT PRIMARY KEY,
	name TEXT NOT NULL
);

CREATE TABLE ratings (
	userid INT,
	movieid INT,
	rating NUMERIC NOT NULL CHECK (rating >=0 and rating<=5),
	timestamp BIGINT NOT NULL,
	PRIMARY KEY (userid, movieid),
	FOREIGN KEY (movieid) REFERENCES movies,
	FOREIGN KEY (userid) REFERENCES users
);

CREATE TABLE taginfo (
	tagid INT PRIMARY KEY,
	content TEXT NOT NULL
);

CREATE TABLE tags (
	userid INT,
	movieid INT,
	tagid INT,
	timestamp BIGINT NOT NULL,
	PRIMARY KEY (movieid, userid, tagid),
	FOREIGN KEY (userid) REFERENCES users,
	FOREIGN KEY (movieid) REFERENCES movies,
	FOREIGN KEY (tagid) REFERENCES taginfo(tagid)
);






