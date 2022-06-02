CREATE TABLE topic_offsets(id serial primary key, topic_name varchar(50) NOT NULL UNIQUE, offsets varchar(250) NOT NULL);
CREATE TABLE shops(id serial primary key, name varchar(50) NOT NULL);
INSERT INTO shops(name) values ('shop#1'), ('shop#2'), ('shop#3');