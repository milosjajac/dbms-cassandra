CREATE KEYSPACE demoks WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };

USE demoks;

CREATE TABLE tweets1 (
    user text,
    year int,
    month int,
    day int,
    uuid text,
    tags set<text>,
    content text,
    PRIMARY KEY(user, year, month, day, uuid)
);

CREATE TABLE tweets2 (
    user text,
    year int,
    month int,
    day int,
    uuid text,
    tags set<text>,
    content text,
    PRIMARY KEY(user, month, year, day, uuid)
);

CREATE INDEX uuid_index ON tweets2(uuid);
CREATE INDEX tags_index ON tweets2(tags);

CREATE TABLE tweets3 (
    user text,
    year int,
    month int,
    day int,
    uuid text,
    tags set<text>,
    content text,
    PRIMARY KEY(user, month, year, day, uuid)
);

CREATE INDEX tags_index1 ON tweets3(tags);

CREATE TABLE tweets3_by_uuid (
    uuid text,
    user text,
    year int,
    month int,
    day int,
    tags set<text>,
    content text,
    PRIMARY KEY(uuid)
);
