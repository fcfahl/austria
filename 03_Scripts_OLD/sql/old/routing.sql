create database  routing;

CREATE EXTENSION postgis;
CREATE EXTENSION hstore;
CREATE EXTENSION pgrouting;


-- Farm roads (crop by ADM)
DROP TABLE IF EXISTS edges2;
CREATE TABLE edges2 AS 
	SELECT * 
	FROM edges 
	WHERE highway = 'primary'
	OR highway = 'secondary' 
	OR highway = 'motorway' ;

ALTER TABLE edges2 ADD source INT4;
ALTER TABLE edges2 ADD target INT4;
SELECT pgr_createTopology('edges2', 1);

SELECT pgr_nodeNetwork('edges2', 1);

SELECT pgr_createTopology('edges2_noded', 1);


ALTER TABLE edges2_noded
  ADD COLUMN name VARCHAR,
  ADD COLUMN type VARCHAR,
  ADD COLUMN oneway VARCHAR,
  ADD COLUMN surface VARCHAR;


UPDATE edges2_noded AS new
SET
  name = CASE WHEN old.name IS NULL THEN old.ref ELSE old.name END,
  type = old.highway,
  oneway = old.oneway,
  surface = old.surface
FROM edges2 AS old
WHERE new.old_id = old.id;

SELECT DISTINCT(type) from edges2_noded;


ALTER TABLE edges2_noded ADD distance FLOAT8;
ALTER TABLE edges2_noded ADD time FLOAT8;
UPDATE edges2_noded SET distance = ST_Length(ST_Transform(the_geom, 4326)::geography) / 1000;


UPDATE edges2_noded SET
  time =
  CASE type
    WHEN 'steps' THEN -1
    WHEN 'path' THEN -1
    WHEN 'footway' THEN -1
    WHEN 'cycleway' THEN -1
    WHEN 'proposed' THEN -1
    WHEN 'construction' THEN -1
    WHEN 'raceway' THEN distance / 100
    WHEN 'motorway' THEN distance / 70
    WHEN 'motorway_link' THEN distance / 70
    WHEN 'trunk' THEN distance / 60
    WHEN 'trunk_link' THEN distance / 60
    WHEN 'primary' THEN distance / 55
    WHEN 'primary_link' THEN distance / 55
    WHEN 'secondary' THEN distance / 45
    WHEN 'secondary_link' THEN distance / 45
    WHEN 'tertiary' THEN distance / 45
    WHEN 'tertiary_link' THEN distance / 40
    WHEN 'unclassified' THEN distance / 35
    WHEN 'residential' THEN distance / 30
    WHEN 'living_street' THEN distance / 30
    WHEN 'service' THEN distance / 30
    WHEN 'track' THEN distance / 20
    ELSE distance / 20
  END;


  UPDATE edges2_noded SET
  distance =
  CASE type
    WHEN 'steps' THEN -1
    WHEN 'path' THEN -1
    WHEN 'footway' THEN -1
    WHEN 'cycleway' THEN -1
    WHEN 'proposed' THEN -1
    WHEN 'construction' THEN -1
    ELSE distance
  END;

--
SELECT  *
FROM pgr_dijkstra('SELECT id, source::integer, target::integer, time::double precision AS cost FROM edges2_noded', 1, 10, false)
ORDER BY seq;

--
DROP TABLE IF EXISTS final_route;
CREATE TABLE final_route AS 
SELECT
  e.old_id,
  e.name,
  e.type,
  e.oneway,
  e.time AS time,
  e.distance AS distance,
  e.the_geom AS geom
FROM
  pgr_dijkstra('SELECT id, source::integer, target::integer, time::double precision AS cost FROM edges2_noded', 1, 10,false) AS r,
  edges2_noded AS e
WHERE r.node = e.id;

--
DROP TABLE IF EXISTS final_route_gr;
CREATE TABLE final_route_gr AS 
SELECT
  e.id,
  e.old_id,
  e.name,
  e.type,
  e.oneway,
  sum(e.time) AS time,
  sum(e.distance) AS distance,
  ST_Collect(e.the_geom) AS geom
FROM
  pgr_dijkstra('SELECT id, source::integer, target::integer, time::double precision AS cost FROM edges2_noded', 1, 10, false) AS r,
  edges2_noded AS e
WHERE r.seq = e.id
GROUP BY e.old_id, e.name, e.type, e.oneway, e.id
ORDER BY e.old_id;

SELECT pgr_analyzeGraph('final_route_gr', 0.000001, 'the_geom', 'id' );
