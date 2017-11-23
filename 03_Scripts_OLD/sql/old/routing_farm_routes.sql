
CREATE EXTENSION pgrouting;


DROP TABLE IF EXISTS farm_roads_edges_noded;

-- Farm roads (crop by ADM)
DROP TABLE IF EXISTS farm_roads_edges;
CREATE TABLE farm_roads_edges AS 
	SELECT * 
	FROM osm_roads 
	WHERE value = 'primary'
	OR value = 'secondary' 
	OR value = 'tertiary' ;

select * from farm_roads_edges limit 20;

ALTER TABLE farm_roads_edges ADD source INT4;
ALTER TABLE farm_roads_edges ADD target INT4;
select * from farm_roads_edges limit 20;

SELECT pgr_createTopology('farm_roads_edges', 0.000001, 'geom');


select * from farm_roads_edges limit 20;
SELECT pgr_analyzeGraph('farm_roads_edges', 0.000001, 'geom', 'id' );

SELECT pgr_nodeNetwork('farm_roads_edges', 0.000001, 'id', 'geom');
SELECT pgr_analyzeGraph('farm_roads_edges_noded', 0.000001, 'geom', 'id' );

SELECT pgr_createTopology('farm_roads_edges_noded', 0.01, 'geom');
select * from farm_roads_edges_noded limit 20;
SELECT pgr_analyzeGraph('farm_roads_edges_noded', 0.000001, 'geom', 'id' );

ALTER TABLE farm_roads_edges_noded
  ADD COLUMN name VARCHAR,
  ADD COLUMN type VARCHAR,
  ADD COLUMN oneway VARCHAR,
  ADD COLUMN surface VARCHAR;


UPDATE farm_roads_edges_noded AS new
SET
  name = old.name,
  type = old.value,
  oneway = old.oneway,
  surface = old.surface
FROM farm_roads_edges AS old
WHERE new.id = old.id;

SELECT DISTINCT(type) from farm_roads_edges_noded;


ALTER TABLE farm_roads_edges_noded ADD distance FLOAT8;
ALTER TABLE farm_roads_edges_noded ADD time FLOAT8;
UPDATE farm_roads_edges_noded SET distance = ST_Length(ST_Transform(geom, 4326)::geography) / 1000;

select * from farm_roads_edges_noded limit 20;

UPDATE farm_roads_edges_noded SET
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


  UPDATE farm_roads_edges_noded SET
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
FROM pgr_dijkstra('SELECT id, source::integer, target::integer, time::double precision AS cost FROM farm_roads_edges_noded', 1, 10, false)
ORDER BY seq;

--
DROP TABLE IF EXISTS farm_roads_final_route;
CREATE TABLE farm_roads_final_route AS 
SELECT
  e.id,
  e.name,
  e.type,
  e.oneway,
  e.time AS time,
  e.distance AS distance,
  e.geom AS geom
FROM
  pgr_dijkstra('SELECT id, source::integer, target::integer, time::double precision AS cost FROM farm_roads_edges_noded', 368, 369,true) AS r,
  farm_roads_edges_noded AS e
WHERE r.node = e.id;

--
DROP TABLE IF EXISTS farm_roads_final_route_group;
CREATE TABLE farm_roads_final_route_group AS 
SELECT
  e.id,
  e.name,
  e.type,
  e.oneway,
  sum(e.time) AS time,
  sum(e.distance) AS distance,
  ST_Collect(e.geom) AS geom
FROM
  pgr_dijkstra('SELECT id, source::integer, target::integer, time::double precision AS cost FROM farm_roads_edges_noded',368, 369, false) AS r,
  farm_roads_edges_noded AS e
WHERE r.seq = e.id
GROUP BY e.id, e.name, e.type, e.oneway, e.id
ORDER BY e.id;

SELECT pgr_analyzeGraph('final_route_gr', 0.000001, 'the_geom', 'id' );
