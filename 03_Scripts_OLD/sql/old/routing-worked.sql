
CREATE EXTENSION pgrouting;


DROP TABLE IF EXISTS farm_roads_edges_noded;
DROP TABLE IF EXISTS farm_roads_edges_noded_vertices_pgr;
DROP TABLE IF EXISTS farm_roads_edges_vertices_pgr;
DROP TABLE IF EXISTS farm_roads_final_route;



-- Farm roads (crop by ADM)
DROP TABLE IF EXISTS farm_roads_edges;
CREATE TABLE farm_roads_edges AS 
	SELECT * 
	FROM osm_roads 
	WHERE value = 'primary' ;

ALTER TABLE farm_roads_edges DROP COLUMN IF EXISTS id;
ALTER TABLE farm_roads_edges ADD COLUMN id SERIAL PRIMARY KEY;


select * from farm_roads_edges limit 20;

ALTER TABLE farm_roads_edges ADD source INT4;
ALTER TABLE farm_roads_edges ADD target INT4;
select * from farm_roads_edges limit 20;

SELECT pgr_createTopology('farm_roads_edges', 1, 'geom');
select * from farm_roads_edges limit 20;
SELECT pgr_analyzeGraph('farm_roads_edges', 1, 'geom', 'id' );

SELECT pgr_nodeNetwork('farm_roads_edges', 1, 'id', 'geom');
SELECT pgr_createTopology('farm_roads_edges_noded', 1, 'geom');

SELECT pgr_analyzeGraph('farm_roads_edges_noded', 1, 'geom', 'id' );
select * from farm_roads_edges_noded limit 20;



ALTER TABLE farm_roads_edges_noded ADD distance FLOAT8;
ALTER TABLE farm_roads_edges_noded ADD time FLOAT8;
UPDATE farm_roads_edges_noded SET distance = ST_Length(ST_Transform(geom, 4326)::geography) / 1000;

select * from farm_roads_edges_noded limit 20;


--
SELECT  *
FROM pgr_dijkstra('SELECT id, source::integer, target::integer, distance::double precision AS cost FROM farm_roads_edges_noded', 1, 10, false)
ORDER BY seq;

--
DROP TABLE IF EXISTS farm_roads_final_route;
CREATE TABLE farm_roads_final_route AS 
SELECT
  b.id,
  b.distance AS distance,
  b.geom AS geom
FROM
  pgr_dijkstra('SELECT id, source::integer, target::integer, distance::double precision AS cost FROM farm_roads_edges_noded', 941, 1,true) AS a,
  farm_roads_edges_noded AS b
WHERE a.edge = b.id;

select * from farm_roads_final_route;


--
DROP TABLE IF EXISTS farm_roads_final_route;
CREATE TABLE farm_roads_final_route AS 
SELECT dijkstra.*, farm_roads_edges_noded.geom
FROM pgr_dijkstra('SELECT id, source::integer, target::integer, distance::double precision AS cost FROM farm_roads_edges_noded', 1, 10, false) AS dijkstra
LEFT JOIN farm_roads_edges_noded
ON (edge = id) ORDER BY seq;


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
