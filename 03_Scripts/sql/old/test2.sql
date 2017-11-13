----
DROP TABLE IF EXISTS farm_roads_test;
CREATE TABLE farm_roads_test AS 
	SELECT a.index, a.id_mun, a.id, a.total_area, a.total_lsu, a.total_manure, a.total_methane,
		ST_Intersection(ST_Buffer(a.geom, 5000), b.geom) AS geom		
	FROM farm_snap AS a, farm_roads AS b
	WHERE a.index = '30531_62'
	;

CREATE INDEX idx ON farm_roads_test USING GIST(geom);

SELECT *, ST_GeometryType(geom) FROM farm_roads_test;

CREATE INDEX idx_mytable_the_ON mytable USING GIST(the_geom);

DROP TABLE IF EXISTS farm_roads_test2;
CREATE TABLE farm_roads_test2 AS 
	SELECT 0 as id_road, (ST_Dump(geom)).geom as geom		
	FROM farm_roads_test
	;

ALTER SEQUENCE serial RESTART WITH 1;
UPDATE farm_roads_test2 
SET id_road = nextval('serial');

SELECT *, ST_GeometryType(geom) FROM farm_roads_test2 LIMIT 10;



DROP TABLE IF EXISTS farm_roads_test3;

SELECT id_road, (ST_Dump(ST_Union(geom))).geom
INTO farm_roads_test3
FROM farm_roads_test2
GROUP BY id_road
; 





DROP TABLE IF EXISTS farm_roads_test3;
CREATE TABLE farm_roads_test3 (id serial);

SELECT AddGeometryColumn('farm_roads_test3','geom',3035,'LINESTRING',2);

INSERT INTO farm_roads_test3 (geom) 
SELECT geom FROM farm_roads_test2;

DROP TABLE IF EXISTS farm_roads_test3;
CREATE TABLE farm_roads_test3 AS 
    SELECT (ST_Dump(ST_Union(ST_SnapToGrid(geom,0.09)))).geom  AS geom FROM farm_roads_buffer_topo.edge;

SELECT *, ST_GeometryType(geom) FROM farm_roads_test3 LIMIT 10;

DROP TABLE IF EXISTS farm_roads_test_split;
CREATE TABLE farm_roads_test_split AS 
	SELECT a.index, a.id_mun, a.id, a.total_area, a.total_lsu, a.total_manure, a.total_methane,
		ST_Intersection (ST_Buffer(a.geom, 5000), b.geom) AS geom		
	FROM farm_snap AS a, test_qgis_dissolve AS b
	WHERE a.index = '30531_62';

	

SELECT *, ST_GeometryType(geom) FROM farm_roads_test_split LIMIT 10;


DROP TABLE IF EXISTS farm_roads_test_split;
CREATE TABLE farm_roads_test_split AS(
SELECT
    (ST_Dump(ST_Split(ST_Snap(a.geom, b.geom, 0.00001),b.geom))).geom
FROM 
    test_qgis_dissolve a
JOIN 
    farm_snap b 
ON 
    ST_DWithin(b.geom, a.geom, 0.001)
)
;

SELECT *, ST_GeometryType(geom) FROM farm_roads_test_split LIMIT 10;


----

DROP TABLE IF EXISTS farm_roads_union;
CREATE TABLE farm_roads_union AS 

WITH RECURSIVE segments (id_road) AS (
	SELECT id_road
	UNION ALL
	SELECT *
	FROM farm_roads

)

SELECT *
FROM segments


CREATE  TABLE test_lines (id serial, geom geometry);

INSERT INTO test_lines (geom)
VALUES
('LINESTRING (0 0, 1 1)'),
('LINESTRING (2 2, 1 1)'),
('LINESTRING (7 3, 0 0)'),
('LINESTRING (2 4, 2 3)'),
('LINESTRING (3 8, 1 5)'),
('LINESTRING (1 5, 2 5)'),
('LINESTRING (7 3, 0 7)');

WITH endpoints AS (SELECT ST_Collect(ST_StartPoint(geom), ST_EndPoint(geom)) AS geom FROM farm_roads),
     clusters  AS (SELECT unnest(ST_ClusterWithin(geom, 1e-8)) AS geom FROM endpoints),
     clusters_with_ids AS (SELECT row_number() OVER () AS cid, ST_CollectionHomogenize(geom) AS geom FROM clusters)
SELECT ST_Collect(farm_roads.geom) AS geom
FROM farm_roads
LEFT JOIN clusters_with_ids ON ST_Intersects(farm_roads.geom, clusters_with_ids.geom)
GROUP BY cid;


WITH RECURSIVE walk_network(id_road, geom) AS (
  SELECT id_road, geom 
    FROM farm_roads 
    WHERE id_road = 2
  UNION ALL
  SELECT n.id_road, n.geom
    FROM farm_roads n, walk_network w
    WHERE ST_DWithin(
      ST_EndPoint(w.geom),
      ST_StartPoint(n.geom),0.01)
)
SELECT id_road
FROM walk_network


SELECT COUNT(CASE WHEN ST_NumGeometries(geom) > 1 THEN 1 END) AS multi_geom,
COUNT(geom) AS total_geom
FROM farm_roads;

ALTER TABLE my_table
ALTER COLUMN geom TYPE geometry(linestring,3857) USING ST_GeometryN(geom, 1);



CREATE  TABLE test_lines AS
SELECT ST_LineMerge(ST_SnapToGrid(geom),0.001)
FROM farm_roads
    ;



