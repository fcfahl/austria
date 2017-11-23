

SELECT DISTINCT value FROM osm_roads;

-- residential areas
DROP TABLE IF EXISTS osm_residential;
CREATE TABLE osm_residential AS 
	SELECT * 
	FROM osm_landuse 
	WHERE value = 'residential';




-- Farm roads (crop by ADM)
DROP TABLE IF EXISTS farm_roads;
CREATE TABLE farm_roads AS 
	SELECT 0 as id_road, a.geom as geom
	FROM osm_roads AS a, adm AS b
	WHERE (a.value = 'primary' 
		OR a.value = 'secondary' 
		OR a.value = 'tertiary'
		OR a.value = 'motorway'
		OR a.value = 'motorway_link'
		OR a.value = 'unclassified'
		OR a.value = 'service'
		OR a.value = 'residential'
		OR a.value = 'track')
	AND	ST_Intersects(a.geom, b.geom)		
	;


ALTER SEQUENCE serial RESTART WITH 1;
UPDATE farm_roads 
SET id_road = nextval('serial');

SELECT *, ST_GeometryType(geom) FROM farm_roads LIMIT 10;


-- Segmentize rods (max length = 500 m)
-- https://gis.stackexchange.com/questions/64898/split-all-osm-roads-within-boundingbox-in-20m-segments-and-save-to-new-table
DROP TABLE IF EXISTS farm_roads_segment;
CREATE TABLE farm_roads_segment AS 
SELECT id_road AS id_road, 0 as length, ST_MakeLine(start_point,end_point) AS geom 
FROM
(
    SELECT 
        ST_Pointn(geom, generate_series(1, ST_NumPoints(geom)-1)) as start_point, 
        ST_Pointn(geom, generate_series(2, ST_NumPoints(geom))) as end_point,
        id_road
    FROM (
        SELECT id_road, ST_Segmentize(geom,500) AS geom
        FROM farm_roads
        ) as line
) as a
;     

UPDATE farm_roads_segment 
SET length = ST_Length(geom);

SELECT *, ST_GeometryType(geom) FROM farm_roads_segment ORDER BY length DESC LIMIT 10;


-- Farm roads (dissolved)
DROP TABLE IF EXISTS farm_roads_union;
CREATE TABLE farm_roads_union AS 
	SELECT ST_Union(geom) as geom
	FROM farm_roads_segment	
	;

SELECT *, ST_GeometryType(geom) FROM farm_roads_union LIMIT 10;


-- Snap farms to roads (faster to do it with dissolved than segmented roads)
DROP TABLE IF EXISTS farm_snap;
CREATE TABLE farm_snap AS 
	SELECT a.index, a.id_mun, a.id, a.total_area, a.total_lsu, a.total_manure, a.total_methane, 			ST_Closestpoint(ST_Collect(b.geom), a.geom) AS geom 
	FROM farm_join a, farm_roads_union b
	GROUP BY a.index, a.id_mun, a.id, a.total_area, a.total_lsu, a.total_manure, a.total_methane, a.geom;

SELECT *, ST_GeometryType(geom) FROM farm_snap;


	
-- Extract roads inside buffer (also faster to do it with dissolved than segmented roads)
DROP TABLE IF EXISTS farm_roads_30531_62;
CREATE TABLE farm_roads_30531_62 AS 
	SELECT a.index, a.id_mun, a.id, a.total_area, a.total_lsu, a.total_manure, a.total_methane,
		ST_Intersection(ST_Buffer(a.geom, 5000), b.geom) AS geom		
	FROM farm_snap AS a, farm_roads_union AS b
	WHERE a.index = '30531_62'
	;

SELECT *, ST_GeometryType(geom) FROM farm_roads_30531_62;


DROP TABLE IF EXISTS farm_roads_30531_63;
CREATE TABLE farm_roads_30531_63 AS 
	SELECT a.index, a.id_mun, a.id, a.total_area, a.total_lsu, a.total_manure, a.total_methane,
		(ST_Dump(ST_SnapToGrid(ST_Intersection(ST_Buffer(a.geom, 5000), b.geom),0.001))).geom AS geom		
	FROM farm_snap AS a, farm_roads_union AS b
	WHERE a.index = '30531_63'
	;

SELECT *, ST_GeometryType(geom) FROM farm_roads_30531_63;

DROP TABLE IF EXISTS farm_roads_30531_64;
CREATE TABLE farm_roads_30531_64 AS 
	SELECT a.index, a.id_mun, a.id, a.total_area, a.total_lsu, a.total_manure, a.total_methane,
		(ST_Dump(ST_SnapToGrid(ST_Intersection(ST_Buffer(a.geom, 5000), b.geom),0.001))).geom AS geom		
	FROM farm_snap AS a, farm_roads AS b
	WHERE a.index = '30531_64'
	;

SELECT *, ST_GeometryType(geom) FROM farm_roads_30531_64;


DROP TABLE IF EXISTS farm_roads_30531_65;
CREATE TABLE farm_roads_30531_65 AS 
	SELECT a.index, a.id_mun, a.id, a.total_area, a.total_lsu, a.total_manure, a.total_methane,
		(ST_Dump(ST_SnapToGrid(ST_Intersection(ST_Buffer(a.geom, 5000), b.geom),0.001))).geom AS geom		
	FROM farm_snap AS a, farm_roads AS b
	WHERE a.index = '30531_65'
	;

SELECT *, ST_GeometryType(geom) FROM farm_roads_30531_65;



-- Find optimal location
DROP TABLE IF EXISTS plant_1;
CREATE TABLE plant_1 AS 
	SELECT (a.total_lsu + b.total_lsu) as total, e.geom	as geom
	FROM farm_roads_30531_62 AS a, farm_roads_30531_62 AS b,
		farm_roads_30531_64 AS c, farm_roads_30531_65 AS d,
		farm_roads AS e

			;

SELECT *, ST_GeometryType(geom) FROM plant_1;


	
-- Create topology
CREATE EXTENSION postgis_topology;
SELECT topology.CreateTopology('farm_roads_buffer_topo', 3035);
SELECT topology.AddTopoGeometryColumn('farm_roads_buffer_topo', 'public', 'farm_roads_buffer', 'topo_geom', 'LINESTRING');
UPDATE farm_roads_buffer SET topo_geom = topology.toTopoGeom(wkb_geometry, 'farm_roads_buffer_topo', 1, 1.0);






