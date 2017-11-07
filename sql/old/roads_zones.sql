-- Destination zonnes
DROP TABLE IF EXISTS roads_zones;
CREATE TABLE roads_zones AS 
	SELECT * 
	FROM osm_roads 
	WHERE value = 'primary' OR value = 'secondary' OR value = 'tertiary';


-- Points every km
-- https://gis.stackexchange.com/questions/88196/how-can-i-transform-polylines-into-points-every-n-metres-in-postgis

DROP TABLE IF EXISTS roads_zones_points;
CREATE TABLE roads_zones_points AS (

WITH line AS 
    (SELECT
        (ST_Dump(geom)).geom AS geom
    FROM farm_roads_union),
linemeasure AS
    (SELECT
        ST_AddMeasure(line.geom, 0, ST_Length(line.geom)) AS linem,
        generate_series(0, ST_Length(line.geom)::int, 100) AS i
    FROM line),
geometries AS (
    SELECT
        i,
        (ST_Dump(ST_GeometryN(ST_LocateAlong(linem, i), 1))).geom AS geom 
    FROM linemeasure)

SELECT
    i,
    ST_SetSRID(ST_MakePoint(ST_X(geom), ST_Y(geom)), 3035) AS geom
FROM geometries)




DROP TABLE IF EXISTS farm_roads_dissolve;
CREATE TABLE farm_roads_dissolve AS 
(
	WITH endpoints AS (
		SELECT ST_Collect(ST_StartPoint(geom), ST_EndPoint(geom)) AS geom FROM farm_roads),
	     clusters  AS (
		SELECT unnest(ST_ClusterWithin(geom, 1e-8)) AS geom FROM endpoints),
	     clusters_with_ids AS (
		SELECT row_number() OVER () AS cid, ST_CollectionHomogenize(geom) AS geom FROM clusters)
		
	SELECT ST_Collect(farm_roads.geom) AS geom
	FROM farm_roads
	LEFT JOIN clusters_with_ids ON ST_Intersects(farm_roads.geom, clusters_with_ids.geom)
	GROUP BY cid
);