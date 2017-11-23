CREATE SEQUENCE serial 
START 1
increment 1;


-- Clip Corine
DROP TABLE IF EXISTS corine_adm;
CREATE TABLE corine_adm AS 
	(SELECT ST_Intersection(a.geom, b.geom) AS geom, b.mun_id, b.name, a.code_12 AS code 
	FROM corine_2012 AS a, adm AS b
	WHERE ST_Intersects(a.geom, b.geom));

-- Crop Areas
DROP TABLE IF EXISTS corine_crop;
CREATE TABLE corine_crop AS 
	SELECT * 
	FROM corine_adm 
	WHERE code = '243' or code = '211' or code = '231' or code = '321' or code = '242';


-- Farm Buildings
DROP TABLE IF EXISTS farm_buildings;
CREATE TABLE farm_buildings AS 
	SELECT * 
	FROM osm_buildings
	WHERE value = 'farm' 
	OR (ST_NRings (geom) > 1 AND (value <> 'commercial' and value <> 'apartments' and value <> 'manufacture' and value <> 'supermarket'and value <> 'train_station' and value <> 'house'  and value <> 'school' and value <> 'residential' and value <> 'greenhouse' ))
	OR ((ST_Area(geom) > 700 and ST_Area(geom) < 5000) AND (value <> 'commercial' and value <> 'apartments' and value <> 'manufacture' and value <> 'supermarket'and value <> 'train_station' and value <> 'house'  and value <> 'school' and value <> 'residential' and value <> 'greenhouse' ))
	;


-- Add fields to Buildings
ALTER TABLE farm_buildings 
ADD COLUMN bld_area double precision;
UPDATE farm_buildings SET bld_area = ST_Area(geom);

-- Add municipal ID to buildings
ALTER TABLE farm_buildings 
ADD COLUMN id_mun integer;
UPDATE farm_buildings 
	SET id_mun = adm.mun_id 
	FROM adm 
	WHERE ST_Within(farm_buildings.geom, adm.geom) ;


-- Select buindings inside crop areas only
DROP TABLE IF EXISTS farm_buildings_crop;
CREATE TABLE farm_buildings_crop AS
	SELECT a.*  	
	FROM farm_buildings AS a, corine_crop AS b
	WHERE ST_Within(a.geom, b.geom);

ALTER TABLE farm_buildings_crop 
ADD COLUMN id_cluster integer;


-- Extract buinding centroids
DROP TABLE IF EXISTS farm_buildings_points;
CREATE TABLE farm_buildings_points AS
	SELECT id_mun, bld_area, value, ST_Centroid(geom) as geom 
	FROM farm_buildings_crop;

-- Cluster buildings
-- https://gis.stackexchange.com/questions/11567/spatial-clustering-with-postgis
DROP TABLE IF EXISTS farm_buildings_clusterd;
CREATE TABLE farm_buildings_clusterd AS
SELECT row_number() over () AS id,
  ST_NumGeometries(gc),
  gc AS geom_collection,
  ST_Centroid(gc) AS centroid,
  ST_MinimumBoundingCircle(gc) AS circle,
  sqrt(ST_Area(ST_MinimumBoundingCircle(gc)) / pi()) AS radius
FROM (
  SELECT unnest(ST_ClusterWithin(geom, 100)) gc FROM farm_buildings_points) f;

--

-- buffer around cluster centroids 
DROP TABLE IF EXISTS farm_cluster_buffer;
CREATE TABLE farm_cluster_buffer AS
	SELECT 
		CASE WHEN a.radius = 0
		THEN ST_Buffer(a.centroid,50) -- distance should be half of the cluster distance
		ELSE a.circle 
		END
	AS geom
	FROM farm_buildings_clusterd AS a;
	
ALTER TABLE farm_cluster_buffer ADD COLUMN id_cluster serial not null;

-- update cluster id on buildings
UPDATE farm_buildings_crop 
	SET id_cluster = farm_cluster_buffer.id_cluster
	FROM farm_cluster_buffer
	WHERE ST_Intersects(farm_buildings_crop.geom, farm_cluster_buffer.geom);


-- union clusted buildings
DROP TABLE IF EXISTS farm_buildings_union;
CREATE TABLE farm_buildings_union AS
	SELECT id_cluster, id_mun, ST_Multi(ST_Union(geom)) as geom, sum(ST_Area(geom)) AS total_area
	FROM farm_buildings_crop 
	GROUP BY id_cluster, id_mun
	ORDER BY total_area;

CREATE INDEX ON farm_buildings_union (total_area);


-- extract envelope centroids
DROP TABLE IF EXISTS farm_buildings_union_centroids;
CREATE TABLE farm_buildings_union_centroids AS
	SELECT id_mun, total_area, 0 AS rank, 0 AS row, '0'::text AS index, ST_Centroid(ST_Envelope(geom)) as geom
	FROM farm_buildings_union
	ORDER BY id_mun ASC,  total_area DESC;


-- add IDS for each farm 
ALTER SEQUENCE serial RESTART WITH 1;

ALTER TABLE farm_buildings_union_centroids 
ADD COLUMN id int;

UPDATE farm_buildings_union_centroids 
SET id = nextval('serial');


-- rank the farms according to municipal ID
WITH a AS 
(
    SELECT id,id_mun,
    row_number() OVER (ORDER BY id_mun) AS row,
    rank() OVER (ORDER BY id_mun) AS rank
    
    FROM farm_buildings_union_centroids 
) 

UPDATE farm_buildings_union_centroids AS b
   SET rank =  a.rank, row = a.row, index = concat (a.id_mun, '_', (1 + a.row - a.rank))
   FROM  a
   WHERE  a.id = b.id;
        

SELECT * FROM farm_buildings_union_centroids WHERE id_mun = 30502 ORDER BY rank  LIMIT 20 ;




