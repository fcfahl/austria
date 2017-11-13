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


-- Add area to Buildings
ALTER TABLE osm_buildings 
ADD COLUMN area double precision;
UPDATE osm_buildings SET area = ST_Area(geom);

-- Farm Buildings
DROP TABLE IF EXISTS farm_buildings_included;
CREATE TABLE farm_buildings_included AS 
	SELECT * 
	FROM osm_buildings
	WHERE ST_NRings (geom) > 1 or (area > 700 and area < 5000);

DROP TABLE IF EXISTS farm_buildings_excluded;
CREATE TABLE farm_buildings_excluded AS 
	SELECT * 
	FROM osm_buildings
	WHERE NOT (ST_NRings (geom) > 1 or (area > 700 and area < 5000));

-- Select buindings inside crop areas only
DROP TABLE IF EXISTS farm_buildings_crop;
CREATE TABLE farm_buildings_crop AS
	SELECT a.*  	
	FROM farm_buildings_included AS a, corine_crop AS b
	WHERE ST_Within(a.geom, b.geom);


-- Select adjacent buindings in a distance
DROP TABLE IF EXISTS farm_buildings_distance;
CREATE TABLE farm_buildings_distance AS
	SELECT b.*  	
	FROM farm_buildings_crop AS a, farm_buildings_excluded AS b
	WHERE ST_Distance(ST_Centroid(a.geom), ST_Centroid(b.geom)) < 500;


DROP TABLE IF EXISTS farm_buildings_distance;
CREATE TABLE farm_buildings_distance AS
	SELECT b.*  	
	FROM farm_buildings_crop AS a, farm_buildings_excluded AS b
	WHERE ST_Within( ST_Centroid(b.geom), ST_Buffer(ST_Centroid(a.geom), 500) );


DROP TABLE IF EXISTS farm_buildings_buffer;
CREATE TABLE farm_buildings_buffer AS
	SELECT ST_Buffer(ST_Centroid(geom), 100) AS geom 	
	FROM farm_buildings_crop ;
