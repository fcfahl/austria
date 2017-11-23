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
DROP TABLE IF EXISTS farm_buildings;
CREATE TABLE farm_buildings AS 
	SELECT * 
	FROM osm_buildings
	WHERE value = 'farm';


-- Select buindings inside crop areas only
DROP TABLE IF EXISTS farm_buildings_crop;
CREATE TABLE farm_buildings_crop AS
	SELECT a.*  	
	FROM farm_buildings AS a, corine_crop AS b
	WHERE ST_Within(a.geom, b.geom);


