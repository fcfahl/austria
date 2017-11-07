

-- Farm Buildings
DROP TABLE IF EXISTS farm_buildings;
CREATE TABLE farm_buildings AS 
	SELECT * 
	FROM osm_buildings
	WHERE value = 'farm' 
	OR (ST_NRings (geom) > 1 AND (value <> 'commercial' and value <> 'apartments' and value <> 'manufacture' and value <> 'supermarket'and value <> 'train_station' and value <> 'house'  and value <> 'school' and value <> 'residential' and value <> 'greenhouse' ))
	OR ((ST_Area(geom) > 700 and ST_Area(geom) < 5000) AND (value <> 'commercial' and value <> 'apartments' and value <> 'manufacture' and value <> 'supermarket'and value <> 'train_station' and value <> 'house'  and value <> 'school' and value <> 'residential' and value <> 'greenhouse' ))
	;


DROP TABLE IF EXISTS farm_buildings_simplification;
CREATE TABLE farm_buildings_simplification AS 
	SELECT *
	FROM farm_buildings_crop;

ALTER TABLE farm_buildings_simplification 
ADD COLUMN ratio double precision;
UPDATE farm_buildings_simplification SET ratio = ST_Area(geom) / ST_Perimeter(geom) * 4;

, 






SELECT 
  ST_Length(ST_LongestLine(
   (SELECT geom FROM farm_buildings_crop WHERE id=3096),
   (SELECT geom FROM farm_buildings_crop WHERE id=3096))
);

area / perimeter * 4

