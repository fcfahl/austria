DROP TABLE IF EXISTS tmp_buffer2;
CREATE TABLE tmp_buffer2 AS
WITH 
    buffer AS (
	SELECT ST_Buffer(ST_Union(geom),500) as geom 
	FROM lulc_corine_12_adm
	WHERE code_12 = '111' OR code_12 = '112' OR code_12 = '121' OR code_12 = '122' OR code_12 = '131'
    ),
    urban AS (
	SELECT ST_Union (geom) as geom 
	FROM lulc_corine_12_adm
	WHERE code_12 = '111' OR code_12 = '112'
    ),
    industrial AS (
	SELECT ST_Union (geom) as geom 
	FROM lulc_corine_12_adm
	WHERE code_12 = '121' OR code_12 = '122' OR code_12 = '131'
    ),
    agriculture AS (
	SELECT (ST_Dump(ST_SnapToGrid(ST_Union (geom),0.0001))).geom as geom 
	FROM lulc_corine_12_adm
	WHERE code_12 = '211' OR code_12 = '231' OR code_12 = '242' OR code_12 = '243'
    ),
    diff AS (
	SELECT ST_Union(ST_Difference (a.geom, b.geom)) as geom 
	FROM buffer a, urban b

    )

SELECT a.geom as buffer, b.geom as urban, c.geom as industrial, d.geom as agri, e.geom as diff
FROM buffer a, urban b, industrial c, agriculture d, diff e
;

