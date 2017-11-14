DROP FUNCTION IF EXISTS jrc_01_get_farm_ids (id_target_ int, distance int);

CREATE OR REPLACE FUNCTION jrc_01_get_farm_ids (id_target_ int, distance int)
RETURNS table (id_target1 int, id_building1 int, geom geometry) AS
$$
    
	SELECT $1 as id_target, a.id_building, a.geom
	FROM (Select * from topo_targets where id_building > 0) as a
	JOIN (Select * from topo_targets where id_target = $1) as b
	ON ST_DWithin (a.geom, b.geom, $2)

$$ LANGUAGE SQL;


DROP TABLE IF EXISTS tmp_function_1_farm_ids;
CREATE TABLE tmp_function_1_farm_ids AS
SELECT * FROM jrc_01_get_farm_ids (2, 5000);


SELECT * FROM tmp_function_1_farm_ids order by id_building1;