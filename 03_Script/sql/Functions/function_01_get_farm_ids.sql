DROP FUNCTION IF EXISTS jrc_01_get_farm_ids (id_target_ int, distance int);

CREATE OR REPLACE FUNCTION jrc_01_get_farm_ids (id_target_ int, distance int)
RETURNS table (id_building1 int) AS
$$
    
	SELECT DISTINCT a.id_building
	FROM (Select * from topo_targets where id_building > 0) as a
	JOIN (Select * from topo_targets where id_target = $1) as b
	ON ST_DWithin (a.geom, b.geom, $2)

$$ LANGUAGE SQL;
