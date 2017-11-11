DROP TABLE IF EXISTS topo_building_selection;
CREATE TABLE topo_building_selection AS
SELECT DISTINCT a.* 
FROM (Select * from topo_targets where id_building > 0) as a
JOIN (Select * from topo_targets where id_target = 10) as b
ON ST_DWithin (a.geom, b.geom, 10000)
;

select * from topo_building_selection;

DROP TABLE IF EXISTS tmp_array;
CREATE TABLE tmp_array AS
SELECT id_building FROM topo_building_selection;

select * from tmp_array;


DROP TABLE IF EXISTS tmp_test;
CREATE TABLE tmp_test (id_building int);
	
DO
$$
DECLARE 
	rec record;
BEGIN
	FOR rec IN
	    SELECT id_building FROM tmp_array
	LOOP	
	    INSERT INTO tmp_test (id_building)
	    SELECT id_building FROM topo_targets where id_building =  rec.id_building ;
	END LOOP;

END
$$;


select * from tmp_test;


-- select * from jrc_get_farms_array (10, 1712);