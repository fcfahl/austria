			-- Step 1 - Select targets 
			
DROP TABLE IF EXISTS topo_step1;
CREATE TABLE topo_step1 AS
SELECT id_target 
FROM topo_targets 
where id_target <=20;
--limit 3; -- to be deleted later
-- 
-- select * from topo_step1;

			-- Step 2 - Find farm IDs for each target

DROP FUNCTION IF EXISTS jrc_get_farm_ids (id_target_ int);

CREATE OR REPLACE FUNCTION jrc_get_farm_ids (id_target_ int)
RETURNS table (id_building int) AS 
$$
	SELECT DISTINCT a.id_building 
	FROM (Select * from topo_targets where id_building > 0) as a
	JOIN (Select * from topo_targets where id_target = $1) as b
	ON ST_DWithin (a.geom, b.geom, 10000)
	--limit 5 -- to be deleted later
		
	
$$ LANGUAGE SQL;


select * from jrc_get_farm_ids (100);

			-- Step 3 - Loop each Farm ID


DROP TABLE IF EXISTS topo_step3;
CREATE TABLE topo_step3 (id_building integer, id_target integer, cost double precision, distance double precision, length double precision);

SELECT AddGeometryColumn ('public', 'topo_step3','geom', 3035, 'multilinestring', 2);
	
DO
$$
DECLARE 
	loop_targets record;	
	loop_farms record;	
BEGIN
    -- LOOP TARGET
    FOR loop_targets IN
        SELECT id_target FROM topo_step1
        
    LOOP
	-- LOOP FARM
	FOR loop_farms IN
	    SELECT a.id_building FROM (
		SELECT * FROM jrc_get_farm_ids (


			(SELECT id_target FROM topo_targets where id_target = loop_targets.id_target)


		)) as a
	LOOP
	    WITH
		nodes as (select * from jrc_get_ids_from_nodes (
			(SELECT id_target FROM topo_targets where id_target =  loop_targets.id_target), 
			(SELECT id_building FROM topo_targets where id_building =  loop_farms.id_building))),
		dijkstra as (
			SELECT
				dijkstra.*, topo_roads_noded.geom
			FROM	pgr_dijkstra
				(
				    'SELECT id, source::integer, target::integer, distance::double precision AS cost FROM topo_roads_noded', 
				     (SELECT node_target FROM nodes), (SELECT node_farm FROM nodes), false
				    
				) AS dijkstra
			LEFT JOIN
				topo_roads_noded
			ON
				(edge = id)
			ORDER BY
				seq
		),
		line as (
			SELECT ST_Multi(ST_Collect(geom)) as geom FROM dijkstra
		)
		INSERT INTO topo_step3 (id_building, id_target, cost, distance, length, geom)
		VALUES (
			(SELECT id_building FROM nodes),
			(SELECT id_target FROM nodes),
			(SELECT sum(dist) FROM nodes),
			(SELECT dist FROM nodes),
			(SELECT ST_Length (geom) FROM line),
			(SELECT geom FROM line)
		);
	END LOOP;
END LOOP;
END
$$;


select * from topo_step3;

