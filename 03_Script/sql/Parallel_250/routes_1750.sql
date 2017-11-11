			-- Step 1 - Select targets 
			
DROP TABLE IF EXISTS target_1750;
CREATE TABLE target_1750 AS
SELECT id_target 
FROM topo_targets 
where id_target > 1500;

			-- Extract Routes

DROP TABLE IF EXISTS topo_route_1750;
CREATE TABLE topo_route_1750 (id_building integer, id_target integer, cost double precision, distance double precision, length double precision);

SELECT AddGeometryColumn ('public', 'topo_route_1750','geom', 3035, 'multilinestring', 2);
	
DO
$$
DECLARE 
	loop_targets record;	
	loop_farms record;	
BEGIN
    -- LOOP TARGET
    FOR loop_targets IN
        SELECT id_target FROM target_1750
        
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
		INSERT INTO topo_route_1750 (id_building, id_target, cost, distance, length, geom)
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


select * from topo_route_1750;

