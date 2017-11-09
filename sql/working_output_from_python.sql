   DO
    $$
    DECLARE
    	loop_targets record;
    	loop_farms record;
    BEGIN
        
        -- selet target nodes
        CREATE TEMPORARY TABLE IF NOT EXISTS targets AS (SELECT id_target FROM topo_targets WHERE id_target <= 1);
    
        -- LOOP 1 (TARGET)
            FOR loop_targets IN SELECT id_target FROM targets
        LOOP
            -- LOOP 2 (FARM)
                
        FOR loop_farms IN
	    SELECT a.id_building FROM (
    		SELECT * FROM jrc_get_farm_ids (
                (SELECT id_target FROM topo_targets WHERE id_target = loop_targets.id_target),
                 -- max travel distance
                50000)
    		) as a
    
            LOOP
                WITH                  

    		nodes as (select * from jrc_get_ids_from_nodes (
                -- get target id (from loop 1)
    			(SELECT id_target FROM topo_targets WHERE id_target =  loop_targets.id_target),
                -- get farm id (from loop 2)
    			(SELECT id_building FROM topo_targets WHERE id_building =  loop_farms.id_building)))   ,
                    
    		dijkstra AS (
                -- extract path with smallest distance between farm and target
                SELECT  b.*, topo_roads_noded.geom
                FROM pgr_dijkstra (
                    -- dijkstra results
                    'SELECT id, source::integer, target::integer, distance::double precision AS cost FROM topo_roads_noded',
                    -- target id
                     (SELECT node_target FROM nodes),
                     -- farm id
                     (SELECT node_farm FROM nodes), false
                     ) AS b
    			LEFT JOIN
    				topo_roads_noded
    			ON
    				(edge = id)
    			ORDER BY
    				seq
            )
    ,
                    
        -- get the linestring from dijkstra
        line as (SELECT ST_Multi(ST_Collect(geom)) as geom FROM dijkstra)
    
                    
            -- insert results into table for each pair of farm and target IDs
    		INSERT INTO topo_route_50km (id_building, id_target, cost, distance, length, geom)
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
