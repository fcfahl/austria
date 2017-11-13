DROP FUNCTION IF EXISTS jrc_03_get_node_arrays (id_target_ int, distance int);
CREATE OR REPLACE FUNCTION jrc_03_get_node_arrays (id_target_ int, distance int)
RETURNS table (id_target3 int, id_building3 int, node_target3 int, node_building3 int) AS
$$
	DECLARE
		n_buildings int[]; i integer;
	BEGIN
	    
		n_buildings := ((SELECT array_agg(id_building1) FROM jrc_01_get_farm_ids (
		(SELECT id_target FROM topo_targets WHERE id_target = $1), $2))::int[]);

		FOR i IN 1 .. array_upper(n_buildings, 1)
		LOOP
			id_target3 := $1;

			id_building3 := n_buildings[i];


			node_target3 := (SELECT node_target2 FROM jrc_02_get_road_node (
					(SELECT id_target FROM topo_targets WHERE id_target =  $1),
					(SELECT id_building FROM topo_targets WHERE id_building =  n_buildings[i]))) ;
			

			node_building3 := (SELECT node_farm2 FROM jrc_02_get_road_node (
					(SELECT id_target FROM topo_targets WHERE id_target =  $1),
					(SELECT id_building FROM topo_targets WHERE id_building =  n_buildings[i]))) ;

-- 			RAISE NOTICE 'The farm id is : % and the node is : % s', n_buildings[i] , node_building2;

			RETURN NEXT;
		END LOOP;

	END;
$$ LANGUAGE PLPGSQL;

select * from jrc_03_get_node_arrays (1, 5000)
