DROP FUNCTION IF EXISTS jrc_get_ids_arrays (id_target_ int, distance int);
CREATE OR REPLACE FUNCTION jrc_get_ids_arrays(id_target_ int, distance int)
-- RETURNS integer[] AS
RETURNS table (id_target2 int, id_building2 int, node_building2 int) AS 
$$

DECLARE
n_buildings int[];
i integer; 
    	
BEGIN
	

	n_buildings := ((SELECT array_agg(id_building) FROM jrc_get_farm_ids (
		(SELECT id_target FROM topo_targets WHERE id_target = $1), $2))::int[]);


	FOR i IN 1 .. array_upper(n_buildings, 1)
	LOOP
	
		id_building2 := n_buildings[i]; 
		id_target2 := $1;

		node_building2 := (select node_farm from jrc_get_ids_from_nodes (
      			(SELECT id_target FROM topo_targets WHERE id_target =  $1),
    			(SELECT id_building FROM topo_targets WHERE id_building =  n_buildings[i]))) ;
    			
      		RAISE NOTICE 'The id is : % and the node is : % s', n_buildings[i] , node_building2;          
       
		RETURN NEXT;

	END LOOP;
		

END;
$$
LANGUAGE PLPGSQL;

select * from jrc_get_ids_arrays (1, 5000);

--

select * from topo_targets;