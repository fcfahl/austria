-- DROP FUNCTION IF EXISTS jrc_get_ids_from_nodes(id_target_ int, id_farm_ int);
-- 
-- CREATE OR REPLACE FUNCTION jrc_get_ids_from_nodes(id_target_ int, id_farm_ int)
-- RETURNS table (id_target int, id_building int, node_target bigint, node_farm bigint, dist double precision) AS 
-- $$
-- 	WITH
-- 	farm as (
-- 		SELECT 	a.id as id_node, b.id_building, ST_Distance(a.the_geom, b.geom) as dist 
-- 		FROM 	topo_roads_noded_vertices_pgr AS a, 
-- 			(Select * from topo_targets where id_building = $2) AS b
-- 		WHERE 	ST_DWithin (a.the_geom, b.geom, 500)
-- 		order by dist ASC limit 1
-- 	),
-- 	target as (
-- 		SELECT 	a.id as id_node, b.id_target, ST_Distance(a.the_geom, b.geom) as dist  
-- 		FROM 	topo_roads_noded_vertices_pgr AS a, 
-- 			(Select * from topo_targets where id_target = $1) AS b
-- 		WHERE 	ST_DWithin (a.the_geom, b.geom, 500)
-- 		order by dist ASC limit 1
-- 	)
-- 	SELECT target.id_target, farm.id_building, target.id_node, farm.id_node, target.dist
-- 	FROM farm, target
-- 	
-- $$ LANGUAGE SQL;
-- 
-- select * from jrc_get_ids_from_nodes (10, 1712);

--

DROP FUNCTION IF EXISTS jrc_get_ids_from_nodes(id_target_ int, id_farm_ int);
CREATE OR REPLACE FUNCTION jrc_get_ids_from_nodes(id_target_ int, id_farm_ int)
RETURNS table (id_target int, id_building int, node_target bigint, node_farm bigint, dist double precision) AS 
$$
	WITH
	farm as (
		SELECT 	a.id as id_node, b.id_building, ST_Distance(a.the_geom, b.geom) as dist 
		FROM 	topo_roads_noded_vertices_pgr AS a, 
			(Select * from topo_targets where id_building = $2) AS b
		WHERE 	ST_DWithin (a.the_geom, b.geom, 500)
		order by dist ASC limit 1
	),
	target as (
		SELECT 	a.id as id_node, b.id_target, ST_Distance(a.the_geom, b.geom) as dist  
		FROM 	topo_roads_noded_vertices_pgr AS a, 
			(Select * from topo_targets where id_target = $1) AS b
		WHERE 	ST_DWithin (a.the_geom, b.geom, 500)
		order by dist ASC limit 1
	)
	SELECT target.id_target, farm.id_building, target.id_node, farm.id_node, target.dist
	FROM farm, target
	
$$ LANGUAGE SQL;

select * from jrc_get_ids_from_nodes (10, 1712);