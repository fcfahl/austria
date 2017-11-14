DROP FUNCTION IF EXISTS jrc_02_get_road_node (id_target_ int, id_farm_ int);
CREATE OR REPLACE FUNCTION jrc_02_get_road_node (id_target_ int, id_farm_ int)
RETURNS table (id_target2 int, id_building2 int, node_target2 bigint, node_farm2 bigint, dist2 double precision) AS 
$$
    
	WITH
	farm as (
		SELECT 	a.id as id_node, b.id_building, ST_Distance(a.the_geom, b.geom) as dist
		FROM 	topo_roads_noded_vertices_pgr AS a,
			(Select * from topo_targets where id_building = $2) AS b
		WHERE 	ST_DWithin (a.the_geom, b.geom, 500) 
		ORDER BY dist ASC limit 1
	),
	target as (
		SELECT a.id as id_node, b.id_target, ST_Distance(a.the_geom, b.geom) as dist
		FROM 	topo_roads_noded_vertices_pgr AS a,
			(Select * from topo_targets where id_target = $1) AS b
		WHERE 	ST_DWithin (a.the_geom, b.geom, 500) 
		ORDER BY dist ASC limit 1
	)
	SELECT target.id_target, farm.id_building, target.id_node, farm.id_node, target.dist
	FROM farm, target

$$ LANGUAGE SQL;

--

SELECT * FROM jrc_02_get_road_node (1, 1398);
