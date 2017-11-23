DROP FUNCTION IF EXISTS jrc_04_routes (id_target_ int, distance int);
CREATE OR REPLACE FUNCTION jrc_04_routes(id_target_ int, distance int)
RETURNS table (id_target3 int, id_building3 int, id_mun3 int, geom geometry) AS 
$$

DECLARE
n_buildings int[];
i integer; 
    	
BEGIN
RETURN QUERY 
WITH
    target AS
    (
	SELECT 	a.id AS id_node, b.id_target, ST_Distance(a.the_geom, b.geom) AS dist
	FROM 	topo_roads_noded_vertices_pgr AS a,
	    (SELECT topo_targets.id_target, topo_targets.geom FROM topo_targets WHERE id_target = $1 ) AS b
	WHERE 	ST_DWithin (a.the_geom, b.geom, 500)
	ORDER BY dist ASC
	LIMIT 1
    ),
    farm AS
    (
	SELECT array_agg(node_building2) AS id_node FROM jrc_03_get_node_arrays ($1, $2)
    ),
    dijkstra as (
	SELECT
	    dijkstra.*, topo_roads_noded.geom
	FROM	pgr_dijkstra(
		'SELECT id, source::integer, target::integer, distance::double precision AS cost FROM topo_roads_noded',
		(SELECT id_node FROM target), (SELECT id_node FROM farm),false) AS dijkstra
	LEFT JOIN
	    topo_roads_noded
	ON
	    (edge = id)
	ORDER BY
	    seq
    ),
    join_ids AS (
	SELECT b.id_building2 AS id_building, b.id_target2 AS id_target, a.*
	FROM dijkstra a
	LEFT JOIN (SELECT * FROM jrc_03_get_node_arrays ($1, $2)) b
	ON (a.end_vid = b.node_building2)
    ),
    routes AS (
	SELECT id_building, ST_Multi(ST_LineMerge(ST_Collect(join_ids.geom))) AS geom
	FROM join_ids
	GROUP BY id_building
	),
    final AS (
	SELECT a.id_building, a.geom, b.id_mun
	FROM routes AS a
	LEFT JOIN topo_targets AS b
	ON (a.id_building = b.id_building)
	ORDER BY a.id_building 
    )
    SELECT b.id_target, a.id_building, a.id_mun, a.geom
    FROM final a, target b   

    ;
END;
$$
LANGUAGE PLPGSQL;


select * from jrc_04_routes (1, 10000)