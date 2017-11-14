DROP FUNCTION IF EXISTS jrc_04_routes (id_target_ int, id_building_ int[], node_target_ int,  node_farm_ int[]);
CREATE OR REPLACE FUNCTION jrc_04_routes (id_target_ int, id_building_ int[], node_target_ int,  node_farm_ int[])
RETURNS table (id_target4 int, id_building4 int, length double precision, geom geometry) AS 
-- RETURNS table (id_target4 int, id_building4 int, geom geometry) AS 
-- RETURNS table (id_target4 int, id_building4 int, length double precision, geom geometry) AS 
-- RETURNS void AS 
$$

DECLARE
n_buildings int[];
i integer; 
    	
BEGIN
RETURN QUERY 
WITH
    target_id_1 AS (SELECT id_target_ AS id_target_1),
    target_node_1 AS (SELECT node_target_ AS node_target_1),
    farm_id_array_1 AS (SELECT id_building_ AS id_farm_array_1),
    farm_node_array_1 AS (SELECT node_farm_ AS node_farm_array_1),


    dijkstra as (
	SELECT
	    dijkstra.*, topo_roads_noded.geom
	FROM	pgr_dijkstra(
		'SELECT id, source::integer, target::integer, distance::double precision AS cost FROM topo_roads_noded',
		$3, $4,false) AS dijkstra
	LEFT JOIN
	    topo_roads_noded
	ON
	    (edge = id)
	ORDER BY
	    seq	
    ),
    join_ids AS (
	SELECT b.farm_, a.*
	FROM dijkstra a
	LEFT JOIN (SELECT * FROM tmp_2_nodes) AS b
	ON (a.end_vid = b.node_farm_)
    ),
    routes AS (
	SELECT c.farm_, ST_Multi(ST_LineMerge(ST_Collect(c.geom))) AS geom
	FROM join_ids AS c
	GROUP BY c.farm_

    ),
    merge_results AS (
	SELECT d.farm_, d.geom
	FROM routes AS d
	LEFT JOIN topo_targets AS e
	ON (d.farm_ = e.id_building)
	ORDER BY d.farm_ 
    )

--     SELECT id_target_ as id_target, f.farm_ as id_building, ST_Length (f.geom), f.geom FROM join_ids AS f  ;  
--     SELECT $1 AS  id_target4, f.farm_ AS id_building4, f.geom FROM join_ids AS f  ;    
--     SELECT $1 AS  id_target4, f.farm_ AS id_building4, f.geom FROM routes AS f  ;
--     
    SELECT id_target_ AS  id_target4, f.farm_, ST_Length(f.geom), f.geom FROM merge_results AS f  ;
    
--     RAISE NOTICE 'target id / node  = % | % ', id_target_, node_target_;
--     RAISE NOTICE 'farm id / node = % | % ', id_building_, node_farm_;
--     RAISE NOTICE ''; 


END;
$$
LANGUAGE PLPGSQL;



-- select * from topo_route_tmp;


select * from jrc_04_routes (1, array[957,1339,1398], 49719, array[33528,33237,340531])