
DROP FUNCTION IF EXISTS jrc_01_get_farm_ids (id_target_ int, distance int);
CREATE OR REPLACE FUNCTION jrc_01_get_farm_ids (id_target_ int, distance int)
RETURNS table (id_building1 int) AS
$$    
	SELECT DISTINCT a.id_building
	FROM (Select * from topo_targets where id_building > 0) as a
	JOIN (Select * from topo_targets where id_target = $1) as b
	ON ST_DWithin (a.geom, b.geom, $2)
	
$$ LANGUAGE SQL;
    
 
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
    
 
DROP FUNCTION IF EXISTS jrc_03_get_node_arrays (id_target_ int, distance int);
CREATE OR REPLACE FUNCTION jrc_03_get_node_arrays (id_target_ int, distance int)
RETURNS table (id_target3 int, id_building3 int, node_target3 int, node_building3 int) AS
$$
DECLARE
	n_buildings int[]; i integer;
	distance_fix int := distance * 0.8; 
BEGIN
    
n_buildings := ((SELECT array_agg(id_building1) FROM jrc_01_get_farm_ids (
	(SELECT id_target FROM topo_targets WHERE id_target = $1), distance_fix))::int[]);

FOR i IN 1 .. array_upper(n_buildings, 1)
LOOP
    id_target3 := $1;
	id_building3 := n_buildings[i];

	node_target3 := (SELECT node_target2 FROM jrc_02_get_road_node (
				(SELECT id_target FROM topo_targets WHERE id_target =  $1),
				(SELECT id_building FROM topo_targets WHERE id_building =  n_buildings[i]))
			) ;


	node_building3 := (SELECT node_farm2 FROM jrc_02_get_road_node (
				(SELECT id_target FROM topo_targets WHERE id_target =  $1),
				(SELECT id_building FROM topo_targets WHERE id_building =  n_buildings[i]))
			) ;

	-- RAISE NOTICE 'The id is : % and the node is : % s', n_buildings[i] , node_building2;

	RETURN NEXT;
END LOOP;

END;
$$ LANGUAGE PLPGSQL;
    
  
DROP FUNCTION IF EXISTS jrc_04_routes (id_target_ int, id_building_ int[], node_target_ int,  node_farm_ int[]);
CREATE OR REPLACE FUNCTION jrc_04_routes (id_target_ int, id_building_ int[], node_target_ int,  node_farm_ int[])
RETURNS table (id_target4 int, id_building4 int, length4 double precision, geom geometry) AS
$$
DECLARE
	n_buildings int[]; i integer;
	distance int := 50000;
BEGIN
    
RETURN QUERY
WITH
    target_id_1 AS (SELECT id_target_ AS id_target_1),
    target_node_1 AS (SELECT node_target_ AS node_target_1),
    farm_id_array_1 AS (SELECT id_building_ AS id_farm_array_1),
    farm_node_array_1 AS (SELECT node_farm_ AS node_farm_array_1),

    dijkstra as (
	SELECT
	    $1 as dij_target, dijkstra.*, topo_roads_noded.geom
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
	SELECT b.target_, b.farm_, a.*
	FROM dijkstra a
	LEFT JOIN (SELECT * FROM route_node_ids) AS b
	ON (a.end_vid = b.node_farm_ AND a.dij_target = b.target_)
    ),
    routes AS (
	SELECT c.farm_, ST_Multi(ST_LineMerge(ST_Collect(c.geom))) AS geom
	FROM join_ids AS c
	GROUP BY c.farm_
    ),
    merge_results AS (
	SELECT d.farm_, d.geom, ST_Length(d.geom) as length
	FROM routes AS d
	LEFT JOIN topo_targets AS e
	ON (d.farm_ = e.id_building)
	ORDER BY d.farm_
    )
    SELECT id_target_ AS  id_target4, f.farm_, f.length, f.geom FROM merge_results AS f WHERE  f.length <= distance ;

    --RAISE NOTICE 'target id / node  = % | % ', id_target_, node_target_;
    --RAISE NOTICE 'farm id / node = % | % ', id_building_, node_farm_;
    --RAISE NOTICE '';


END;
$$ LANGUAGE PLPGSQL;
    
