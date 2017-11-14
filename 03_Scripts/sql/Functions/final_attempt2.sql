DROP TABLE IF EXISTS topo_route_tmp;
CREATE TABLE topo_route_tmp (
	id_target integer, 
	id_building integer, 
	length double precision);

SELECT AddGeometryColumn ('public', 'topo_route_tmp','geom', 3035, 'multilinestring', 2);


-- Step 1: Selet target points

DROP TABLE IF EXISTS tmp_1_targets;
CREATE TABLE tmp_1_targets AS (SELECT id_target, geom FROM site_clean WHERE id_target <= 250);

-- Step 2: Create node table

DROP TABLE IF EXISTS tmp_2_nodes;
CREATE TABLE tmp_2_nodes (target_ int, farm_ int, node_target_ int, node_farm_ int);


-- Step 2: Loop all the target points

DO
$$
DECLARE
	i integer; 
	distance int := 50000; 
	n_targets int[];
	id_target_ int;
	node_target_ int;
	id_building_ int[];
	node_building_ int[];
	n_farms int;
	results RECORD;
	geom_ geometry;

BEGIN

	-- Step 3: Get number of target points
	n_targets := (SELECT array_agg(id_target) FROM tmp_1_targets);


	-- Step 4: Perform the Loop 
	FOR i IN 1 .. array_upper(n_targets, 1)
	LOOP	
		
		id_target_ := n_targets[i];

		-- Step 4: Get the id nodes (from roads) of the target and farm locations within a given distance
		INSERT INTO "tmp_2_nodes" (target_, farm_, node_target_, node_farm_)
			SELECT id_target3 AS target_, id_building3 AS farm_, 
				node_target3 AS node_target_, node_building3 AS node_farm_ 
			FROM jrc_03_get_node_arrays (id_target_, distance);
		

		-- Step 5: Assign the ids to variables	
	 	node_target_ := (SELECT a.node_target_ FROM tmp_2_nodes AS a WHERE target_ = id_target_ LIMIT 1);
		id_building_ := (SELECT array_agg(farm_) FROM tmp_2_nodes WHERE target_ = id_target_);
		node_building_ := (SELECT array_agg(node_farm_) FROM tmp_2_nodes);

		n_farms := (SELECT COUNT (*) FROM tmp_2_nodes);
-- 	
		RAISE NOTICE 'target id / node  = % | % ', id_target_, node_target_;
		RAISE NOTICE 'number of farms = % ', n_farms;
		RAISE NOTICE 'farm id / node = % | % ', id_building_, node_building_;
		RAISE NOTICE '';
-- 	

		-- Step 5: do the routing between the target (point) and the farms (array)

		INSERT INTO "topo_route_tmp" (id_target, id_building, length, geom)
		SELECT id_target4, id_building4, length, geom FROM jrc_04_routes (id_target_, id_building_, node_target_, node_building_);

		

		
-- 		SELECT * FROM jrc_04_routes(n_targets[i], 10000);
		
	END LOOP;
END
$$;

-- select * from tmp_1_targets
-- select * from tmp_2_nodes
-- SELECT *  FROM topo_targets