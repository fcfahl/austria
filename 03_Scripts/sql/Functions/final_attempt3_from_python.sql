﻿DROP TABLE IF EXISTS route_distance_3_5km;
CREATE TABLE route_distance_3_5km (id_target int, id_building int, length double precision );
SELECT AddGeometryColumn ('public', 'route_distance_3_5km','geom', 3035, 'multilinestring', 2);

DROP TABLE IF EXISTS route_targets;
CREATE TABLE route_targets AS
SELECT id_target, geom
FROM topo_targets
WHERE id_target >0  AND id_target < = 2 ;

DROP TABLE IF EXISTS route_node_ids;
CREATE TABLE route_node_ids (target_ int, farm_ int, node_target_ int, node_farm_ int );

    
DO
$$
DECLARE

	i integer;
	distance int := 5000;
	n_targets int[];
	id_target_ int;
	node_target_ int;
	id_building_ int[];
	node_building_ int[];
	n_farms int;
	results RECORD;
	geom_ geometry;

BEGIN
-- Get number of target points
n_targets := (SELECT array_agg(id_target) FROM route_targets);

-- Perform the Loop
FOR i IN 1 .. array_upper(n_targets, 1)
LOOP

	id_target_ := n_targets[i];

	-- Get the id nodes (from roads) of the target and farm locations within a given distance
	INSERT INTO "route_node_ids" (target_, farm_, node_target_, node_farm_)
		SELECT id_target3 AS target_, id_building3 AS farm_,
			node_target3 AS node_target_, node_building3 AS node_farm_
		FROM jrc_03_get_node_arrays (id_target_, distance);

	-- Assign the ids to variables
	node_target_ := (SELECT a.node_target_ FROM route_node_ids AS a WHERE target_ = id_target_ LIMIT 1);
	id_building_ := (SELECT array_agg(farm_) FROM route_node_ids WHERE target_ = id_target_);
	node_building_ := (SELECT array_agg(node_farm_) FROM route_node_ids);

	n_farms := (SELECT COUNT (*) FROM route_node_ids);

	RAISE NOTICE 'target id / node  = % | % ', id_target_, node_target_;
	RAISE NOTICE 'number of farms = % ', n_farms;
	RAISE NOTICE 'farm id / node = % | % ', id_building_, node_building_;
	RAISE NOTICE '';

	-- Do the routing between the target (point) and the farms (array)

	INSERT INTO "route_distance_3_5km" (id_target, id_building, length, geom)
	SELECT id_target4, id_building4, length, geom FROM jrc_04_routes (id_target_, id_building_, node_target_, node_building_);

END LOOP;
END
$$;


    