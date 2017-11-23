DROP TABLE IF EXISTS route_distance_50km_500__;
CREATE TABLE route_distance_50km_500__ (id_target int, id_building int, length double precision );
SELECT AddGeometryColumn ('public', 'route_distance_50km_500__','geom', 3035, 'multilinestring', 2);

DROP TABLE IF EXISTS route_targets_500__;
CREATE TABLE route_targets_500__ AS
SELECT id_target, geom
FROM topo_targets
WHERE id_target > 250 and id_target <= 500;

DROP TABLE IF EXISTS route_node_ids_500__;
CREATE TABLE route_node_ids_500__ (target_ int, farm_ int, node_target_ int, node_farm_ int );
    
	
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
    	-- Get number of target points
    	n_targets := (SELECT array_agg(id_target) FROM route_targets_500__);

    	-- Perform the Loop
    	FOR i IN 1 .. array_upper(n_targets, 1)
    	LOOP

    		id_target_ := n_targets[i];

    		-- Get the id nodes (from roads) of the target and farm locations within a given distance
    		INSERT INTO "route_node_ids_500__" (target_, farm_, node_target_, node_farm_)
    			SELECT id_target3 AS target_, id_building3 AS farm_,
    				node_target3 AS node_target_, node_building3 AS node_farm_
    			FROM jrc_03_get_node_arrays (id_target_, distance);

    		-- Assign the ids to variables
    	 	node_target_ := (SELECT a.node_target_ FROM route_node_ids_500__ AS a WHERE target_ = id_target_ LIMIT 1);
    		id_building_ := (SELECT array_agg(farm_) FROM route_node_ids_500__ WHERE target_ = id_target_);
    		node_building_ := (SELECT array_agg(node_farm_) FROM route_node_ids_500__ WHERE target_ = id_target_);

    		n_farms := (SELECT COUNT (*) FROM route_node_ids_500__  WHERE target_ = id_target_);

    		RAISE NOTICE 'target id  = % ', id_target_;
    		RAISE NOTICE 'target node  = % ', node_target_;
    		RAISE NOTICE 'number of farms = % ', n_farms;
    		RAISE NOTICE '____________________________';
    		RAISE NOTICE 'farm id  % ', id_building_;
    		RAISE NOTICE '____________________________';
    		RAISE NOTICE 'farm node = % ', node_building_;
    		RAISE NOTICE '____________________________';
    		RAISE NOTICE '';

    		-- Do the routing between the target (point) and the farms (array)

    		INSERT INTO "route_distance_50km_500__" (id_target, id_building, length, geom)
    		SELECT id_target4, id_building4, length4, geom FROM jrc_04_routes (id_target_, id_building_, node_target_, node_building_);

    	END LOOP;
    END
    $$;
