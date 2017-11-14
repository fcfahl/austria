from pyModules.variables import *
from postGIS import *
from pg_queries import *
from logs import *

def Step_01_create_Topo_Nodes():

    sql_merge= """
        {create} AS
        SELECT id_mun, 'target' AS node, Null AS id_building, id_target,  geom FROM {target}
            UNION ALL
        SELECT id_mun, 'farm' AS node, id_building, Null AS id_target,  geom FROM {farms}
    """.format (
            create = create_table(SQL_topology['targets'].name),
            target = SQL_target['site_clean'].name,
            farms = SQL_buildings['location'].name
            )

    sql_custom (table = SQL_topology['targets'].name, sql=sql_merge)
    add_column (table = SQL_topology['targets'].name, column = 'id_type SERIAL PRIMARY KEY')

def Step_02_segmentize_OSM_Roads ():

    # ______________ create points over the roads every 250 m
    #https://gis.stackexchange.com/questions/64898/split-all-osm-roads-within-boundingbox-in-20m-segments-and-save-to-new-table

    sql_columns = "value, oneway, junction, surface, maxspeed"

    sql_segmentize = """
        {create} AS
        SELECT {id} AS id_road, 0 as length, {columns}, ST_MakeLine(start_point,end_point) AS geom
        FROM
        (
            SELECT
                ST_Pointn(geom, generate_series(1, ST_NumPoints(geom)-1)) as start_point,
                ST_Pointn(geom, generate_series(2, ST_NumPoints(geom))) as end_point,
                {id},
                {columns}
            FROM (
                SELECT {id}, {columns}, ST_Segmentize(geom,{distance}) AS geom
                FROM {table}
                -- WHERE {id} > 35000 -- to be deleted
                ) as line
        ) as a;
    """.format(
            create = create_table(SQL_topology['roads'].name),
            table = SQL_roads['osm'].name,
            id = 'id',
            columns = sql_columns,
            distance = SQL_distances['osm']
        )

    sql_custom (table=SQL_topology['roads'].name, sql=sql_segmentize)
    update_column (table = SQL_topology['roads'].name, column='length', value='ST_Length(geom)')

    # ______________ clean lines with no length
    delete_records (table=SQL_topology['roads'].name, where="length = 0")

def Step_03_create_Road_Topology ():

    # ______________ drop tables
    drop_table (table = SQL_topology['noded'].name)
    drop_table (table = SQL_topology['roads_ver'].name)
    drop_table (table = SQL_topology['roads_ver'].name)

    # ______________ add columns for topology analysis
    drop_column (table = SQL_topology['roads'].name, column = 'id_road')
    add_column (table = SQL_topology['roads'].name, column = 'id_road SERIAL PRIMARY KEY')
    add_column (table = SQL_topology['roads'].name, column = 'source INT4')
    add_column (table = SQL_topology['roads'].name, column = 'target INT4')

    # ______________ create topology
    pgr_topology (table=SQL_topology['roads'].name, tolerance=SQL_distances['tolerance'], id='id_road')
    pgr_nodeNetwork (table=SQL_topology['roads'].name, tolerance=SQL_distances['tolerance'], id='id_road')
    pgr_topology (table=SQL_topology['noded'].name, tolerance=SQL_distances['tolerance'], id='id')


    # pgr_analyzeGraph (table=SQL_topology['roads'].name, tolerance=SQL_distances['tolerance'], id='id_road')
    # pgr_analyzeGraph (table=SQL_topology['noded'].name, tolerance=SQL_distances['tolerance'], id='id')

def Step_04_update_Topology ():

    add_column (table = SQL_topology['noded'].name, column = 'type VARCHAR')
    add_column (table = SQL_topology['noded'].name, column = 'oneway VARCHAR')
    add_column (table = SQL_topology['noded'].name, column = 'surface VARCHAR')
    add_column (table = SQL_topology['noded'].name, column = 'distance FLOAT8')
    add_column (table = SQL_topology['noded'].name, column = 'time FLOAT8')

    sql_attr= """
        UPDATE {table} a
        SET type = b.value,
            oneway = b.oneway,
            surface = b.surface
        FROM {edge} b
        WHERE a.id = b.id_road;
    """.format (
            table = SQL_topology['noded'].name,
            edge = SQL_topology['roads'].name
            )

    sql_dist= """
        UPDATE {table}
        SET {column} = ST_Length(geom) / 1000;
    """.format (
            table = SQL_topology['noded'].name,
            column = 'distance'
            )

    sql_time= """
        UPDATE {table}
        SET {column} =
        CASE {criteria}
            WHEN 'steps' THEN -1
            WHEN 'path' THEN -1
            WHEN 'footway' THEN -1
            WHEN 'cycleway' THEN -1
            WHEN 'proposed' THEN -1
            WHEN 'construction' THEN -1
            WHEN 'raceway' THEN distance / 100
            WHEN 'motorway' THEN distance / 70
            WHEN 'motorway_link' THEN distance / 70
            WHEN 'trunk' THEN distance / 60
            WHEN 'trunk_link' THEN distance / 60
            WHEN 'primary' THEN distance / 55
            WHEN 'primary_link' THEN distance / 55
            WHEN 'secondary' THEN distance / 45
            WHEN 'secondary_link' THEN distance / 45
            WHEN 'tertiary' THEN distance / 45
            WHEN 'tertiary_link' THEN distance / 40
            WHEN 'unclassified' THEN distance / 35
            WHEN 'residential' THEN distance / 30
            WHEN 'living_street' THEN distance / 30
            WHEN 'service' THEN distance / 30
            WHEN 'track' THEN distance / 20
            ELSE distance / 20
        END;""".format (
            table = SQL_topology['noded'].name,
            column = 'time',
            criteria = 'type'
            )

    sql_custom (table=SQL_topology['noded'].name, sql=sql_attr)
    sql_custom (table=SQL_topology['noded'].name, sql=sql_dist)
    sql_custom (table=SQL_topology['noded'].name, sql=sql_time)

def Step_05_create_PG_Functions ():

    number_features = SQL_distances['features'] # just to test the results: use > 0 to get all the results

    points_table = SQL_topology['targets'].name
    nodes_table = SQL_topology['noded'].name
    nodes_vertices = SQL_topology['noded_ver'].name
    route_table = SQL_route['route'].name
    route_targets = SQL_route['targets'].name
    route_nodes = "{0}_{1}__".format(SQL_route['nodes'].name, number_features)

    # ________________________ Function 1
    function_ids = "jrc_01_get_farm_ids"
    columns_ids  = "id_target_ int, distance int"
    return_ids  = "table (id_building1 int)"
    sql_ids = """
    	SELECT DISTINCT a.id_building
    	FROM (Select * from {table_1} where id_building > 0) as a
    	JOIN (Select * from {table_1} where id_target = $1) as b
    	ON ST_DWithin (a.geom, b.geom, $2)

    """.format(table_1 = points_table,  table_2 = nodes_vertices)


    # ________________________ Function 2

    columns_base = "a.id as id_node, b.{0}, ST_Distance(a.the_geom, b.geom) as dist"
    columns_farm = columns_base.format('id_building')
    columns_target = columns_base.format('id_target')

    criteria = "ST_DWithin (a.the_geom, b.geom, 500) \n\t\tORDER BY dist ASC limit 1"

    function_nodes = "jrc_02_get_road_node"
    columns_nodes = "id_target_ int, id_farm_ int"
    return_nodes = "table (id_target2 int, id_building2 int, node_target2 bigint, node_farm2 bigint, dist2 double precision)"
    sql_nodes = """
    	WITH
    	farm as (
    		SELECT 	{columns_1}
    		FROM 	{table_2} AS a,
    			(Select * from {table_1} where id_building = $2) AS b
    		WHERE 	{criteria}
    	),
    	target as (
    		SELECT {columns_2}
    		FROM 	{table_2} AS a,
    			(Select * from {table_1} where id_target = $1) AS b
    		WHERE 	{criteria}
    	)
    	SELECT target.id_target, farm.id_building, target.id_node, farm.id_node, target.dist
    	FROM farm, target

    """.format(
        table_1 = points_table,
        table_2 = nodes_vertices,
        columns_1 = columns_farm,
        columns_2 = columns_target,
        criteria = criteria,
    )

    # ________________________ Function 3
    function_array= "jrc_03_get_node_arrays"
    columns_array  = "id_target_ int, distance int"
    return_array  = "table (id_target3 int, id_building3 int, node_target3 int, node_building3 int)"

    declare_array = "n_buildings int[]; i integer; distance_fix int := distance * 0.8; -- distance correction factor"

    sql_array = """
    	n_buildings := ((SELECT array_agg(id_building1) FROM jrc_01_get_farm_ids (
    		(SELECT id_target FROM {table_1} WHERE id_target = $1), distance_fix))::int[]);

    	FOR i IN 1 .. array_upper(n_buildings, 1)
    	LOOP
            id_target3 := $1;
    		id_building3 := n_buildings[i];

			node_target3 := (SELECT node_target2 FROM jrc_02_get_road_node (
					(SELECT id_target FROM {table_1} WHERE id_target =  $1),
					(SELECT id_building FROM {table_1} WHERE id_building =  n_buildings[i]))) ;


    		node_building3 := (select node_farm2 from jrc_02_get_road_node (
    	  			(SELECT id_target FROM {table_1} WHERE id_target =  $1),
    				(SELECT id_building FROM {table_1} WHERE id_building =  n_buildings[i]))) ;

    	  	-- RAISE NOTICE 'The id is : % and the node is : % s', n_buildings[i] , node_building2;

    		RETURN NEXT;
    	END LOOP;
    """.format(table_1 = points_table)

    # ________________________ Function 4
    function_route= "jrc_04_routes"
    columns_route  = "id_target_ int, id_building_ int[], node_target_ int,  node_farm_ int[]"
    return_route  = "table (id_target4 int, id_building4 int, length4 double precision, geom geometry)"

    declare_route = "n_buildings int[]; i integer;distance int := {0};".format(SQL_distances['max_travel'])

    sql_route = """
        RETURN QUERY
        WITH
            target_id_1 AS (SELECT id_target_ AS id_target_1),
            target_node_1 AS (SELECT node_target_ AS node_target_1),
            farm_id_array_1 AS (SELECT id_building_ AS id_farm_array_1),
            farm_node_array_1 AS (SELECT node_farm_ AS node_farm_array_1),

            dijkstra as (
            	SELECT
            	    $1 as dij_target, dijkstra.*, {nodes}.geom
            	FROM	pgr_dijkstra(
            		'SELECT id, source::integer, target::integer, distance::double precision AS cost FROM {nodes}',
            		$3, $4,false) AS dijkstra
            	LEFT JOIN
            	    {nodes}
            	ON
            	    (edge = id)
            	ORDER BY
            	    seq
            ),
            join_ids AS (
            	SELECT b.farm_, a.*
            	FROM dijkstra a
            	LEFT JOIN (SELECT * FROM {ids}) AS b
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
            	LEFT JOIN {targets} AS e
            	ON (d.farm_ = e.id_building)
            	ORDER BY d.farm_
            )
            SELECT id_target_ AS  id_target4, f.farm_, f.length, f.geom FROM merge_results AS f  WHERE  f.length <= distance ;

            --RAISE NOTICE 'target id / node  = % | % ', id_target_, node_target_;
            --RAISE NOTICE 'farm id / node = % | % ', id_building_, node_farm_;
            --RAISE NOTICE '';

    """.format(
        nodes = nodes_table,
        ids = route_nodes,
        targets = points_table
        )


    # ________________________ EXECUTE QUERIES
    sql_create_SQL_function(name=function_ids, columns=columns_ids, return_=return_ids, sql=sql_ids)
    sql_create_SQL_function(name=function_nodes, columns=columns_nodes, return_=return_nodes, sql=sql_nodes)
    sql_create_PLPGSQL_function(name=function_array, columns=columns_array, return_=return_array, declare=declare_array, sql=sql_array)
    sql_create_PLPGSQL_function(name=function_route, columns=columns_route, return_=return_route, declare=declare_route, sql=sql_route)

def Step_06_extract_Routes ():

    start = get_time()

    number_features = SQL_distances['features'] # just to test the results: use > 0 to get all the results


    # ________________________ Tables
    points_table = SQL_topology['targets'].name
    noded_table = SQL_topology['noded'].name
    nodes_vertices = SQL_topology['noded_ver'].name
    route_table = SQL_route['route'].name
    route_targets = SQL_route['targets'].name
    route_nodes = "{0}_{1}__".format(SQL_route['nodes'].name, number_features)

    route_table_name = "{0}_{1}m_{2}__".format(route_table, SQL_distances['max_travel'], number_features).replace('000m', 'km')
    route_target_name = "route_targets_{0}__".format(number_features)



    # ________________________ Step 01 Create route table (structure only)
    sql_step1= "{create} (id_target int, id_building int, length double precision );".format (
        create = create_table(route_table_name)  )

    sql_custom (table = "", sql=sql_step1)
    add_geometry (scheme = 'public', table = route_table_name, column = 'geom', srid = 3035, type_='multilinestring', dimension=2)

    # ________________________ Step 02 Create target points
    sql_create_table (
        table = route_target_name,
        select = 'id_target, geom',
        from_ = points_table,
        where =  SQL_distances['criteria']
    )

    # ________________________ Step 03 Create node points
    sql_step3= "{create} (target_ int, farm_ int, node_target_ int, node_farm_ int );".format (
        create = create_table(route_nodes)  )
    sql_custom (table = "", sql=sql_step3)


    # ________________________ Step 04 Loop all the target points
    declarations = """
    	i integer;
    	distance int := {distance};
    	n_targets int[];
    	id_target_ int;
    	node_target_ int;
    	id_building_ int[];
    	node_building_ int[];
    	n_farms int;
    	results RECORD;
    	geom_ geometry;
    """.format(distance = SQL_distances['max_travel'])

    sql_route= """
    DO
    $$
    DECLARE
        {declarations}
    BEGIN
    	-- Get number of target points
    	n_targets := (SELECT array_agg(id_target) FROM {targets});

    	-- Perform the Loop
    	FOR i IN 1 .. array_upper(n_targets, 1)
    	LOOP

    		id_target_ := n_targets[i];

    		-- Get the id nodes (from roads) of the target and farm locations within a given distance
    		INSERT INTO "{nodes}" (target_, farm_, node_target_, node_farm_)
    			SELECT id_target3 AS target_, id_building3 AS farm_,
    				node_target3 AS node_target_, node_building3 AS node_farm_
    			FROM jrc_03_get_node_arrays (id_target_, distance);

    		-- Assign the ids to variables
    	 	node_target_ := (SELECT a.node_target_ FROM {nodes} AS a WHERE target_ = id_target_ LIMIT 1);
    		id_building_ := (SELECT array_agg(farm_) FROM {nodes} WHERE target_ = id_target_);
    		node_building_ := (SELECT array_agg(node_farm_) FROM {nodes} WHERE target_ = id_target_);

    		n_farms := (SELECT COUNT (*) FROM {nodes}  WHERE target_ = id_target_);

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

    		INSERT INTO "{route}" (id_target, id_building, length, geom)
    		SELECT id_target4, id_building4, length4, geom FROM jrc_04_routes (id_target_, id_building_, node_target_, node_building_);

    	END LOOP;
    END
    $$;


    """.format(
        declarations=declarations,
        targets=route_target_name,
        nodes=route_nodes,
        route=route_table_name
    )

    sql_custom (table = route_table_name,  sql=sql_route)

    end = get_time()
    debug ("Total time step 6 = {0}".format(get_total_time(start,end)))

def Step_06_extract_Routes_DEPRECATED ():

    ####### TO SLOW

    start = get_time()

    # ________________________ Table
    target_table = SQL_topology['targets'].name
    noded_table = SQL_topology['noded'].name

    # ________________________ Create output table
    route_table = ("{0}_{1}m".format(SQL_topology['route'].name, SQL_distances['max_travel'] )).replace('000m', 'km')
    columns = "id_building int, id_target int, cost double precision, distance double precision, length double precision"

    sql_out= "{create} ({columns})".format (create = create_table(route_table), columns = columns)
    sql_custom (table = route_table,  sql=sql_out)
    add_geometry(scheme='public', table=route_table, column='geom', srid=3035, type_='multilinestring', dimension=2)

    # ________________________ TARGETS
    sql_target = """
        -- selet target nodes
        CREATE TEMPORARY TABLE IF NOT EXISTS targets AS (SELECT id_target FROM {table} WHERE {criteria});
    """.format(
        table = target_table,
        criteria = 'id_target <= 1'
    )

    # ________________________ Sub Queries
    sql_farm_IDs = "SELECT id_target FROM topo_targets WHERE id_target = loop_targets.id_target"

    sql_nodes_IDs = """

    		nodes as (select * from jrc_get_ids_from_nodes (
                -- get target id (from loop 1)
    			(SELECT id_target FROM {table} WHERE id_target =  loop_targets.id_target),
                -- get farm id (from loop 2)
    			(SELECT id_building FROM {table} WHERE id_building =  loop_farms.id_building)))
    """.format (table=target_table)

    sql_dijkstra = """
    		dijkstra AS (
                -- extract path with smallest distance between farm and target
                SELECT  b.*, {table}.geom
                FROM pgr_dijkstra (
                    -- dijkstra results
                    'SELECT id, source::integer, target::integer, distance::double precision AS cost FROM {table}',
                    -- target id
                     (SELECT node_target FROM nodes),
                     -- farm id
                     (SELECT node_farm FROM nodes), false
                     ) AS b
    			LEFT JOIN
    				{table}
    			ON
    				(edge = id)
    			ORDER BY
    				seq
            )
    """.format (table = noded_table)

    sql_line = """
        -- get the linestring from dijkstra
        line as (SELECT ST_Multi(ST_Collect(geom)) as geom FROM dijkstra)
    """

    sql_insert = """
            -- insert results into table for each pair of farm and target IDs
    		INSERT INTO {out_Table} (id_building, id_target, cost, distance, length, geom)
    		VALUES (
    			(SELECT id_building FROM nodes),
    			(SELECT id_target FROM nodes),
    			(SELECT sum(dist) FROM nodes),
    			(SELECT dist FROM nodes),
    			(SELECT ST_Length (geom) FROM line),
    			(SELECT geom FROM line)
            );
    """.format(out_Table = route_table)

    # ________________________ LOOPS

    sql_loop_01 = "FOR loop_targets IN SELECT id_target FROM targets"

    sql_loop_02 = """
        FOR loop_farms IN
	    SELECT a.id_building FROM (
    		SELECT * FROM jrc_get_farm_ids (
                ({farm_IDs}),
                 -- max travel distance
                {distance})
    		) as a
    """.format(farm_IDs = sql_farm_IDs,  distance=SQL_distances['max_travel'])

    # ________________________ Extract routes
    sql_main = """
    DO
    $$
    DECLARE
    	loop_targets record;
    	loop_farms record;
    BEGIN
        {targets}
        -- LOOP 1 (TARGET)
            {loop_01}
        LOOP
            -- LOOP 2 (FARM)
                {loop_02}
            LOOP
                WITH
                    {nodes_IDs},
                    {dijkstra},
                    {line}
                    {insert}
            END LOOP;
        END LOOP;
    END
    $$;

    """.format(
        targets = sql_target,
        loop_01 = sql_loop_01,
        loop_02 = sql_loop_02,
        nodes_IDs = sql_nodes_IDs,
        dijkstra  = sql_dijkstra,
        line  = sql_line,
        insert  = sql_insert
    )


    print sql_main
    sql_custom (table = route_table,  sql=sql_main)

    end = get_time()
    debug ("Total time = {0}".format(get_total_time(start,end)))
