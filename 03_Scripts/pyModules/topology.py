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


    points_table = SQL_topology['targets'].name
    nodes_table = SQL_topology['noded_ver'].name


    # ________________________ Function 1

    columns_base = "a.id as id_node, b.{0}, ST_Distance(a.the_geom, b.geom) as dist"
    columns_farm = columns_base.format('id_building')
    columns_target = columns_base.format('id_target')

    criteria = "ST_DWithin (a.the_geom, b.geom, 500) \n\t\tORDER BY dist ASC limit 1"

    function_nodes = "jrc_get_ids_from_nodes"
    columns_nodes = "id_target_ int, id_farm_ int"
    return_nodes = "table (id_target int, id_building int, node_target bigint, node_farm bigint, dist double precision)"
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
        table_2 = nodes_table,
        columns_1 = columns_farm,
        columns_2 = columns_target,
        criteria = criteria,
    )

    # ________________________ Function 2
    function_ids = "jrc_get_farm_ids"
    columns_ids  = "id_target_ int, distance int"
    return_ids  = "table (id_building int)"
    sql_ids = """
    	SELECT DISTINCT a.id_building
    	FROM (Select * from {table_1} where id_building > 0) as a
    	JOIN (Select * from {table_1} where id_target = $1) as b
    	ON ST_DWithin (a.geom, b.geom, $2)

    """.format(
        table_1 = points_table,
        table_2 = nodes_table
    )

    # ________________________ Function 3
    function_array= "jrc_get_ids_arrays"
    columns_array  = "id_target_ int, distance int"
    return_array  = "table (id_target2 int, id_building2 int, node_building2 int)"

    sql_declare = "n_buildings int[]; i integer;"

    sql_array = """
    	n_buildings := ((SELECT array_agg(id_building) FROM jrc_get_farm_ids (
    		(SELECT id_target FROM {table_1} WHERE id_target = $1), $2))::int[]);

    	FOR i IN 1 .. array_upper(n_buildings, 1)
    	LOOP
    		id_building2 := n_buildings[i];
    		id_target2 := $1;

    		node_building2 := (select node_farm from jrc_get_ids_from_nodes (
    	  			(SELECT id_target FROM {table_1} WHERE id_target =  $1),
    				(SELECT id_building FROM {table_1} WHERE id_building =  n_buildings[i]))) ;

    	  		RAISE NOTICE 'The id is : % and the node is : % s', n_buildings[i] , node_building2;

    		RETURN NEXT;
    	END LOOP;
    """.format(table_1 = points_table)

    # ________________________ EXECUTE QUERIES
    sql_create_SQL_function(name=function_nodes, columns=columns_nodes, return_=return_nodes, sql=sql_nodes)
    sql_create_SQL_function(name=function_ids, columns=columns_ids, return_=return_ids, sql=sql_ids)
    sql_create_PLPGSQL_function(name=function_array, columns=columns_array, return_=return_array, declare=sql_declare, sql=sql_array)


def Step_06_extract_Routes ():

    print ""

    start = get_time()

    # ________________________ Table
    points_table = SQL_topology['targets'].name
    noded_table = SQL_topology['noded'].name
    vertice_table = SQL_topology['noded_ver'].name

    # ________________________ Create output table
    route_table = ("{0}_{1}m".format(SQL_topology['route'].name, SQL_distances['max_travel'] )).replace('000m', 'km')
    columns = "id_building int, id_target int, cost double precision, distance double precision, length double precision"

    sql_route = """
        {create} AS
        WITH
            target AS
            (
                SELECT 	a.id AS id_node, b.id_target, ST_Distance(a.the_geom, b.geom) AS dist
                FROM 	{table_3} AS a,
                    (SELECT id_target, geom FROM {table_1} where id_target <= 2) AS b
                WHERE 	ST_DWithin (a.the_geom, b.geom, 500)
                ORDER BY dist ASC
                LIMIT 1
            ),
            farm AS
            (
                SELECT array_agg(node_building2) AS id_node FROM jrc_get_ids_arrays (1, {distance})
            ),
            dijkstra as (
                SELECT
                    dijkstra.*, topo_roads_noded.geom
                FROM	pgr_dijkstra(
                        'SELECT id, source::integer, target::integer, distance::double precision AS cost FROM topo_roads_noded',
                        (SELECT id_node FROM target), (SELECT id_node FROM farm),false) AS dijkstra
                LEFT JOIN
                    {table_2}
                ON
                    (edge = id)
                ORDER BY
                    seq
            ),
            join_ids AS (
                SELECT b.id_building2 AS id_building, b.id_target2 AS id_target, a.*
                FROM dijkstra a
                LEFT JOIN (SELECT * FROM jrc_get_ids_arrays (1, {distance})) b
                ON (a.end_vid = b.node_building2)
            ),
        	routes AS (
        		SELECT id_building, ST_Multi(ST_LineMerge(ST_Collect(geom))) AS geom
                FROM join_ids
        		GROUP BY id_building
        	),
        	final AS (
        		SELECT a.id_building, a.geom, b.id_mun
                FROM routes a
                LEFT JOIN {table_1} b
                ON (a.id_building = b.id_building)
                ORDER BY a.id_building
        	)
            SELECT *
            FROM final   ;
    """.format(
        create = create_table(route_table),
        table_1 = points_table,
        table_2 = noded_table,
        table_3 = vertice_table,
        distance = SQL_distances['max_travel']
    )

    end = get_time()
    debug ("Total time = {0}".format(get_total_time(start,end)))

    print sql_route
    sql_custom (table = route_table,  sql=sql_route)

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
