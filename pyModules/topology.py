from pyModules.variables import *
from postGIS import *
from pg_queries import *

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


def Step_05_do_Dijkstra ():

    sql_route = """
        {create} AS
        SELECT
            e.old_id,
            e.type,
            e.oneway,
            e.time AS time,
            e.distance AS distance,
            e.geom AS geom
        FROM
             pgr_dijkstra('SELECT id, source::integer, target::integer, time::double precision AS cost FROM {node}', 1, 10,false) AS r,
             {node} AS e
    """.format (
            create = create_table(SQL_topology['route'].name),
            node = SQL_topology['noded'].name
            )

    sql_custom (table = SQL_topology['route'].name, sql=sql_route)


    # add_column (table = SQL_topology['targets'].name, column = 'id_type SERIAL PRIMARY KEY')

# DROP TABLE IF EXISTS final_route;
# CREATE TABLE final_route AS
# SELECT
#   e.old_id,
#   e.name,
#   e.type,
#   e.oneway,
#   e.time AS time,
#   e.distance AS distance,
#   e.the_geom AS geom
# FROM
#   pgr_dijkstra('SELECT id, source::integer, target::integer, time::double precision AS cost FROM edges2_noded', 1, 10,false) AS r,
#   edges2_noded AS e
# WHERE r.node = e.id;
