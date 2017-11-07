from variables import *
from postGIS import *


def SQL_custom (sql):
    return "{0};".format (sql)

def drop_table (table):
    return "DROP TABLE IF EXISTS {0};".format (table)

def create_table (table):
    return "DROP TABLE IF EXISTS {0}; CREATE TABLE {0}".format (table)

def select_data (select):
    return "SELECT {0}".format (select)

def from_table (from_):
    return "FROM {0}".format (from_)

def filter_table (where):
    return "WHERE {0}".format (where)

def add_column (table, column):
    return "ALTER TABLE  {0} ADD {1};".format (table, column)

def update_column (table, column, value):
    return "UPDATE {0} SET {1} = {2};".format (table, column, value)

def create_topology (table, tolerance):
    return "SELECT pgr_createTopology(\'{0}\', {1}, 'geom');".format (table, tolerance)

def create_nodeNetwork (table, tolerance):
    return "SELECT pgr_nodeNetwork(\'{0}\', {1}, 'id', 'geom');".format (table, tolerance)

def sql_create_table (table, select, from_, where):
    return "{drop} {create} AS {select} {from_} {where};".format (
        drop = drop_table (table),
        create = create_table (table),
        select = select_data (select),
        from_ = from_table (from_),
        where = filter_table (where)
    )



drop_tables = (
    drop_table (table = SQL_tables['roads_main'].name) +
    drop_table (table = SQL_tables['roads_main_vertices_pgr'].name) +
    drop_table (table = SQL_tables['roads_main_noded'].name)
    )


roads_main = sql_create_table (
    table = SQL_tables['roads_main'].name,
    select = '*',
    from_ = SQL_tables['roads_main'].from_,
    where = "value = 'primary' OR value = 'secondary' OR value = 'tertiary'"
    )

roads_main_add_columns = (
    add_column (table = SQL_tables['roads_main'].name, column = 'source INT4') +
    add_column (table = SQL_tables['roads_main'].name, column = 'target INT4')
    )


roads_main_topology = (
    create_topology (table = SQL_tables['roads_main'].name, tolerance=0.001) +
    create_nodeNetwork (table = SQL_tables['roads_main'].name , tolerance=0.001) +
    create_topology (table = SQL_tables['roads_main_noded'].name, tolerance=0.001)
    )


roads_edge_add_columns = (
    add_column (table = SQL_tables['roads_main_noded'].name, column = 'name VARCHAR') +
    add_column (table = SQL_tables['roads_main_noded'].name, column = 'type VARCHAR') +
    add_column (table = SQL_tables['roads_main_noded'].name, column = 'oneway VARCHAR') +
    add_column (table = SQL_tables['roads_main_noded'].name, column = 'surface VARCHAR') +
    add_column (table = SQL_tables['roads_main_noded'].name, column = 'distance FLOAT8') +
    add_column (table = SQL_tables['roads_main_noded'].name, column = 'time FLOAT8')
    )


roads_edge_update_attr = SQL_custom("UPDATE {0} AS new \
        SET name = old.name, type = old.value, oneway = old.oneway, surface = old.surface \
        FROM {1} AS old \
        WHERE new.old_id = old.id;".format (
            SQL_tables['roads_main_noded'].name,
            SQL_tables['roads_main'].name
        ))


time_condition = """
    CASE type
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
    END
"""

distance_condition  = """
    CASE type
        WHEN 'steps' THEN -1
        WHEN 'path' THEN -1
        WHEN 'footway' THEN -1
        WHEN 'cycleway' THEN -1
        WHEN 'proposed' THEN -1
        WHEN 'construction' THEN -1
        ELSE distance
    END
"""

roads_edge_distance  = (
    update_column (table=SQL_tables['roads_main_noded'].name, column='distance', value='ST_Length(ST_Transform(geom, 4326)::geography) / 1000') +
    update_column (table=SQL_tables['roads_main_noded'].name, column='time', value=time_condition) +
    update_column (table=SQL_tables['roads_main_noded'].name, column='distance', value=distance_condition)
    )


pgr_dijkstra = ("pgr_dijkstra (\'SELECT id, source::integer, target::integer, time AS cost FROM {0}\', {1}, {2}, {3} )").format(
    SQL_tables['roads_main_noded'].name, '1', '10', True
    )

roads_route = SQL_custom(
    create_table (SQL_tables['roads_route'].name) + " AS " +
    select_data ('e.id, e.name, e.type, e.oneway, e.time AS time, e.distance AS distance, e.geom AS geom ') +
    from_table (pgr_dijkstra) + ' AS r, ' +
    SQL_tables['roads_main_noded'].name + ' AS e ' +
    filter_table ('r.node = e.id')
    )


# pgr_dijkstra = ("pgr_dijkstra (\'SELECT id, source::integer, target::integer, time::double precision AS cost FROM {0}\', {1}, {2}, {3} )").format(
#     SQL_tables['roads_main_noded'].name, '1', '10', True
#     )
#
#
# SELECT  *
# FROM pgr_dijkstra('SELECT id, source::integer, target::integer, time::double precision AS cost FROM farm_roads_edges_noded', 1, 10, false)
# ORDER BY seq;


# DROP TABLE IF EXISTS farm_roads_final_route;
# CREATE TABLE farm_roads_final_route AS
# SELECT
#   e.id,
#   e.name,
#   e.type,
#   e.oneway,
#   e.time AS time,
#   e.distance AS distance,
#   e.geom AS geom
# FROM
#   pgr_dijkstra('SELECT id, source::integer, target::integer, time::double precision AS cost FROM farm_roads_edges_noded', 1, 2,true) AS r,
#   farm_roads_edges_noded AS e
# WHERE r.node = e.id;
# #
#
#
#
# CREATE TABLE roads_route AS
# SELECT e.id, e.name, e.type, e.oneway, e.time AS time, e.distance AS distance, e.geom AS geom
# FROM pgr_dijkstra ('SELECT id, source::integer, target::integer, time::double precision AS cost FROM roads_main_noded', 1, 10, True ) AS r, roads_main_noded AS e
# WHERE r.node = e.id;
