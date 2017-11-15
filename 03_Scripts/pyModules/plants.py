from variables import *
from pyModules.postGIS import *
from pg_queries import *

def Step_01_merge_Routes ():

    # route distance process was split in several tables in order to avoid errors



    sql_results= "{create} (id_mun int, id_target int, id_building int, length double precision, head_density double precision, lsu double precision, manure double precision, methane double precision, crop double precision, total_area double precision);".format (
        create = create_table(SQL_plants['initial'].name)  )

    sql_custom (table = "", sql=sql_results)
    add_geometry (scheme = 'public', table = SQL_plants['initial'].name, column = 'geom', srid = 3035, type_='POINT', dimension=2)


    # routes = ['route_distance_50km_250_', 'route_distance_50km_500__', 'route_distance_50km_750__']
    routes = ['500', '750']

    for key in routes:

        route = "route_distance_50km_{0}__".format(key)

        # ______________ select points based on lulc zones
        sql_merge= """
            INSERT INTO {plants} (id_mun, id_target, id_building, length, head_density, lsu, manure, methane, crop, total_area, geom)
            SELECT NULL AS id_mun, id_target, id_building, length, NULL AS head_density, NULL AS lsu, NULL AS manure, NULL AS methane, NULL AS crop, NULL AS total_area, NULL AS geom
            FROM {route};
        """.format (
                plants = SQL_plants['initial'].name,
                route = route
                )

        sql_custom (table = SQL_plants['initial'].name, sql=sql_merge)

    add_column (table = SQL_plants['initial'].name, column = 'id_plants SERIAL PRIMARY KEY')


def Step_02_join_Farm_Resources ():

    sql_join = """
        {create_table} AS
        SELECT b.id_mun, a.id_target, a.id_building, a.length, b.head_density, b.total_lsu AS lsu, b.total_manure AS manure, b.total_methane AS methane, b.total_crop AS crop, b.total_area, b.geom
        FROM {initial} AS a
        LEFT JOIN {farms} AS b ON a.id_building = b.id_building
        ORDER BY a.id_target
        ;
    """.format (
            create_table = create_table(SQL_plants['resources'].name),
            initial = SQL_plants['initial'].name,
            farms = SQL_farms['biomass'].name
            )

    sql_custom (table=SQL_plants['resources'].name, sql=sql_join)
    add_column (table = SQL_plants['resources'].name, column = 'id_plants SERIAL PRIMARY KEY')


def Step_03_calculate_Target_Resources ():


    sql_sum = """
        WITH
            selection AS (
                SELECT a.id_target, a.length, a.lsu, a.manure, a.methane, a.crop, a.total_area, a.geom
                     FROM plants_resources AS a
                     LEFT JOIN topo_targets AS b
                     ON a.id_target = b.id_target
                     ORDER BY a.id_target, a.length
                ),
            manure AS (
                SELECT b.id_target, sum(b.lsu) AS lsu, sum(b.manure) AS manure, sum(b.methane) AS methane, sum(b.length) AS manure_distance
                FROM (SELECT * FROM  selection WHERE length < {manure_distance}) AS b
                GROUP BY b.id_target
               ),
            crop AS (
                SELECT b.id_target, sum(b.crop) AS crop, sum(b.total_area) as total_area, sum(b.length) AS crop_distance
                FROM selection AS b
                GROUP BY b.id_target
               ),
            join_tables AS (
                SELECT m.id_target, m.lsu, m.manure, m.methane, c.crop, c.total_area, m.manure_distance, c.crop_distance
                FROM manure AS m
                LEFT JOIN crop AS c
                ON m.id_target = c.id_target
               )
        SELECT
            a.*, s.geom
        FROM
            join_tables AS a
        LEFT JOIN (SELECT DISTINCT ON (id_target) * FROM selection) AS s
        ON a.id_target = s.id_target
    """.format (
        resources = SQL_plants['resources'].name,
        targets = SQL_topology['targets'].name,
        manure_distance = SQL_distances['manure'])

    sql_create_table_with (
        table = SQL_plants['grouped'].name,
        with_ = sql_sum,
        where = ""
        )


    add_column (table = SQL_plants['grouped'].name, column = 'id_plants SERIAL PRIMARY KEY')
