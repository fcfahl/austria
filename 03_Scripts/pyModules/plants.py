from variables import *
from pyModules.postGIS import *
from pg_queries import *

def Step_01_merge_Routes ():

    # route distance process was split in several tables in order to avoid errors

    columns = """
        id_mun int, id_target int, id_building int, length double precision,
        heads double precision, lsu double precision, manure double precision, life_methane double precision,
        crop_area double precision, crop_production double precision, crop_methane double precision
    """

    sql_results= "{create} ({columns});".format (
        create = create_table(SQL_plants['initial'].name), columns=columns )

    sql_custom (table = "", sql=sql_results)
    add_geometry (scheme = 'public', table = SQL_plants['initial'].name, column = 'geom', srid = 3035, type_='POINT', dimension=2)

    routes = ['250', '500', '750', '1500', '2000']

    for key in routes:

        route = "route_distance_50km_{0}__".format(key)

        # ______________ select points based on lulc zones
        sql_merge= """
            INSERT INTO {plants} (id_mun, id_target, id_building, length, heads, lsu, manure, life_methane, crop_area, crop_production, crop_methane, geom)
            SELECT NULL AS id_mun, id_target, id_building, length, NULL AS heads, NULL AS lsu, NULL AS manure, NULL AS life_methane, NULL AS crop_area, NULL AS crop_production, NULL AS crop_methane, NULL AS geom
            FROM {route};
        """.format (
                plants = SQL_plants['initial'].name,
                route = route
                )

        sql_custom (table = SQL_plants['initial'].name, sql=sql_merge)


def Step_02_join_Farm_Resources ():

    sql_join = """
        {create_table} AS
        SELECT b.id_mun, a.id_target, a.id_building, a.length, b.heads, b.lsu, b.manure, b.life_methane, b.crop_area, b.crop_production, b.crop_methane, a.geom
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
    drop_table (SQL_plants['initial'].name)

def Step_03_calculate_Methane ():

    sql_methane = """
        {create_table} AS
        SELECT id_plants, id_mun, id_target, id_building, length, manure, life_methane, crop_production, crop_methane,
        CASE
            WHEN length <= {distance} THEN (life_methane * {manure}) + (crop_methane * {crop})
            ELSE (crop_methane * {crop})
        END AS methane
        FROM {resources}
        ORDER BY id_target, length ASC;
    """.format (
            create_table = create_table(SQL_plants['methane'].name),
            resources = SQL_plants['resources'].name,
            manure = SQL_methane_ratio['manure'],
            crop = SQL_methane_ratio['crop'],
            distance = SQL_distances['manure']
            )

    sql_custom (table=SQL_plants['methane'].name, sql=sql_methane)
    add_column (table = SQL_plants['methane'].name, column = 'id_plants SERIAL PRIMARY KEY')

def Step_04_sum_Resources ():

    # ______ select resources until reach the plant capacity

    sql_sum = """
        {create_table} AS
        SELECT *
        FROM (
            SELECT *,
            SUM (methane) OVER (PARTITION BY id_target ORDER BY length ASC) AS capacity
            FROM {resources}
        ) AS capacity
        WHERE capacity <= {capacity}
            ;
    """.format (
        create_table = create_table(SQL_plants['capacity'].name),
        resources = SQL_plants['methane'].name,
        capacity = SQL_plant_capacity['250'],
        )

    sql_custom (table=SQL_plants['capacity'].name, sql=sql_sum)

def Step_05_calculate_Costs ():

    cost_harvest = "(crop_production * {0})".format(SQL_costs['harvest'])
    cost_ensiling = "((length / 1000) * manure * {0})".format(SQL_costs['ensiling'])

    sql_cost = """
        {create_table} AS
        SELECT *, {harvest} AS cost_harvest, {ensiling} AS cost_ensiling, {harvest} + {ensiling} as cost_total
        FROM {capacity}
            ;
    """.format (
        create_table = create_table(SQL_plants['cost'].name),
        capacity = SQL_plants['capacity'].name,
        harvest = cost_harvest,
        ensiling = cost_ensiling
        )

    sql_custom (table=SQL_plants['cost'].name, sql=sql_cost)


def Step_06_aggregate_Costs ():

    sql_aggr = """
        {create_table} AS
        WITH
            total AS (
                SELECT a.id_target,
                SUM (a.cost_harvest) AS cost_harvest, SUM (a.cost_ensiling) AS cost_ensiling, SUM (a.cost_total) AS cost_total
                FROM {cost} a
                GROUP BY a.id_target
                ORDER BY a.id_target
            )
        SELECT a.*, b.geom
        FROM total AS a, {targets} AS b
        WHERE a.id_target = b.id_target
            ;
    """.format (
        create_table = create_table(SQL_plants['cost_total'].name),
        cost = SQL_plants['cost'].name,
        targets = SQL_topology['targets'].name
        )

    sql_custom (table=SQL_plants['cost_total'].name, sql=sql_aggr)

    # sql_sum = """
    #     {create_table} AS
    #     SELECT id_plants, id_mun, id_target, id_building, length, capacity
    #     FROM (
    #         SELECT id_plants, id_mun, id_target, id_building, length,
    #         SUM (methane) OVER (ORDER BY length ASC) AS capacity
    #         FROM {resources}
    #     ) AS capacity
    #     WHERE capacity <= {capacity}
    #         ;
    # """.format (
    #     create_table = create_table(SQL_plants['capacity'].name),
    #     resources = SQL_plants['methane'].name,
    #     capacity = SQL_plant_capacity['250'],
    #     )
    #
    # sql_custom (table=SQL_plants['capacity'].name, sql=sql_sum)




    # sql_sum = """
    #     {create_table} AS
    #     SELECT a.id_target, sum(a.methane) AS total_methane, b.geom
    #         FROM {resources} AS a
    #         LEFT JOIN (SELECT DISTINCT ON (id_target) * FROM {targets})  AS b
    #         ON a.id_target = b.id_target
    #         GROUP BY a.id_target, b.geom
    #         ORDER BY a.id_target
    #         ;
    # """.format (
    #     create_table = create_table(SQL_plants['capacity'].name),
    #     resources = SQL_plants['methane'].name,
    #     targets = SQL_topology['targets'].name
    #     )
    #
    # sql_custom (table=SQL_plants['capacity'].name, sql=sql_sum)




    # sql_sum = """
    #     WITH
    #         selection AS (
    #             SELECT a.id_target, a.length, a.lsu, a.manure, a.methane, a.crop, a.total_area, a.geom
    #                  FROM plants_resources AS a
    #                  LEFT JOIN topo_targets AS b
    #                  ON a.id_target = b.id_target
    #                  ORDER BY a.id_target, a.length
    #             ),
    #         manure AS (
    #             SELECT b.id_target, sum(b.lsu) AS lsu, sum(b.manure) AS manure, sum(b.methane) AS methane, sum(b.length) AS manure_distance
    #             FROM (SELECT * FROM  selection WHERE length < {manure_distance}) AS b
    #             GROUP BY b.id_target
    #            ),
    #         crop AS (
    #             SELECT b.id_target, sum(b.crop) AS crop, sum(b.total_area) as total_area, sum(b.length) AS crop_distance
    #             FROM selection AS b
    #             GROUP BY b.id_target
    #            ),
    #         join_tables AS (
    #             SELECT m.id_target, m.lsu, m.manure, m.methane, c.crop, c.total_area, m.manure_distance, c.crop_distance
    #             FROM manure AS m
    #             LEFT JOIN crop AS c
    #             ON m.id_target = c.id_target
    #            )
    #     SELECT
    #         a.*, s.geom
    #     FROM
    #         join_tables AS a
    #     LEFT JOIN (SELECT DISTINCT ON (id_target) * FROM selection) AS s
    #     ON a.id_target = s.id_target
    # """.format (
    #     resources = SQL_plants['methane'].name,
    #     targets = SQL_topology['targets'].name,
    #     manure_distance = SQL_distances['manure'])
    #
    # sql_create_table_with (
    #     table = SQL_plants['grouped'].name,
    #     with_ = sql_sum,
    #     where = ""
    #     )
    #
    #
    # add_column (table = SQL_plants['grouped'].name, column = 'id_plants SERIAL PRIMARY KEY')
