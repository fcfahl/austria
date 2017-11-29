from variables import *
from pyModules.postGIS import *
from pg_queries import *

def Step_01_initiate_Plants ():

    # route distance process was split in several tables in order to avoid errors

    columns = """
        id_target int, id_building int, length double precision,
        manure double precision, live_methane double precision,
        crop_production double precision, crop_methane double precision
    """

    sql_results= "{create} ({columns});".format (
        create = create_table(SQL_plants['initial'].name), columns=columns )

    sql_custom (table = "", sql=sql_results)

    routes = ['250', '500', '750', '1500', '2000']

    for key in routes:

        route = "route_distance_50km_{0}__".format(key)

        sql_merge= """
            INSERT INTO {plants} (id_target, id_building, length, manure, live_methane, crop_production, crop_methane)
            SELECT  id_target, id_building, length, NULL AS manure, NULL AS live_methane, NULL AS crop_production, NULL AS crop_methane
            FROM {route};
        """.format (
                plants = SQL_plants['initial'].name,
                route = route
                )

        sql_custom (table = SQL_plants['initial'].name, sql=sql_merge)

def Step_02_join_Farm_Resources ():

    sql_join = """
        {create_table} AS
        SELECT a.id_target, a.id_building, a.length, b.manure, b.crop_production, b.live_methane, b.crop_methane
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


def Step_03_calculate_Costs ():

    cost_harvest = "(COALESCE(crop_production,0) * {0})".format(SQL_costs['harvest'])
    cost_ensiling = "((length / 1000) * COALESCE(crop_production,0) * {0})".format(SQL_costs['ensiling'])
    cost_manure = "((length / 1000) * COALESCE(manure,0)  * {0}) + ({1} * COALESCE(manure,0))".format(SQL_costs['manure_fixed'], SQL_costs['manure'])

    sql_cost = """
        {create_table} AS
        WITH
        costs AS
        (
            SELECT *,
            {harvest} AS cost_harvest,
            {ensiling} AS cost_ensiling,
            {manure} AS cost_manure
            FROM {capacity}
        )
        SELECT *,
        CASE
            WHEN cost_manure is not null THEN cost_manure + cost_harvest + cost_ensiling
            ELSE cost_harvest + cost_ensiling
        END AS cost_total
        FROM costs
            ;
    """.format (
        create_table = create_table(SQL_plant_costs['cost'].name),
        capacity = SQL_plants['resources'].name,
        harvest = cost_harvest,
        ensiling = cost_ensiling,
        manure = cost_manure,
        )

    sql_custom (table=SQL_plant_costs['cost'].name, sql=sql_cost)

def Step_04_aggregate_Costs ():

    sql_aggr = """
        {create_table} AS
        WITH
        aggregated AS (
            SELECT id_target,
            SUM (manure) AS total_manure,
            SUM (crop_production) AS total_crop,
            SUM (cost_harvest) AS cost_harvest,
            SUM (cost_ensiling) AS cost_ensiling,
            SUM (cost_manure) AS cost_manure,
            SUM (cost_total) AS cost_total
            FROM {cost_total}
            GROUP BY id_target
            ORDER BY id_target
        )
        SELECT a.id_target, b.rank, a.total_manure, a.total_crop, a.cost_harvest, a.cost_ensiling, a.cost_manure, a.cost_total, b.geom
        FROM aggregated AS a
        LEFT JOIN {target} AS b ON a.id_target = b.id_target
        ORDER BY  b.rank DESC, a.total_crop DESC, a.cost_total ASC
            ;
    """.format (
        create_table = create_table(SQL_plant_costs['cost_aggr'].name),
        cost_total = SQL_plant_costs['cost'].name,
        target = SQL_target['site_clean'].name,
        )

    sql_custom (table=SQL_plant_costs['cost_aggr'].name, sql=sql_aggr)

def Step_06_test_Route_Plants ():

    plant_id = 113

    if plant_id <= 250:
        route = SQL_route_distance['250'].name
    elif plant_id <= 500:
        route = SQL_route_distance['500'].name
    elif plant_id <= 750:
        route = SQL_route_distance['750'].name
    elif plant_id <= 1500:
        route = SQL_route_distance['1500'].name
    else:
        route = SQL_route_distance['2000'].name

    sql_test = """
        {create_table} AS
        SELECT a.*, b.geom as farms, c.geom as route
        FROM {cost} AS a
        LEFT JOIN {farms} AS b ON a.id_building = b.id_building
        LEFT JOIN {route} AS c ON a.id_building = c.id_building
        WHERE a.id_target = {plant}
        AND c.id_target = {plant}
            ;
    """.format (
        create_table = create_table('test_plants_route'),
        plant = plant_id,
        targets = SQL_target['site_clean'].name,
        farms = SQL_farms['biomass'].name,
        capacity = SQL_plants['capacity'].name,
        cost = SQL_plant_costs['cost'].name,
        route = route,
        )

    sql_custom (table='', sql=sql_test)


    sql_target = """
        {create_table} AS
        SELECT *
        FROM {cost}
        WHERE id_target = {plant}
            ;
    """.format (
        create_table = create_table('test_plants_target'),
        plant = plant_id,
        cost = SQL_plant_costs['cost_aggr'].name,
        targets = SQL_target['site_clean'].name,
        )

    sql_custom (table='', sql=sql_target)
