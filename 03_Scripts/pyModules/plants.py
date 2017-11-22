from variables import *
from pyModules.postGIS import *
from pg_queries import *

def Step_01_initiate_Plants ():

    # route distance process was split in several tables in order to avoid errors

    columns = """
        id_mun int, id_target int, id_building int, length double precision,
        heads double precision, lsu double precision, manure double precision, live_methane double precision,
        crop_area double precision, crop_production double precision, crop_methane double precision
    """

    sql_results= "{create} ({columns});".format (
        create = create_table(SQL_plants['initial'].name), columns=columns )

    sql_custom (table = "", sql=sql_results)

    routes = ['250', '500', '750', '1500', '2000']

    for key in routes:

        route = "route_distance_50km_{0}__".format(key)

        sql_merge= """
            INSERT INTO {plants} (id_mun, id_target, id_building, length, heads, lsu, manure, live_methane, crop_area, crop_production, crop_methane)
            SELECT NULL AS id_mun, id_target, id_building, length, NULL AS heads, NULL AS lsu, NULL AS manure, NULL AS live_methane, NULL AS crop_area, NULL AS crop_production, NULL AS crop_methane
            FROM {route};
        """.format (
                plants = SQL_plants['initial'].name,
                route = route
                )

        sql_custom (table = SQL_plants['initial'].name, sql=sql_merge)

def Step_02_join_Farm_Resources ():

    sql_join = """
        {create_table} AS
        SELECT b.id_mun, a.id_target, a.id_building, a.length, b.heads, b.lsu, b.manure, b.live_methane, b.crop_area, b.crop_production, b.crop_methane
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
    # ______ select resources until reach the plant capacity

    sql_sum = """
        {create_table} AS
        WITH
            aggregation AS
            (
                SELECT *
                FROM (
                    SELECT *,
                    -- crop methane aggregated by distance
                    SUM (crop_methane) OVER (PARTITION BY id_target ORDER BY length ASC) AS crop_capacity_aggr,
                    -- livestock methane aggregated by distance < 10 km
                    CASE
                        WHEN (length <= {distance} AND live_methane is not Null) THEN
                            SUM (coalesce (live_methane,0)) OVER (PARTITION BY id_target ORDER BY length ASC)
                        ELSE 0
                    END AS live_capacity_aggr
                    FROM {resources}
                ) AS capacities
            ),
            sum AS
            (
                SELECT a.*, a.crop_capacity_aggr + a.live_capacity_aggr AS total_capacity_aggr
                FROM aggregation AS a
            ),
            capacity AS
            (
                SELECT *,
                CASE
                    WHEN total_capacity_aggr <= {capacity1} THEN 100
                    WHEN total_capacity_aggr <= {capacity2} THEN 250
                    WHEN total_capacity_aggr <= {capacity3} THEN 500
                    WHEN total_capacity_aggr <= {capacity4} THEN 750
                    ELSE Null
                END AS plant_capacity
                FROM sum
            )
            SELECT * FROM capacity
            WHERE plant_capacity is not Null
            ORDER BY id_target, total_capacity_aggr , live_capacity_aggr, crop_capacity_aggr , plant_capacity
            ;
    """.format (
        create_table = create_table(SQL_plants['capacity'].name),
        resources = SQL_plants['resources'].name,
        distance = SQL_distances['manure'],
        capacity1 = SQL_plant_capacity['100'],
        capacity2 = SQL_plant_capacity['250'],
        capacity3 = SQL_plant_capacity['500'],
        capacity4 = SQL_plant_capacity['750'],
        )

    sql_custom (table=SQL_plants['capacity'].name, sql=sql_sum)

    add_column (table = SQL_plants['capacity'].name, column = 'id_aggregate SERIAL PRIMARY KEY')

def Step_04_calculate_Costs ():

    cost_harvest = "(crop_production * {0})".format(SQL_costs['harvest'])
    cost_ensiling = "((length / 1000) * crop_production * {0})".format(SQL_costs['ensiling'])
    cost_manure = "((length / 1000) * manure * {0})".format(SQL_costs['manure'])

    for key in SQL_plant_capacity:

        plant_capacity = int(key)

        sql_cost = """
            {create_table} AS
            WITH
            costs AS
            (
                SELECT *,
                {harvest} AS cost_harvest,
                {ensiling} AS cost_ensiling,
                CASE
                    WHEN live_capacity_aggr > 0 THEN {manure}
                    ELSE 0
                END AS cost_manure
                FROM {capacity}
                WHERE plant_capacity <= {plant_capacity}
            )
            SELECT *,
            CASE
                WHEN cost_manure is not null THEN cost_manure + cost_harvest + cost_ensiling
                ELSE cost_harvest + cost_ensiling
            END AS cost_{key}kw,
            COALESCE (live_methane,0) + crop_methane AS methane_{key}kw
            FROM costs
                ;
        """.format (
            create_table = create_table(SQL_plant_costs[key].name),
            capacity = SQL_plants['capacity'].name,
            harvest = cost_harvest,
            ensiling = cost_ensiling,
            manure = cost_manure,
            plant_capacity=plant_capacity,
            key=key
            )

        sql_custom (table=SQL_plant_costs[key].name, sql=sql_cost)

def Step_05_join_Costs ():

    sql_costs = """
        {create_table} AS
        SELECT a.id_aggregate, a.id_target, a.id_building, a.length, a.plant_capacity,
            a.manure, a.crop_production, a.live_methane, a.crop_methane,
            d.methane_100kw, c.methane_250kw,
            b.methane_500kw, a.methane_750kw,
            d.cost_100kw, c.cost_250kw,
            b.cost_500kw, a.cost_750kw
        FROM {cost_750kw} AS a
        LEFT JOIN {cost_500kw} AS b ON a.id_aggregate = b.id_aggregate
        LEFT JOIN {cost_250kw} AS c ON a.id_aggregate = c.id_aggregate
        LEFT JOIN {cost_100kw} AS d ON a.id_aggregate = d.id_aggregate


            ;
    """.format (
        create_table = create_table(SQL_plant_costs['cost'].name),
        cost_100kw = SQL_plant_costs['100'].name,
        cost_250kw = SQL_plant_costs['250'].name,
        cost_500kw = SQL_plant_costs['500'].name,
        cost_750kw = SQL_plant_costs['750'].name,
        )

    sql_custom (table=SQL_plant_costs['cost'].name, sql=sql_costs)

def Step_05_aggregate_Costs ():

    sql_aggr = """
        {create_table} AS
        WITH
            p100kw AS (
                SELECT id_target,
                SUM (cost_100kw) AS cost_100kw,
                SUM (methane_100kw) AS methane_100kw
                FROM {cost_100kw}
                GROUP BY id_target
                ORDER BY id_target
            ),
            p250kw AS (
                SELECT id_target,
                SUM (cost_250kw) AS cost_250kw,
                SUM (methane_250kw) AS methane_250kw
                FROM {cost_250kw}
                GROUP BY id_target
                ORDER BY id_target
            ),
            p500kw AS (
                SELECT id_target,
                SUM (cost_500kw) AS cost_500kw,
                SUM (methane_500kw) AS methane_500kw
                FROM {cost_500kw}
                GROUP BY id_target
                ORDER BY id_target
            ),
            p750kw AS (
                SELECT id_target,
                SUM (cost_750kw) AS cost_750kw,
                SUM (methane_750kw) AS methane_750kw
                FROM {cost_750kw}
                GROUP BY id_target
                ORDER BY id_target
            )
        SELECT a.id_target, a.rank,
        b.methane_100kw, c.methane_250kw, d.methane_500kw, e.methane_750kw,
        b.cost_100kw, c.cost_250kw, d.cost_500kw, e.cost_750kw, a.geom
        FROM {target}  AS a
        LEFT JOIN p100kw AS b ON a.id_target = b.id_target
        LEFT JOIN p250kw AS c ON a.id_target = c.id_target
        LEFT JOIN p500kw AS d ON a.id_target = d.id_target
        LEFT JOIN p750kw AS e ON a.id_target = e.id_target
            ;
    """.format (
        create_table = create_table(SQL_plant_costs['cost_total'].name),
        target = SQL_target['site_clean'].name,
        cost_100kw = SQL_plant_costs['100'].name,
        cost_250kw = SQL_plant_costs['250'].name,
        cost_500kw = SQL_plant_costs['500'].name,
        cost_750kw = SQL_plant_costs['750'].name,
        )

    sql_custom (table=SQL_plant_costs['cost_total'].name, sql=sql_aggr)

def Step_09_test_Route_Plants ():

    plant_id = 102

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
        cost = SQL_plant_costs['cost_total'].name,
        targets = SQL_target['site_clean'].name,
        )

    sql_custom (table='', sql=sql_target)
