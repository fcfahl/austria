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

    routes = ['250', '500', '750', '1000', '1250', '1500']

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
        SELECT a.id_target, a.id_building, a.length, c.rank, b.manure, b.crop_production, b.live_methane, b.crop_methane
        FROM {initial} AS a
        LEFT JOIN {farms} AS b ON a.id_building = b.id_building
        LEFT JOIN {target} AS c ON a.id_target = c.id_target
        ORDER BY a.id_target
        ;
    """.format (
            create_table = create_table(SQL_plants['resources'].name),
            initial = SQL_plants['initial'].name,
            farms = SQL_farms['biomass'].name,
            target = SQL_target['site_clean'].name,
            )

    sql_custom (table=SQL_plants['resources'].name, sql=sql_join)
    drop_table (SQL_plants['initial'].name)


def Step_03_aggregate_Resources ():

    # for key in SQL_methane_capacity:
    for key in ['750', '500', '250', '100']:

        if key != '1':

            resources_aggr = "{0}_{1}kw".format(SQL_plants['resources_aggr'].name, key)

            sql_resources = """
                {create_table} AS
                WITH
                parameters AS (
                    SELECT
                    {manure_demand} AS manure_required,
                    {crop_demand} AS crop_required,
                    {methane_demand} AS methane_required
                ),
                manure_available AS (
                    SELECT id_target,
                        SUM (manure) AS manure_available
                    FROM {resources}
                    WHERE length <= {manure_distance}
                    GROUP BY id_target
                    ORDER BY id_target
                ),
                crop_available AS (
                    SELECT id_target,
                        SUM (crop_production) AS crop_available
                    FROM {resources}
                    WHERE length <= {crop_distance}
                    GROUP BY id_target
                    ORDER BY id_target
                ),
                required AS (
                    SELECT a.id_target, a.manure_available, c.manure_required,
            		    CASE
                			WHEN a.manure_available > c.manure_required THEN c.manure_required
                			ELSE a.manure_available
                        END AS manure_used,
                        a.manure_available - c.manure_required AS manure_residual,
                        b.crop_available,
                        c.crop_required,
                        b.crop_available - c.crop_required AS crop_demand
                    FROM manure_available AS a, crop_available AS b, parameters AS c
                    WHERE a.id_target = b.id_target
                ),
                manure_methane AS (
                    SELECT id_target, manure_available, manure_required, manure_used, manure_residual,
                        manure_used * 14.4 AS manure_methane_produced,
                        CASE
                			WHEN manure_residual < 0 THEN (manure_available * 14.4 - manure_required * 14.4) * -1
                			ELSE 0
                        END AS manure_methane_residual
                    FROM required
                ),
                crop_methane_missing AS (
                    SELECT a.id_target, b.crop_available, b.crop_required,
                		a.manure_methane_residual * 14.4 AS methane_lacking_from_manure,
                		a.manure_methane_residual / 125.4 AS crop_additional
                    FROM manure_methane AS a, required AS b
                    WHERE a.id_target = b.id_target
                ),
                crop_methane AS (
                    SELECT a.*,
                		b.crop_available, b.crop_additional, b.crop_required,
                		b.crop_required + b.crop_additional AS crop_used
                    FROM manure_methane AS a, crop_methane_missing AS B
                    WHERE a.id_target = b.id_target
                ),
                total_methane AS (
                    SELECT *,
                		manure_used * 14.4 AS methane_from_manure,
                		crop_used * 125.4 AS methane_from_crop,
                		manure_used * 14.4 + crop_used * 125.4 AS methane_total_produced
                    FROM crop_methane
                )
                SELECT *
                FROM total_methane
                    ;
            """.format (
                create_table = create_table(resources_aggr),
                resources = SQL_plants['resources'].name,
                manure_demand = SQL_manure_demand[key],
                crop_demand = SQL_crop_demand[key],
                methane_demand = SQL_methane_capacity[key],
                manure_yield = SQL_methane_yield['manure'],
                crop_yield = SQL_methane_yield['crop'],
                manure_distance = SQL_distances['manure'],
                crop_distance = SQL_distances['max_travel'],
                )

            sql_custom (table=resources_aggr, sql=sql_resources)


def Step_04_calculate_Costs ():

    manure='COALESCE(manure_used,0)'
    crop='COALESCE(crop_used,0)'
    distance='length / 1000'
    harvest=SQL_costs['harvest']
    ensiling=SQL_costs['ensiling']
    km=SQL_costs['manure']
    fixed=SQL_costs['manure_fixed']

    cost_harvest = "({crop} * {harvest})".format(crop=crop, harvest=harvest)
    cost_ensiling = "({crop} * {ensiling} * {distance})".format(crop=crop, ensiling=ensiling, distance=distance)
    cost_manure = "({manure} * ({fixed} + ({km}  * ({distance}))) )".format(manure=manure, fixed=fixed, km=km, distance=distance)

    # for key in SQL_methane_capacity:
    for key in ['750', '500', '250', '100']:

        if key != '1':

            cost = "{0}_{1}kw".format(SQL_plant_costs['cost'].name, key)
            resources_aggr = "{0}_{1}kw".format(SQL_plants['resources_aggr'].name, key)

            sql_cost = """
            {create_table} AS
                WITH
                limits AS
                (
                    SELECT id_target, manure_used, crop_used
                    FROM {resources_aggr}
                ),
                manure_columns AS (
                    -- it get the last row of the sequence of farms
                    -- this is necessary to grab the next value of the query, not retrieve without it
                    SELECT id_target, max(manure_row_1) as manure_row_1
                    FROM (
                        SELECT a.id_target, a.manure, a.length, b.manure_used,
                        SUM (a.manure) OVER (PARTITION BY a.id_target ORDER BY a.length ASC) AS manure_aggregated,
                        row_number () OVER (ORDER BY a.id_target, a.length ASC) AS manure_row_1
                        FROM {resources} AS a, limits AS b
                        WHERE a.id_target = b.id_target and (a.length < {manure_distance})
                        ) AS f
                    WHERE manure_aggregated  <= manure_used
                    GROUP BY id_target
                    ORDER BY id_target
                ),
                manure AS (
                    SELECT f.id_target,  f.id_building, f.length, f.rank, f.manure, f.crop_production, f.live_methane, f.crop_methane
                    FROM (
                        SELECT a.*,
                        row_number () OVER (ORDER BY a.id_target, a.length ASC) AS manure_row
                        FROM {resources} AS a, limits AS b
                        WHERE a.id_target = b.id_target and (a.length < {manure_distance})
                        ) AS f, manure_columns AS g
                    WHERE f.id_target = g.id_target AND f.manure > 0 AND f.manure_row <= g.manure_row_1 + 1-- grab the next value of the sequence
                ),
                crop_columns AS (
                    -- it get the last row of the sequence of farms
                    -- this is necessary to grab the next value of the query, not retrieve without it
                    SELECT id_target, max(crop_row_1) as crop_row_1
                    FROM (
                        SELECT a.id_target, a.crop_production, a.length, b.crop_used,
                        SUM (a.crop_production) OVER (PARTITION BY a.id_target ORDER BY a.length ASC) AS crop_aggregated,
                        row_number () OVER (ORDER BY a.id_target, a.length ASC) AS crop_row_1
                        FROM {resources} AS a, limits AS b
                        WHERE a.id_target = b.id_target and (a.length < {crop_distance})
                        ) AS f
                    WHERE crop_aggregated  <= crop_used
                    GROUP BY id_target
                    ORDER BY id_target
                ),
                crop AS (
                    SELECT f.id_target, f.id_building, f.length, f.rank, f.manure, f.crop_production, f.live_methane, f.crop_methane
                    FROM (
                        SELECT a.*,
                        row_number () OVER (ORDER BY a.id_target, a.length ASC) AS crop_row
                        FROM {resources} AS a, limits AS b
                        WHERE a.id_target = b.id_target and (a.length < {crop_distance})
                        ) AS f, crop_columns AS g
                    WHERE f.id_target = g.id_target AND f.crop_production > 0 AND f.crop_row <= g.crop_row_1 + 1-- grab the next value of the sequence
                ),
                join_farms AS (
                    SELECT
                        b.id_target, b.id_building, b.length, b.rank,
                        a.manure AS manure_used, b.crop_production AS crop_used,
                        a.live_methane, b.crop_methane,
                        COALESCE(a.live_methane,0) + COALESCE(b.crop_methane,0) AS methane_total
                    FROM crop AS b
                    LEFT JOIN manure AS a ON a.id_target = b.id_target AND a.id_building = b.id_building

                ),
                costs AS (
                    SELECT a.id_target, a.id_building, a.length, a.rank,
                        {cost_harvest} AS cost_harvest,
                        {cost_ensiling} AS cost_ensiling,
                        {cost_manure} AS cost_manure
                    FROM join_farms AS a
                ),
                cost_total AS (
                    SELECT  a.*, b.cost_harvest, b.cost_ensiling, b.cost_manure, COALESCE(b.cost_harvest,0) + COALESCE(b.cost_ensiling,0) + COALESCE(b.cost_manure,0) AS  cost_total
                    FROM plants_resources AS a
                    LEFT JOIN costs AS b ON a.id_target = b.id_target AND a.id_building = b.id_building
                    WHERE a.id_target = b.id_target AND a.id_building = b.id_building
                )
                SELECT * FROM cost_total
                    ;
            """.format (
                create_table = create_table(cost),
                resources = SQL_plants['resources'].name,
                resources_aggr = resources_aggr,
                cost_harvest = cost_harvest,
                cost_ensiling = cost_ensiling,
                cost_manure = cost_manure,
                manure_distance = SQL_distances['manure'],
                crop_distance = SQL_distances['max_travel'],
                )

            sql_custom (table=cost, sql=sql_cost)

def Step_05_aggregate_Costs ():

    # for key in SQL_methane_capacity:
    for key in ['750', '500', '250', '100']:

        if key != '1':

            cost = "{0}_{1}kw".format(SQL_plant_costs['cost'].name, key)
            cost_aggr = "{0}_{1}kw".format(SQL_plant_costs['cost_aggr'].name, key)

            sql_aggr = """
                {create_table} AS
                    SELECT id_target,
            			SUM(manure) AS manure_used,
            			AVG(length) AS length_avg,
                        MAX(rank) AS rank,
            			SUM(crop_production) AS crop_used,
            			SUM(live_methane) AS live_methane_used,
            			SUM(crop_methane) AS crop_methane_used,
            			SUM(cost_harvest) AS cost_harvest,
            			SUM(cost_ensiling) AS cost_ensiling,
            			SUM(cost_manure) AS cost_manure,
            			SUM(cost_total) AS cost_total
                    FROM {cost}
                    GROUP BY id_target
                    ORDER BY rank DESC, cost_total
                    ;
            """.format (
                create_table = create_table(cost_aggr),
                cost = cost
                )

            sql_custom (table=cost_aggr, sql=sql_aggr)

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
