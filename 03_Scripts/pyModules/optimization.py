import os
from variables import *
from pyModules.postGIS import *
from pg_queries import *


def Step_01_create_Residuals (allocation, residual):

    # _________________________ Residual resources
    sql_residual = """
    {create_table} AS
    SELECT a.id_aggregate, a.id_target, a.id_building, a.length, b.rank, a.plant_capacity, a.manure, a.crop_production, a.live_methane, a.crop_methane, a.methane_total, a.cost_harvest, a.cost_ensiling, a.cost_manure, a.cost_total
    FROM {cost} AS a
    LEFT JOIN {cost_aggr} AS b
    ON a.id_target = b.id_target
    ORDER BY  a.methane_total DESC, a.cost_total
    ;
    """.format (
        create_table = create_table(residual),
        cost = SQL_plant_costs['cost'].name,
        cost_aggr = SQL_plant_costs['cost_aggr'].name,
    )

    sql_custom (table=residual, sql=sql_residual)

    # _________________________ Allocated resources (copy table structure (empty))
    sql_allocation = """
        {create_table} AS
    		SELECT  * FROM {residual} WHERE 0 > 1
            ;
    """.format(
        create_table = create_table(allocation),
        residual = residual,
        )

    sql_custom (table=allocation, sql=sql_allocation)

def Step_02_initialize_Plant (location, links, plant_capacity):

    # ______________ copy table structure (empty)
    sql_plants = """
        {create_table} (
            id_order SERIAL PRIMARY KEY, id_target int, plant_capacity int, rank int,
            live_required double precision, live_aggr double precision, live_demand double precision,
            crop_required double precision, crop_aggr double precision, crop_demand double precision,
            total_required double precision,
            methane_available double precision, methane_demand double precision,
            live_methane_used double precision, crop_methane_used double precision, methane_used double precision,
            cost_harvest double precision, cost_ensiling double precision, cost_manure double precision, cost_total double precision
        );
    """.format(
        create_table = create_table(location),
        )

    sql_custom (table=location, sql=sql_plants)

    add_geometry (scheme = 'public', table = location, column = 'geom', srid = 3035, type_='POINT', dimension=2)

    # ______________ table to hold the links between target and farms
    sql_links = """
        {create_table} (
            id_aggregate int, id_target int, id_building int, plant_capacity int, length double precision,
            manure_used double precision, live_methane_used double precision,
            crop_used double precision, crop_methane_used double precision,
            methane_used double precision,
            cost_harvest double precision, cost_ensiling double precision, cost_manure double precision, cost_total double precision
        );
    """.format(create_table = create_table(links))
    sql_custom (table=links, sql=sql_links)

def Step_03_aggregate_Resources (residual, residual_aggr, plant_capacity, minimum_value):

    sql_cost = """
        {create_table} AS
        WITH
        requirements AS (
            SELECT
            {minimum_value} AS  total_required,
            {minimum_value} * {manure_ratio} AS live_required,
            {minimum_value} * {crop_ratio} AS crop_required
        ),
        total AS (
            SELECT id_target,
                SUM (live_methane) AS live_aggr,
                SUM (crop_methane) AS crop_aggr
            FROM {residual}
            GROUP BY id_target
            ORDER BY id_target
        ),
        demand AS (
            SELECT a.id_target, b.*, a.live_aggr, a.crop_aggr,
                CASE
                    -- manure not reach the minimin ratio amount
                    WHEN live_aggr > live_required THEN  live_required
                    ELSE live_aggr
                END AS live_demand,
                CASE
                    -- manure not reach the minimin ratio amount
                    WHEN crop_aggr > crop_required AND live_aggr > live_required THEN  crop_required
                    WHEN crop_aggr > crop_required AND live_aggr < live_required AND crop_aggr + live_aggr > total_required THEN  crop_required + (live_required - live_aggr)
                    ELSE 0
                END AS crop_demand
        FROM total AS a, requirements AS b
        ),
        aggregation AS (
            SELECT
                id_target,
                SUM (methane_total) AS methane_available,
                SUM (cost_harvest) AS cost_harvest_aggr,
                SUM (cost_ensiling) AS cost_ensiling_aggr,
                SUM (cost_manure) AS cost_manure_aggr,
                SUM (cost_total) AS cost_total_aggr
            FROM {residual}
            GROUP BY id_target
            ORDER BY id_target
        )
        SELECT
            a.id_target, {plant_capacity} AS plant_capacity, c.rank,
            a.live_required, a.live_aggr, a.live_demand,
            a.crop_required, a.crop_aggr, a.crop_demand,
            a.total_required, b.methane_available,  a.live_demand + a.crop_demand AS methane_demand,
            b.cost_harvest_aggr, b.cost_ensiling_aggr, b.cost_manure_aggr, b.cost_total_aggr,
            c.geom
        FROM demand AS a
        LEFT JOIN aggregation AS b ON a.id_target = b.id_target
        LEFT JOIN {target} AS c ON a.id_target = c.id_target
        WHERE a.id_target = b.id_target AND a.id_target = c.id_target
        ORDER BY rank DESC, cost_total_aggr ASC
            ;
    """.format (
        create_table = create_table(residual_aggr),
        residual =residual,
        target = SQL_target['site_clean'].name,
        plant_capacity = plant_capacity,
        manure_ratio = SQL_methane_ratio['manure'],
        crop_ratio = SQL_methane_ratio['crop'],
        minimum_value = minimum_value,
        )

    sql_custom (table=residual_aggr, sql=sql_cost)

def Step_04_select_Plant (links, location, residual_aggr, plant_capacity, rank, minimum_value):

    global n_plants, n_rank, select_fist_plant, first_plant, found_plant

    if select_fist_plant:
        first_plant = "AND a.id_target = {0}".format(first_plant)
        select_fist_plant = False
    else:
        first_plant = ""

    # _________________ check if it is the first running time
    sql_select = """


		SELECT *
        FROM {residual_aggr}
        WHERE methane_demand >= {minimum_value} AND cost_total_aggr > 0  AND rank = {rank}
        {first_plant}
        ORDER BY cost_total_aggr ASC
        LIMIT 1
            ;
        """.format(
            residual_aggr = residual_aggr,
            minimum_value = minimum_value,
            rank = rank,
            first_plant = first_plant,
            location = location,
            )

    sql_custom (table="", sql=sql_select)

    # _________________get the remaining number of plants
    n_plants = db_PostGIS['cursor'].rowcount

    if n_plants > 0:

        info ("found plant ")
        found_plant = True

        sql_insert = """
            INSERT INTO {location} (
                id_target, plant_capacity, rank,
                live_required, live_aggr, live_demand,
                crop_required, crop_aggr, crop_demand,
                total_required, methane_available, methane_demand,
                cost_harvest, cost_ensiling, cost_manure, cost_total,
                geom
                )

    		{sql_select}
                ;
        """.format(
            location = location,
            sql_select = sql_select,
            )

        sql_custom (table=location, sql=sql_insert)

    else:
        debug ("no more plants for the rank {0}".format(n_rank))
        n_rank -= 1
        found_plant = False

def Step_05_select_Farms (links, location, residual, plant_capacity, minimum_value):

    global found_plant

    sql_links = """
        WITH
        last_record AS (
            SELECT id_target, live_required, crop_required
            FROM {location}
            ORDER BY id_order DESC
            LIMIT 1
        ),
        live_columns AS (
            -- it get the last row of the sequence of farms
            -- this is necessary to grab the next value of the query, not retrieve without it
            SELECT live_row,
                -- calculate if the available methane from livestock reachs the minimum amount, otherwise grep the remaining from crops
                CASE
                    WHEN COALESCE(live_required,0) - COALESCE(live_methane_aggregated,0) > 0 THEN COALESCE(live_required,0) - COALESCE(live_methane_aggregated,0)
                    ELSE 0
                    END AS livestock_demand
            FROM (
                SELECT b.live_required,
                SUM (a.live_methane) OVER (PARTITION BY a.id_target ORDER BY a.length ASC) AS live_methane_aggregated,
                row_number () OVER (ORDER BY a.length ASC) AS live_row
                FROM {residual} AS a, last_record AS b
                WHERE a.id_target = b.id_target AND a.live_methane > 0
                ) AS f
            WHERE live_methane_aggregated <= (live_required)
            ORDER BY  live_row DESC
            LIMIT 1
        ),
        crop_columns AS (
            -- it get the last row of the sequence of farms
            -- this is necessary to grab the next value of the query, not retrieve without it
            SELECT crop_row
            FROM (
                SELECT b.crop_required,
                SUM (a.crop_methane) OVER (PARTITION BY a.id_target ORDER BY a.length ASC) AS crop_methane_aggregated,
                row_number () OVER (ORDER BY a.length ASC) AS crop_row
                FROM {residual} AS a, last_record AS b
                WHERE a.id_target = b.id_target AND a.crop_production > 0
                ) AS f, live_columns AS g
            WHERE crop_methane_aggregated <= (crop_required) + g.livestock_demand -- this is necessary for the situation where manure does not reach the minimum required amount
            ORDER BY  crop_row DESC
            LIMIT 1
        ),
        manure AS (
            SELECT *
            FROM (
                SELECT a.id_aggregate, a.manure, a.live_methane, b.live_required,
                SUM (a.live_methane) OVER (PARTITION BY a.id_target ORDER BY a.length ASC) AS live_methane_aggregated,
                row_number () OVER (ORDER BY a.length ASC) AS live_row
                FROM {residual} AS a, last_record AS b
                WHERE a.id_target = b.id_target AND a.live_methane > 0
                ) AS f, live_columns AS g
            WHERE f.live_row <= g.live_row + 1 -- grab the next value of the sequence
        ),
        crop AS (
            SELECT *
            FROM (
                SELECT a.id_aggregate, a.id_building, a.id_target, a.length, a.crop_production, a.crop_methane, b.crop_required,
                SUM (a.crop_methane) OVER (PARTITION BY a.id_target ORDER BY a.length ASC) AS crop_methane_aggregated,
                row_number () OVER (ORDER BY a.length ASC) AS crop_row
                FROM {residual} AS a, last_record AS b
                WHERE a.id_target = b.id_target AND a.crop_production > 0
                ) AS f, crop_columns AS g
            WHERE f.crop_row <= g.crop_row + 1 -- grab the next value of the sequence
        ),
        cost_total AS (
            SELECT a.id_aggregate, a.cost_harvest, a.cost_ensiling, a.cost_manure, a.cost_total
            FROM {residual} AS a, crop AS b
            WHERE a.id_aggregate = b.id_aggregate
        )
        INSERT INTO {links} (
            id_aggregate, id_target, id_building, length,
            manure_used, crop_used, live_methane_used, crop_methane_used, methane_used, cost_harvest, cost_ensiling, cost_manure, cost_total
            )
        SELECT
            p.id_aggregate, p.id_target, p.id_building, p.length,
            r.manure AS manure_used, p.crop_production AS crop_used,
            r.live_methane AS live_methane_used, p.crop_methane AS crop_methane_used,
            COALESCE(r.live_methane,0) + COALESCE(p.crop_methane,0) AS methane_used,
            s.cost_harvest, s.cost_ensiling, s.cost_manure, s.cost_total
        FROM crop AS p
        LEFT JOIN manure AS r ON p.id_aggregate = r.id_aggregate
        LEFT JOIN cost_total AS s ON p.id_aggregate = s.id_aggregate

        ;
    """.format (
        links = links,
        location = location,
        residual = residual,
        plant_capacity = plant_capacity,
        minimum_value = minimum_value,
        manure_ratio = SQL_methane_ratio['manure'],
        crop_ratio = SQL_methane_ratio['crop'],
        )

    if found_plant:
        sql_custom (table=links, sql=sql_links)

def Step_06_update_Plant_Capacity (location, links):

    global found_plant

    sql_update = """
        WITH
        aggretate AS (
            SELECT id_target,
            SUM (live_methane_used) AS live_methane_used,
            SUM (crop_methane_used) AS crop_methane_used,
            SUM (methane_used) AS methane_used,
            SUM (cost_harvest) AS cost_harvest,
            SUM (cost_ensiling) AS cost_ensiling,
            SUM (cost_manure) AS cost_manure,
            SUM (cost_total) AS cost_total
            FROM {links}
            GROUP BY id_target
            ORDER BY id_target
        )
        UPDATE {location} AS a
        SET live_methane_used = b.live_methane_used,
            crop_methane_used = b.crop_methane_used,
            methane_used = b.methane_used,
            cost_harvest = b.cost_harvest,
            cost_ensiling = b.cost_ensiling,
            cost_manure = b.cost_manure,
            cost_total = b.cost_total
        FROM aggretate AS b
        WHERE a.id_target = b.id_target
        ;
    """.format (
        location = location,
        links = links,
        )

    if found_plant:
        sql_custom (table=location, sql=sql_update)

def Step_07_update_Residuals (allocation, residual, links):

    global found_plant

    cost_harvest = "(crop_production * {0})".format(SQL_costs['harvest'])
    cost_ensiling = "((length / 1000) * crop_production * {0})".format(SQL_costs['ensiling'])
    cost_manure = "((length / 1000) * manure * {0})".format(SQL_costs['manure'])

    # __________________________ update allocation
    sql_allocation = """
        INSERT INTO {allocation}
        SELECT a.*
        FROM {residual} AS a, {links} AS b
        WHERE a.id_building = b.id_building AND a.id_target = b.id_target
        ;
    """.format (
        residual = residual,
        allocation = allocation,
        links = links,
        )

    # __________________________ update residuals
    sql_residual= """
        WITH
        costs AS
        (
            SELECT *,
            {harvest} AS cost_harvest,
            {ensiling} AS cost_ensiling,
            {manure} AS cost_manure
            FROM {allocation}
        )
        UPDATE {residual} AS a
        SET
            manure = a.manure - b.manure,
            crop_production = a.crop_production - b.crop_production,
            live_methane = a.live_methane - b.live_methane,
            crop_methane = a.crop_methane - b.crop_methane,
            methane_total = a.methane_total - b.methane_total,
            cost_harvest = a.cost_harvest,
            cost_ensiling = a.cost_ensiling,
            cost_manure = a.cost_manure,
            cost_total = COALESCE(a.cost_harvest,0) + COALESCE(a.cost_ensiling,0) +  COALESCE(a.cost_manure,0)
        FROM {links} AS b
        WHERE a.id_building = b.id_building
        ;
    """.format (
        residual = residual,
        links = allocation,
        harvest = cost_harvest,
        ensiling = cost_ensiling,
        manure = cost_manure,
        allocation = allocation,
        )



    # __________________________ remove empty data from residuals
    sql_remove = """
        DELETE FROM {residual}
        WHERE methane_total = 0 or methane_total is null
        ;
    """.format (residual = residual)


    if found_plant:
        sql_custom (table=allocation, sql=sql_allocation)
        sql_custom (table=residual, sql=sql_residual)
        sql_custom (table=residual, sql=sql_remove)


def Step_08_map_Route_Plants (map_routes, location, links):

    sql_map = """
        {create_table} AS
        SELECT a.*, b.geom AS farms, c.geom AS route
        FROM {links} AS a
        LEFT JOIN {farms} AS b  ON a.id_building = b.id_building
        LEFT JOIN {route} AS c  ON a.id_building = c.id_building AND a.id_target = c.id_target
        WHERE a.id_building = b.id_building AND a.id_building = c.id_building
        ;
    """.format (
        create_table = create_table(map_routes),
        location = location,
        links = links,
        farms = SQL_farms['biomass'].name,
        route = SQL_route_distance['250'].name,
        )

    sql_custom (table=map_routes, sql=sql_map)


    for distance in ['500', '750', '1500', '2000']:

        sql_merge= """
            INSERT INTO {map_routes} (
                id_aggregate, id_target, id_building, plant_capacity, length,
                manure_used, crop_used, live_methane_used, crop_methane_used, methane_used, cost_harvest, cost_ensiling, cost_manure, cost_total,
                farms, route)
            SELECT a.*, b.geom AS farms, c.geom AS route
            FROM {links} AS a
            LEFT JOIN {farms} AS b  ON a.id_building = b.id_building
            LEFT JOIN {route} AS c  ON a.id_building = c.id_building AND a.id_target = c.id_target
            WHERE a.id_building = b.id_building AND a.id_building = c.id_building
            ;
        """.format (
                map_routes = map_routes,
                location = location,
                links = links,
                farms = SQL_farms['biomass'].name,
                route = SQL_route_distance[distance].name,
                )

        sql_custom (table = map_routes, sql=sql_merge)

def pause_script (count):

    if count <0 :
        programPause = raw_input("Press the <ENTER> key to continue...")


def extract_plants_by_capacity ():

    global n_plants, n_rank, select_fist_plant, first_plant, found_plant

    for plant_capacity in [100, 1]:
    # for plant_capacity in [750, 500]:

        if plant_capacity == 1:
            exit()
#
        select_fist_plant = False
        found_plant = False
        first_plant = 113
        count = 0
        n_plants = 1
        n_rank = 3

        cost_column = "cost_total"
        methane_column = "methane_total"
        minimum_value = SQL_plant_capacity[str(plant_capacity)]

        allocation = "{0}_{1}kw".format(SQL_optmization['allocation'].name, plant_capacity)
        residual = "{0}_{1}kw".format(SQL_optmization['residual'].name, plant_capacity)
        location = "{0}_{1}kw".format(SQL_optmization['location'].name, plant_capacity)
        links = "{0}_{1}kw".format(SQL_optmization['links'].name, plant_capacity)
        residual_aggr = "{0}_{1}kw".format(SQL_optmization['residual_aggr'].name, plant_capacity)
        map_routes = "{0}_{1}kw".format(SQL_optmization['map_routes'].name, plant_capacity)
        plant_costs = "{0}_{1}kw".format(SQL_plant_costs['cost'].name, plant_capacity)


        Step_01_create_Residuals(allocation, residual)

        pause_script (count)

        Step_02_initialize_Plant(location, links, plant_capacity)

        pause_script(count)

        while n_rank > 0:

            Step_03_aggregate_Resources(residual, residual_aggr, plant_capacity, minimum_value)

            pause_script(count)

            Step_04_select_Plant(links, location, residual_aggr, plant_capacity, n_rank, minimum_value)

            pause_script(count)

            Step_05_select_Farms(links, location, residual, plant_capacity, minimum_value)

            pause_script(count)

            Step_06_update_Plant_Capacity (location, links)

            pause_script(count)

            Step_07_update_Residuals(allocation, residual, links)

            pause_script(count)


            count += 1

            debug ("plant capacity: {0} \t iteration: {1} \t rank: {2}".format(plant_capacity, count, n_rank))

            if count >= 100:
                exit()
                break

        Step_08_map_Route_Plants(map_routes, location, links)
