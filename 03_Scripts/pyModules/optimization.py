import os
from variables import *
from pyModules.postGIS import *
from pg_queries import *


def Step_01_create_Residuals (allocation, residual):

    # _________________________ Residual resources
    sql_residual = """
    {create_table} AS
    SELECT a.id_target, a.id_building, a.length, a.manure, a.crop_production, a.live_methane, a.crop_methane,  a.live_methane + a.crop_methane AS methane_total, a.cost_harvest, a.cost_ensiling, a.cost_manure, a.cost_total, 0 AS only_manure
    FROM {cost} AS a
    ;
    """.format (
        create_table = create_table(residual),
        cost = SQL_plant_costs['cost'].name,
    )

    sql_custom (table=residual, sql=sql_residual)
    add_column (table=residual, column='id_aggregate SERIAL PRIMARY KEY')

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
            manure_required double precision, manure_available double precision, manure_allocated double precision,
            crop_required double precision, crop_available double precision, crop_allocated double precision,
            total_required double precision,
            live_methane_used double precision, crop_methane_used double precision, total_methane_used double precision,
            cost_harvest double precision, cost_ensiling double precision, cost_manure double precision, cost_total double precision, manure_ratio double precision, crop_ratio double precision
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
            manure_used double precision, crop_used double precision,
            cost_harvest double precision, cost_ensiling double precision, cost_manure double precision, cost_total double precision, only_manure int
        );
    """.format(create_table = create_table(links))
    sql_custom (table=links, sql=sql_links)

def Step_03_aggregate_Resources (residual, residual_aggr, plant_capacity):

    key = str(plant_capacity)

    sql_resources = """
        {create_table} AS
        WITH
        parameters AS (
            SELECT
            {manure_demand} AS manure_required,
            {crop_demand} AS crop_required,
            {methane_demand} AS methane_required
        ),
        available AS (
            SELECT id_target,
                SUM (manure) AS manure_available,
                SUM (crop_production) AS crop_available
            FROM {residual}
            GROUP BY id_target
            ORDER BY id_target
        ),
        required AS (
            SELECT a.id_target, a.manure_available, b.manure_required,
    		    CASE
        			WHEN a.manure_available > b.manure_required THEN b.manure_required
        			ELSE a.manure_available
                END AS manure_used,
                a.manure_available - b.manure_required AS manure_residual,
                a.crop_available,
                b.crop_required,
                a.crop_available - b.crop_required AS crop_demand
            FROM available AS a, parameters AS b
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
        SELECT
            a.*, {plant_capacity} AS plant_capacity, b.rank,
            0.0 AS cost_harvest, 0.0 AS cost_ensiling, 0.0 AS cost_manure, 0.0 AS cost_total, 0 AS only_manure,
            b.geom
        FROM total_methane AS a
        LEFT JOIN {target} AS b ON a.id_target = b.id_target
        WHERE a.id_target = b.id_target
        ORDER BY rank DESC, id_target ASC
            ;
    """.format (
        create_table = create_table(residual_aggr),
        residual =residual,
        target = SQL_target['site_clean'].name,
        plant_capacity = plant_capacity,
        manure_demand = SQL_manure_demand[key],
        crop_demand = SQL_crop_demand[key],
        methane_demand = SQL_methane_capacity[key],
        manure_yield = SQL_methane_yield['manure'],
        crop_yield = SQL_methane_yield['crop'],
        )

    sql_custom (table=residual_aggr, sql=sql_resources)

def Step_04_calculate_Costs (residual, residual_aggr):

    cost_harvest = "(COALESCE(crop_production,0) * {0})".format(SQL_costs['harvest'])
    cost_ensiling = "((length / 1000) * COALESCE(crop_production,0) * {0})".format(SQL_costs['ensiling'])
    cost_manure = "((length / 1000) * COALESCE(manure,0)  * {0}) + ({1} * COALESCE(manure,0))".format(SQL_costs['manure_fixed'], SQL_costs['manure'])

    sql_cost = """
        WITH
        limits AS
        (
            SELECT id_target, manure_used, crop_used
            FROM {resources}
        ),
        manure_columns AS (
            -- it get the last row of the sequence of farms
            -- this is necessary to grab the next value of the query, not retrieve without it
            SELECT manure_row_1
            FROM (
                SELECT a.id_target, a.manure, a.length, b.manure_used,
                SUM (a.manure) OVER (PARTITION BY a.id_target ORDER BY a.length ASC) AS manure_aggregated,
                row_number () OVER (ORDER BY a.id_target, a.length ASC) AS manure_row_1
                FROM {residual} AS a, limits AS b
                WHERE a.id_target = b.id_target and (a.manure > 0 or a.manure is not null)
                ) AS f
            WHERE manure_aggregated  <= manure_used
            ORDER BY id_target, length DESC
            LIMIT 1
        ),
        manure AS (
            SELECT *
            FROM (
                SELECT a.id_aggregate, a.id_target, a.id_building, a.length, a.manure, a.crop_production, a.live_methane, a.crop_methane, a.methane_total, 0 AS only_manure,
                row_number () OVER (ORDER BY a.length ASC) AS live_row
                FROM {residual} AS a, last_record AS b
                WHERE a.id_target = b.id_target
                ) AS f, live_columns AS g
            WHERE f.live_row <= g.live_row_1 + 1-- grab the next value of the sequence
        ),
        crop_columns AS (
            -- it get the last row of the sequence of farms
            -- this is necessary to grab the next value of the query, not retrieve without it
            SELECT crop_row_1
            FROM (
                SELECT a.id_target, a.crop_production, a.length, b.crop_used,
                SUM (a.crop_production) OVER (PARTITION BY a.id_target ORDER BY a.length ASC) AS crop_aggregated,
                row_number () OVER (ORDER BY a.id_target, a.length ASC) AS crop_row_1
                FROM {residual} AS a, limits AS b
                WHERE a.id_target = b.id_target and (a.crop_production > 0 or a.crop_production is not null)
                ) AS f
            WHERE crop_aggregated  <= crop_used
            ORDER BY id_target, length DESC
            LIMIT 1
        )
        SELECT * FROM crop_columns
            ;
    """.format (
        resources = residual_aggr,
        residual = residual,
        harvest = cost_harvest,
        ensiling = cost_ensiling,
        manure = cost_manure,
        )

    sql_custom (table=residual_aggr, sql=sql_cost)



    # sql_cost = """
    #     WITH
    #     costs AS
    #     (
    #         SELECT *,
    #         {harvest} AS cost_harvest,
    #         {ensiling} AS cost_ensiling,
    #         {manure} AS cost_manure
    #         FROM {residual}
    #     ),
    #     crop AS (
    #         SELECT *
    #         FROM (
    #             SELECT a.id_aggregate, a.id_target, a.id_building, a.length, a.manure, a.crop_production, a.live_methane, a.crop_methane, a.methane_total, 0 AS only_manure,
    #             row_number () OVER (ORDER BY a.length ASC) AS crop_row
    #             FROM {residual} AS a, last_record AS b
    #             WHERE a.id_target = b.id_target
    #             ) AS f, crop_columns AS g
    #         WHERE f.crop_row <= g.crop_row_1 + 1-- grab the next value of the sequence
    #     )
    #     UPDATE {resources} AS a
    #     SELECT *,
    #         SET cost_harvest = b.live_methane_used,
    #             cost_ensiling = b.live_methane_used,
    #             cost_manure = b.live_methane_used,
    #             cost_total = b.live_methane_used
    #     FROM costs AS b
    #     WHERE a.id_target = b.id_target
    #         ;
    # """.format (
    #     resources = residual_aggr,
    #     residual = residual,
    #     harvest = cost_harvest,
    #     ensiling = cost_ensiling,
    #     manure = cost_manure,
    #     )
    #
    # sql_custom (table=residual_aggr, sql=sql_cost)

def Step_04_select_Plant (links, location, residual_aggr, plant_capacity, rank, minimum_value):

    global n_plants, n_rank, select_fist_plant, first_plant, found_plant, proximity_plant

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
        AND id_target NOT IN (
            SELECT DISTINCT id_target FROM {location}  -- avoid getting duplicates
        )
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
                manure_required, live_available, live_allocated,
                crop_required, crop_available, crop_allocated,
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
        error ("no more plants for the rank {0}".format(n_rank))
        n_rank -= 1
        found_plant = False

def Step_05_select_Farms (links, location, residual, plant_capacity, minimum_value):

    global found_plant

    sql_links = """
        WITH
        last_record AS (
            SELECT id_target, manure_required, live_allocated, crop_required, crop_allocated
            FROM {location}
            ORDER BY id_order DESC
            LIMIT 1
        ),
        live_columns AS (
            -- it get the last row of the sequence of farms
            -- this is necessary to grab the next value of the query, not retrieve without it
            SELECT live_row_1
            FROM (
                SELECT b.manure_required, b.live_allocated,
                SUM (a.live_methane) OVER (PARTITION BY a.id_target ORDER BY a.length ASC) AS live_methane_aggregated,
                row_number () OVER (ORDER BY a.length ASC) AS live_row_1
                FROM {residual} AS a, last_record AS b
                WHERE a.id_target = b.id_target
                ) AS f
            WHERE live_methane_aggregated  <= (live_allocated)
            ORDER BY  live_row_1 DESC
            LIMIT 1
        ),
        crop_columns AS (
            -- it get the last row of the sequence of farms
            -- this is necessary to grab the next value of the query, not retrieve without it
            SELECT crop_row_1
            FROM (
                SELECT b.crop_required, b.crop_allocated,
                SUM (a.crop_methane) OVER (PARTITION BY a.id_target ORDER BY a.length ASC) AS crop_methane_aggregated,
                row_number () OVER (ORDER BY a.length ASC) AS crop_row_1
                FROM {residual} AS a, last_record AS b
                WHERE a.id_target = b.id_target
                ) AS f, live_columns AS g
            WHERE crop_methane_aggregated <= (crop_allocated)
            ORDER BY  crop_row_1 DESC
            LIMIT 1
        ),
        manure AS (
            SELECT *
            FROM (
                SELECT a.id_aggregate, a.id_target, a.id_building, a.length, a.manure, a.crop_production, a.live_methane, a.crop_methane, a.methane_total, 0 AS only_manure,
                row_number () OVER (ORDER BY a.length ASC) AS live_row
                FROM {residual} AS a, last_record AS b
                WHERE a.id_target = b.id_target
                ) AS f, live_columns AS g
            WHERE f.live_row <= g.live_row_1 + 1-- grab the next value of the sequence
        ),
        crop AS (
            SELECT *
            FROM (
                SELECT a.id_aggregate, a.id_target, a.id_building, a.length, a.manure, a.crop_production, a.live_methane, a.crop_methane, a.methane_total, 0 AS only_manure,
                row_number () OVER (ORDER BY a.length ASC) AS crop_row
                FROM {residual} AS a, last_record AS b
                WHERE a.id_target = b.id_target
                ) AS f, crop_columns AS g
            WHERE f.crop_row <= g.crop_row_1 + 1-- grab the next value of the sequence
        ),
        no_matched_farms AS (
            SELECT DISTINCT  a.id_aggregate, a.id_target, (a.id_building), a.length, a.manure, a.crop_production, a.live_methane, a.crop_methane, a.live_methane AS methane_total, 1 AS only_manure
            FROM manure AS a, crop AS b
            WHERE a.id_building NOT IN
		          (
                  SELECT DISTINCT b.id_building
                  FROM manure AS a, crop AS b, last_record AS c
                  WHERE a.id_target = c.id_target
                  )
        ),
        join_farms AS (
	        SELECT
    		    b.id_aggregate, b.id_target, b.id_building, b.length,
    		    a.manure AS manure_used, b.crop_production AS crop_used,
    		    a.live_methane AS live_methane_used, b.crop_methane AS crop_methane_used,
    		    COALESCE(a.live_methane,0) + COALESCE(b.crop_methane,0) AS methane_used, b.only_manure
    		FROM crop AS b
    		LEFT JOIN manure AS a ON a.id_aggregate = b.id_aggregate
    		UNION ALL
    		SELECT * FROM  no_matched_farms
        ),
        cost_total AS (
            SELECT b.*, a.cost_harvest, a.cost_ensiling, a.cost_manure, a.cost_total
            FROM {residual} AS a, join_farms AS b
            WHERE a.id_aggregate = b.id_aggregate
        )
        INSERT INTO {links} (
            id_aggregate, id_target, id_building, length,
            manure_used, crop_used,
            live_methane_used, crop_methane_used, methane_used, only_manure,
            cost_harvest, cost_ensiling, cost_manure, cost_total
            )
        SELECT *
        FROM cost_total

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

def Step_06_update_Plant_Location (location, links):

    global found_plant

    sql_update = """
        WITH
        manure AS (
            SELECT id_target,
            SUM (live_methane_used) AS live_methane_used,
            SUM (methane_used) AS methane_used,
            SUM (cost_harvest) AS cost_harvest,
            SUM (cost_ensiling) AS cost_ensiling,
            SUM (cost_manure) AS cost_manure,
            SUM (cost_total) AS cost_total
            FROM {links}
            GROUP BY id_target
            ORDER BY id_target
        ),
        crop AS (
            SELECT id_target,
            SUM (crop_methane_used) AS crop_methane_used
            FROM {links}
            WHERE only_manure = 0
            GROUP BY id_target
            ORDER BY id_target
        )
        UPDATE {location} AS a
        SET live_methane_used = b.live_methane_used,
            crop_methane_used = c.crop_methane_used,
            methane_used = b.methane_used,
            cost_harvest = b.cost_harvest,
            cost_ensiling = b.cost_ensiling,
            cost_manure = b.cost_manure,
            cost_total = b.cost_total,
            manure_ratio = ROUND((cast(b.live_methane_used / b.methane_used AS numeric)),2) ,
            crop_ratio = ROUND((cast(c.crop_methane_used / b.methane_used AS numeric)),2)
        FROM manure AS b, crop AS c
        WHERE a.id_target = b.id_target AND a.id_target = c.id_target
        ;
    """.format (
        location = location,
        links = links,
        )

    if found_plant:
        sql_custom (table=location, sql=sql_update)

def Step_07_update_Residuals (location, allocation, residual, links):

    global found_plant

    cost_harvest = "(b.crop_production * {0})".format(SQL_costs['harvest'])
    cost_ensiling = "((b.length / 1000) * b.crop_production * {0})".format(SQL_costs['ensiling'])
    cost_manure = "((a.length / 1000) * a.manure * {0})".format(SQL_costs['manure'])

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
        current_plant AS (
            SELECT id_target
            FROM {location}
            ORDER BY id_order DESC
            LIMIT 1
        ),
        residuals AS (
    		SELECT a.*
    		FROM {residual} AS a, current_plant AS b
    		WHERE a.id_target = b.id_target
        ),
        links AS (
    		SELECT a.*
    		FROM {links} AS a, current_plant AS b
    		WHERE a.id_target = b.id_target
        ),
        manure AS (
            SELECT
                a.id_target, a.id_building, a.length,
                a.manure - b.manure_used AS  manure,
                a.live_methane - b.live_methane_used AS live_methane
            FROM residuals AS a, links AS b
            WHERE a.id_building = b.id_building
        ),
        crop AS (
            SELECT
                a.id_target, a.id_building, a.length,
                a.crop_production - b.crop_used AS  crop_production,
                a.crop_methane - b.crop_methane_used AS  crop_methane
            FROM residuals AS a, links AS b
            WHERE a.id_building = b.id_building AND a.only_manure = 0
        ),
        costs AS (
            SELECT
                a.id_target, a.id_building, a.length,
                (b.crop_production * 5) AS cost_harvest,
                ((b.length / 1000) * b.crop_production * 6) AS cost_ensiling,
                ((a.length / 1000) * a.manure * 0.2) AS cost_manure
            FROM manure AS a, crop AS b
            WHERE a.id_building = b.id_building
        ),
        join_tables AS (
            SELECT  a.id_target, a.id_building, a.manure, a.live_methane, b.crop_production, b.crop_methane, c.cost_harvest, c.cost_ensiling, c.cost_manure
            FROM costs AS c
            JOIN crop AS b ON b.id_building = c.id_building
            JOIN manure AS a ON a.id_building = c.id_building
        )
        UPDATE {residual} AS a
        SET
            manure = b.manure,
            crop_production = b.crop_production,
            live_methane = b.live_methane,
            crop_methane = b.crop_methane,
            methane_total = COALESCE(b.live_methane,0) +  COALESCE(b.crop_methane,0),
            cost_harvest = b.cost_harvest,
            cost_ensiling = b.cost_ensiling,
            cost_manure = b.cost_manure,
            cost_total = COALESCE(b.cost_harvest,0) + COALESCE(b.cost_ensiling,0) +  COALESCE(b.cost_manure,0)
        FROM join_tables AS b
        WHERE a.id_building = b.id_building
        ;
    """.format (
        residual = residual,
        links = links,
        allocation = allocation,
        harvest = cost_harvest,
        ensiling = cost_ensiling,
        manure = cost_manure,
        location = location,
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
        # sql_custom (table=residual, sql=sql_remove)


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
                manure_used, crop_used, live_methane_used, crop_methane_used, methane_used, cost_harvest, cost_ensiling, cost_manure, cost_total, only_manure,
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

def pause_script (count, step):

    if count >=0  :
        debug ("\n######################## step {0} ##################\n".format(step))
        programPause = raw_input("Press the <ENTER> key to continue...")


def extract_plants_by_capacity ():

    global n_plants, n_rank, select_fist_plant, first_plant, found_plant, proximity_plant

    for plant_capacity in [500, 1]:
    # for plant_capacity in [1000, 750, 500, 250, 100]:

        if plant_capacity == 1:
            exit()
#
        select_fist_plant = False
        proximity_plant = True  # select plants based on distance: SQL_distances['proximity']
        found_plant = False
        first_plant = 113
        count = 0
        n_plants = 1
        n_rank = 3


        cost_column = "cost_total"
        methane_column = "methane_total"
        minimum_value = SQL_methane_capacity[str(plant_capacity)]

        allocation = "{0}_{1}kw".format(SQL_optmization['allocation'].name, plant_capacity)
        residual = "{0}_{1}kw".format(SQL_optmization['residual'].name, plant_capacity)
        location = "{0}_{1}kw".format(SQL_optmization['location'].name, plant_capacity)
        links = "{0}_{1}kw".format(SQL_optmization['links'].name, plant_capacity)
        residual_aggr = "{0}_{1}kw".format(SQL_optmization['residual_aggr'].name, plant_capacity)
        map_routes = "{0}_{1}kw".format(SQL_optmization['map_routes'].name, plant_capacity)
        plant_costs = "{0}_{1}kw".format(SQL_plant_costs['cost'].name, plant_capacity)


        # Step_01_create_Residuals(allocation, residual)

        # pause_script (count, "step 1")

        # Step_02_initialize_Plant(location, links, plant_capacity)

        # pause_script(count, "step 2")

        while n_rank > 0:

            Step_03_aggregate_Resources(residual, residual_aggr, plant_capacity)
            #
            # pause_script(count, "step 3")
            #
            # Step_04_calculate_Costs(residual, residual_aggr)

            # Step_04_select_Plant(links, location, residual_aggr, plant_capacity, n_rank, minimum_value)
            #
            # pause_script(count, "step 4")
            #
            # Step_05_select_Farms(links, location, residual, plant_capacity, minimum_value)
            #
            # pause_script(count, "step 5")
            #
            # Step_06_update_Plant_Location (location, links)
            #
            # pause_script(count, "step 6")
            #
            # Step_07_update_Residuals(location, allocation, residual, links)
            #
            # pause_script(count, "step 7")


            count += 1

            debug ("plant capacity: {0} \t iteration: {1} \t rank: {2}".format(plant_capacity, count, n_rank))

            if count >= 1:
                print ">>>>>>>>>>>>>>>>>>>>>>>>> EXIT <<<<<<<<<<<<<<<<<<<<<<"
                exit()
                break

        Step_08_map_Route_Plants(map_routes, location, links)
