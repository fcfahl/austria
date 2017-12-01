import os
from variables import *
from pyModules.postGIS import *
from pg_queries import *


def Step_01_initialize_Files (allocation, residual, location, links, residual_aggr ):


    sql_plants = "{create_table} ({columns});".format(create_table = create_table(allocation), columns=columns_all)
    sql_custom (table=allocation, sql=sql_plants)

    sql_plants = "{create_table} ({columns});".format(create_table = create_table(residual), columns=columns_all)
    sql_custom (table=residual, sql=sql_plants)

    sql_links = "{create_table} ({columns});".format(create_table = create_table(links), columns=columns_all)
    sql_custom (table=links, sql=sql_links)

    sql_links = "{create_table} ({columns});".format(create_table = create_table(residual_aggr), columns=columns_all)
    sql_custom (table=residual_aggr, sql=sql_links)

    sql_plants = "{create_table} ({columns});".format(create_table = create_table(location), columns=columns_all)
    sql_custom (table=location, sql=sql_plants)

    add_geometry (scheme = 'public', table = location, column = 'geom', srid = 3035, type_='POINT', dimension=2)
    add_geometry (scheme = 'public', table = residual_aggr, column = 'geom', srid = 3035, type_='POINT', dimension=2)

def Step_02_update_Residues (residual, residual_aggr, links, plant_capacity, plant_costs):

    columns_plants_costs = """

    """

    # _________________________ Residual resources
    sql_residual = """
    INSERT INTO {residual} AS b (
        id_target,
        id_building,
        length,
        rank,
        manure_available,
        crop_available,
        cost_harvest,
        cost_ensiling,
        cost_manure,
        cost_total,
        only_manure
    )
    SELECT
        a.id_target,
        a.id_building,
        a.length,
        a.rank,
        a.manure,
        a.crop_production,
        a.cost_harvest,
        a.cost_ensiling,
        a.cost_manure,
        a.cost_total,
        0
    FROM {plant_costs} AS a
    ;
    """.format (
        residual = residual,
        plant_costs = plant_costs,
    )

    sql_custom (table=residual, sql=sql_residual)

    # _________________________ remove manure from distances not allowed
    sql_update = """
        UPDATE {residual}
        SET manure_available = 0
        WHERE length > {distance_allowed}
        ;
    """.format (residual = residual, distance_allowed=SQL_distances['manure'])

    sql_custom (table=residual, sql=sql_update)

    # _________________________set unique id constraint
    sql_constraint = """
        ALTER TABLE {residual_aggr}
        ADD CONSTRAINT id_target
        UNIQUE (id_target)
        ;
    """.format (residual_aggr = residual_aggr)

    sql_custom (table=residual_aggr, sql=sql_constraint)

    # _________________________populate aggr with ids
    sql_ids = """
        INSERT INTO {residual_aggr} (id_target)
        SELECT DISTINCT (id_target)
        FROM {plant_costs}
        ;
    """.format (residual_aggr = residual_aggr, plant_costs=plant_costs)

    sql_custom (table=residual_aggr, sql=sql_ids)

def Step_03_aggregate_Resources (residual, residual_aggr, plant_capacity, links, count):

    key = str(plant_capacity)

    if count == 0:
        link_buildings = "FROM {residual} AS a".format(residual=residual)
    else:
        link_buildings = """
        FROM {residual} AS a, farms AS b
        WHERE a.id_target = b.id_target AND a.id_building IN (SELECT DISTINCT id_building FROM farms)""".format(residual=residual)


    sql_resources = """
        WITH
        parameters AS (
            SELECT
            {manure_demand} AS manure_required,
            {crop_demand} AS crop_required,
            {methane_demand} AS methane_required
        ),
        farms AS (
    		SELECT id_target, id_building
    		FROM {links}
    		ORDER BY id_building
        ),
        available AS (
            SELECT id_target,
                SUM (manure_available) AS manure_available,
                SUM (crop_available) AS crop_available
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
        ),
        total_cost AS (
            SELECT a.id_target,
    			SUM(a.cost_harvest) AS cost_harvest,
                SUM(a.cost_ensiling) AS cost_ensiling,
                SUM(a.cost_manure) AS cost_manure,
                SUM(a.cost_total) AS cost_total
                -- necessary otherwise it will add buidingns not selected for the plant
            {link_buildings}
            GROUP BY a.id_target
        ),
        final AS (
            SELECT a.*, c.rank, b.cost_harvest, b.cost_ensiling, b.cost_manure, b.cost_total, c.geom
            FROM total_methane AS a
            LEFT JOIN total_cost AS b ON a.id_target = b.id_target
            LEFT JOIN site_clean AS c ON a.id_target = c.id_target
        )
            UPDATE {residual_aggr} AS b
            SET id_target = a.id_target,
                rank = a.rank,
                manure_available = a.manure_available,
                manure_required = a.manure_required,
                manure_used = a.manure_used,
                manure_residual = a.manure_residual,
                manure_methane_produced = a.manure_methane_produced,
                manure_methane_residual = a.manure_methane_residual,
                crop_available = a.crop_available,
                crop_additional = a.crop_additional,
                crop_required = a.crop_required,
                crop_used = a.crop_used,
                methane_from_manure = a.methane_from_manure,
                methane_from_crop = a.methane_from_crop,
                methane_total_produced = a.methane_total_produced,
                cost_harvest = a.cost_harvest,
                cost_ensiling = a.cost_ensiling,
                cost_manure = a.cost_manure,
                cost_total = a.cost_total,
                geom = a.geom
        FROM final AS a
        WHERE a.id_target = b.id_target
            ;
    """.format (
        residual_aggr = residual_aggr,
        residual =residual,
        links =links,
        target = SQL_target['site_clean'].name,
        plant_capacity = plant_capacity,
        manure_demand = SQL_manure_demand[key],
        crop_demand = SQL_crop_demand[key],
        methane_demand = SQL_methane_capacity[key],
        manure_yield = SQL_methane_yield['manure'],
        crop_yield = SQL_methane_yield['crop'],
        link_buildings = link_buildings,
        )

    sql_custom (table=residual_aggr, sql=sql_resources)

def Step_04_select_Plant (links, location, residual_aggr, plant_capacity, rank, minimum_value):

    global n_plants, n_rank, select_fist_plant, first_plant, found_plant, proximity_plant

    columns = """
            id_target,
            manure_available,
            manure_required,
            manure_used,
            manure_residual,
            manure_methane_produced,
            manure_methane_residual,
            crop_available,
            crop_additional,
            crop_required,
            crop_used,
            methane_from_manure,
            methane_from_crop,
            methane_total_produced,
            rank,
            cost_harvest,
            cost_ensiling,
            cost_manure,
            cost_total,
            only_manure,
            geom
    """

    if select_fist_plant:
        first_plant = "AND id_target = {0}".format(first_plant)
        select_fist_plant = False
    else:
        first_plant = ""


    # _________________ check if it is the first running time
    sql_select = """
		SELECT  {columns},
                {plant_capacity},
                ROUND((cast(methane_from_manure / methane_total_produced  AS numeric)),2),
                ROUND((cast(methane_from_crop / methane_total_produced AS numeric)),2)
        FROM {residual_aggr}
        WHERE methane_total_produced * 1.01 >= {minimum_value}
        AND crop_available >= crop_required
        AND cost_total > 0
        AND rank = {rank}
        AND id_target NOT IN (
            SELECT DISTINCT id_target FROM {location}  -- avoid getting duplicates
        )
        {first_plant}
        ORDER BY cost_total ASC
        LIMIT 1
            ;
        """.format(
            residual_aggr = residual_aggr,
            minimum_value = minimum_value,
            rank = rank,
            first_plant = first_plant,
            location = location,
            columns = columns,
            plant_capacity = plant_capacity
            )

    sql_custom (table="", sql=sql_select)

    # _________________get the remaining number of plants
    n_plants = db_PostGIS['cursor'].rowcount

    if n_plants > 0:

        info ("found plant ")
        found_plant = True

        sql_insert = """
            INSERT INTO {location} (
                {columns},
                plant_capacity,
                ratio_manure,
                ratio_crop)
    		{sql_select}
                ;
        """.format(
            location = location,
            sql_select = sql_select,
            columns = columns,
            )

        sql_custom (table=location, sql=sql_insert)

    else:
        error ("no more plants for the rank {0}".format(n_rank))
        n_rank -= 1
        found_plant = False

def Step_05_select_Farms (links, location, residual, plant_capacity, minimum_value):

    global found_plant

    columns_links = """
            id_target, id_building, length,
            manure_used, crop_used,
            cost_harvest, cost_ensiling, cost_manure, cost_total, only_manure
    """


    sql_links = """
        WITH
        last_record AS (
            SELECT id_target, manure_used, crop_used
            FROM {location}
            ORDER BY id_order DESC
            LIMIT 1
        ),
        manure_columns AS (
            -- it get the last row of the sequence of farms
            -- this is necessary to grab the next value of the query, not retrieve without it
            SELECT id_target, max(manure_row_1) as manure_row_1
            FROM (
                SELECT a.id_target, a.manure_available, a.length, b.manure_used,
                SUM (a.manure_available) OVER (PARTITION BY a.id_target ORDER BY a.length ASC) AS manure_aggregated,
                row_number () OVER (ORDER BY a.id_target, a.length ASC) AS manure_row_1
                FROM {residual} AS a, last_record AS b
                WHERE a.id_target = b.id_target and (a.length < {manure_distance})
                ) AS f
            WHERE manure_aggregated  <= manure_used
            GROUP BY id_target
            ORDER BY id_target
        ),
        manure AS (
            SELECT f.id_target,  f.id_building, f.length, f.rank, f.manure_available, f.crop_available, 0 AS only_manure
            FROM (
                SELECT a.*,
                row_number () OVER (ORDER BY a.id_target, a.length ASC) AS manure_row
                FROM {residual} AS a, last_record AS b
                WHERE a.id_target = b.id_target and (a.length < {manure_distance})
                ) AS f, manure_columns AS g
            WHERE f.id_target = g.id_target AND f.manure_available > 0 AND f.manure_row <= g.manure_row_1 + 1-- grab the next value of the sequence
        ),
        crop_columns AS (
            -- it get the last row of the sequence of farms
            -- this is necessary to grab the next value of the query, not retrieve without it
            SELECT id_target, max(crop_row_1) as crop_row_1
            FROM (
                SELECT a.id_target, a.crop_available, a.length, b.crop_used,
                SUM (a.crop_available) OVER (PARTITION BY a.id_target ORDER BY a.length ASC) AS crop_aggregated,
                row_number () OVER (ORDER BY a.id_target, a.length ASC) AS crop_row_1
                FROM {residual} AS a, last_record AS b
                WHERE a.id_target = b.id_target and (a.length < {crop_distance})
                ) AS f
            WHERE crop_aggregated  <= crop_used
            GROUP BY id_target
            ORDER BY id_target
        ),
        crop AS (
            SELECT f.id_target, f.id_building, f.length, f.rank, f.manure_available, f.crop_available, 0 AS only_manure
            FROM (
                SELECT a.*,
                row_number () OVER (ORDER BY a.id_target, a.length ASC) AS crop_row
                FROM {residual} AS a, last_record AS b
                WHERE a.id_target = b.id_target and (a.length < {crop_distance})
                ) AS f, crop_columns AS g
            WHERE f.id_target = g.id_target AND f.crop_available > 0 AND f.crop_row <= g.crop_row_1 + 1-- grab the next value of the sequence
        ),
        no_matched_farms AS (
            SELECT DISTINCT  a.id_target, (a.id_building), a.length, a.rank, a.manure_available AS manure_used, a.crop_available AS crop_used, 1 AS only_manure
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
                b.id_target, b.id_building, b.length, b.rank,
                a.manure_available AS manure_used, b.crop_available AS crop_used,
                 b.only_manure
            FROM crop AS b
            LEFT JOIN manure AS a ON a.id_target = b.id_target AND a.id_building = b.id_building
    		UNION ALL
    		SELECT * FROM  no_matched_farms
        ),

        cost_total AS (
            SELECT b.*, a.cost_harvest, a.cost_ensiling, a.cost_manure, a.cost_total
            FROM {residual} AS a, join_farms AS b
            WHERE  a.id_target = b.id_target and a.id_building = b.id_building
        )
        INSERT INTO {links} ({columns_links})
        SELECT  {columns_links}
        FROM cost_total

            ;
    """.format (
        links = links,
        location = location,
        residual = residual,
        plant_capacity = plant_capacity,
        manure_yield = SQL_methane_yield['manure'],
        crop_yield = SQL_methane_yield['crop'],
        manure_distance = SQL_distances['manure'],
        crop_distance = SQL_distances['max_travel'],
        columns_links = columns_links,

        )

    if found_plant:
        sql_custom (table=links, sql=sql_links)

def Step_06_update_Residuals (location, allocation, residual, links):

    global found_plant

    manure='COALESCE(c.manure_used,0)'
    crop='COALESCE(c.crop_used,0)'
    distance='c.length / 1000'
    harvest=SQL_costs['harvest']
    ensiling=SQL_costs['ensiling']
    km=SQL_costs['manure']
    fixed=SQL_costs['manure_fixed']

    cost_harvest = "({crop} * {harvest})".format(crop=crop, harvest=harvest)
    cost_ensiling = "({crop} * {ensiling} * {distance})".format(crop=crop, ensiling=ensiling, distance=distance)
    cost_manure = "({manure} * ({fixed} + ({km}  * ({distance}))) )".format(manure=manure, fixed=fixed, km=km, distance=distance)

    # __________________________ update allocation
    sql_allocation = """
        WITH
        current_plant AS (
            SELECT id_target
            FROM {location}
            ORDER BY id_order DESC
            LIMIT 1
        )
        INSERT INTO {allocation}
        SELECT a.*
        FROM {residual} AS a, {links} AS b, current_plant AS c
        WHERE a.id_building = b.id_building AND b.id_target = c.id_target AND a.id_target = c.id_target
        ;
    """.format (
        residual = residual,
        allocation = allocation,
        location = location,
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
                a.id_target, a.id_building, a.length, b.manure_used,
                a.manure_available - b.manure_used AS  manure_available
            FROM residuals AS a, links AS b
            WHERE a.id_building = b.id_building
        ),
        crop AS (
            SELECT
                a.id_target, a.id_building, a.length, b.crop_used,
                a.crop_available - b.crop_used AS  crop_available
            FROM residuals AS a, links AS b
            WHERE a.id_building = b.id_building AND a.only_manure = 0
        ),
        costs AS (
            SELECT
                a.id_target, a.id_building, a.length,
                {cost_harvest} AS cost_harvest,
                {cost_ensiling} AS cost_ensiling,
                {cost_manure} AS cost_manure
            FROM manure AS a, crop AS b, links AS c
            WHERE a.id_building = b.id_building AND a.id_building = c.id_building
        ),
        join_tables AS (
            SELECT  a.id_target, a.id_building, a.manure_available, a.manure_used, b.crop_available, b.crop_used, c.cost_harvest, c.cost_ensiling, c.cost_manure
            FROM costs AS c
            JOIN crop AS b ON b.id_building = c.id_building
            JOIN manure AS a ON a.id_building = c.id_building
        )
        UPDATE {residual} AS a
        SET
            manure_available = b.manure_available,
            manure_used = b.manure_used,
            crop_available = b.crop_available,
            crop_used = b.crop_used,
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
        cost_harvest = cost_harvest,
        cost_ensiling = cost_ensiling,
        cost_manure = cost_manure,
        location = location,
        )


    if found_plant:
        sql_custom (table=allocation, sql=sql_allocation)
        sql_custom (table=residual, sql=sql_residual)


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

    columns = """
            id_order,
            id_target,
            id_building,
            plant_capacity,
            length,
            rank,
            manure_available,
            manure_required,
            manure_used,
            manure_residual,
            manure_methane_produced,
            manure_methane_residual,
            crop_available,
            crop_additional,
            crop_required,
            crop_used,
            methane_from_manure,
            methane_from_crop,
            methane_total_produced,
            cost_harvest,
            cost_ensiling,
            cost_manure,
            cost_total,
            only_manure,
            ratio_manure,
            ratio_crop,
            farms,
            route
    """


    for distance in ['500', '750', '1500', '2000']:

        sql_merge= """
            INSERT INTO {map_routes} ({columns}
                )
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
                columns = columns,
                )

        sql_custom (table = map_routes, sql=sql_merge)

def pause_script (count, step):

    if count <0  :
        debug ("\n######################## {0} ##################\n".format(step))
        programPause = raw_input("Press the <ENTER> key to continue...")


def extract_plants_by_capacity ():

    global n_plants, n_rank, select_fist_plant, first_plant, found_plant, proximity_plant

    # capacities = [750, 500, 250, 100]
    capacities = [500, 1]
    for plant_capacity in capacities:

        if plant_capacity == 1:
            exit()
#
        select_fist_plant = True
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
        plant_costs_aggr = "{0}_{1}kw".format(SQL_plant_costs['cost_aggr'].name, plant_capacity)

        Step_01_initialize_Files(allocation, residual, location, links, residual_aggr )

        # pause_script (count, "after step 1")

        Step_02_update_Residues (residual, residual_aggr, links, plant_capacity, plant_costs)

        # pause_script(count, "after step 2")

        while n_rank > 0:

            Step_03_aggregate_Resources(residual, residual_aggr, plant_capacity, links, count)
            #
            pause_script(count, "after step 3")
            #
            Step_04_select_Plant(links, location, residual_aggr, plant_capacity, n_rank, minimum_value)
            #
            pause_script(count, "after step 4")
            #
            Step_05_select_Farms(links, location, residual, plant_capacity, minimum_value)
            #
            pause_script(count, "after step 5")            #
            #
            Step_06_update_Residuals(location, allocation, residual, links)
            #
            # pause_script(count, "after step 6")


            count += 1

            debug ("plant capacity: {0} \t iteration: {1} \t rank: {2}".format(plant_capacity, count, n_rank))

            if count >= 3:
                print ">>>>>>>>>>>>>>>>>>>>>>>>> EXIT <<<<<<<<<<<<<<<<<<<<<<"
                exit()
                break

        Step_08_map_Route_Plants(map_routes, location, links)
