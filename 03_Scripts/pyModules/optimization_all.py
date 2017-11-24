from variables import *
from pyModules.postGIS import *
from pg_queries import *


def Step_01_create_Residuals (allocation, residual):

    # _________________________ Residual resources
    sql_residual = """
    {create_table} AS
    SELECT a.id_aggregate, a.id_target, a.id_building, a.length, b.rank, a.plant_capacity, a.manure, a.crop_production, a.live_methane, a.crop_methane, a.methane_total, a.cost_total
    FROM {cost} AS a
    LEFT JOIN {cost_aggr} AS b
    ON a.id_target = b.id_target
    ORDER BY a.id_target, a.plant_capacity DESC, a.methane_total DESC, a.cost_total
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


def Step_02_initialize_Plant (location, links):

    # ______________ copy table structure (empty)
    sql_plants = """
        {create_table} (
            id_order SERIAL PRIMARY KEY, id_target int, plant_capacity int, rank int,
            live_methane_req double precision, crop_methane_req double precision,
            methane_required double precision, live_methane_used double precision,
            crop_methane_used double precision, methane_used double precision,
            cost_total double precision
        );
    """.format(create_table = create_table(location))
    sql_custom (table=location, sql=sql_plants)

    add_geometry (scheme = 'public', table = location, column = 'geom', srid = 3035, type_='POINT', dimension=2)

    # ______________ table to hold the links between target and farms
    sql_links = """
        {create_table} (
            id_aggregate int, id_target int, id_building int, plant_capacity int, length double precision,
            manure_used double precision, live_methane_used double precision,
            crop_used double precision, crop_methane_used double precision,
            methane_used double precision,
            cost_total double precision
        );
    """.format(create_table = create_table(links))
    sql_custom (table=links, sql=sql_links)

def Step_03_aggregate_Costs (residual, residual_aggr, plant_capacity):

    sql_cost = """
        {create_table} AS
        WITH
        aggregated AS (
            SELECT id_target,
            SUM (methane_total) AS methane_total_aggr,
            SUM (cost_total) AS cost_total_aggr
            FROM {residual}
            GROUP BY id_target
            ORDER BY id_target
        )
        SELECT a.id_target, b.rank, {plant_capacity} AS plant_capacity, a.methane_total_aggr, a.cost_total_aggr, b.geom
        FROM aggregated AS a
        LEFT JOIN {target} AS b ON a.id_target = b.id_target
        ORDER BY b.rank DESC, a.methane_total_aggr DESC, a.cost_total_aggr ASC
            ;
    """.format (
        create_table = create_table(residual_aggr),
        residual =residual,
        target = SQL_target['site_clean'].name,
        plant_capacity = plant_capacity,
        )

    sql_custom (table=residual_aggr, sql=sql_cost)

def Step_04_select_Plant (links, location, residual_aggr, plant_capacity, rank, minimum_value):

    global n_plants, n_rank, select_fist_plant, first_plant

    if select_fist_plant:
        first_plant = "AND a.id_target = {0}".format(first_plant)
        select_fist_plant = False
    else:
        first_plant = ""

    # _________________ check if it is the first running time
    sql_custom (table="", sql="SELECT * FROM {0}".format(links) )
    n_links = db_PostGIS['cursor'].rowcount

    if n_links > 0: # first running (this is necessary to avoid duplicates as there are shared resoruces)
        sql_from = "FROM {0} AS a, (SELECT array_agg(distinct id_target) as array_targets FROM {1}) AS b".format(residual_aggr, links)
        sql_links = "AND a.id_target != ALL (b.array_targets)"
    else:
        sql_from = "FROM {0} AS a".format(residual_aggr)
        sql_links = ""

    sql_select = """
    		SELECT a.id_target, a.rank, {plant_capacity} AS plant_capacity,
            {minimum_value} * {manure_ratio} AS live_methane_req,
            {minimum_value} * {crop_ratio} AS crop_methane_req,
            {minimum_value} AS methane_required,
            a.geom
            {sql_from}
            WHERE a.methane_total_aggr >= {minimum_value} AND a.cost_total_aggr > 0  AND a.rank = {rank}
            {sql_links} {first_plant}
            ORDER BY a.cost_total_aggr ASC
            LIMIT 1
            ;
        """.format(
            residual_aggr = residual_aggr,
            minimum_value = minimum_value,
            rank = rank,
            plant_capacity = plant_capacity,
            first_plant = first_plant,
            location = location,
            sql_links = sql_links,
            sql_from = sql_from,
            manure_ratio = SQL_methane_ratio['manure'],
            crop_ratio = SQL_methane_ratio['crop'],

            )

    sql_custom (table="", sql=sql_select)

    # _________________get the remaining number of plants
    n_plants = db_PostGIS['cursor'].rowcount

    if n_plants > 0 or n_links == 0:

        info ("found plant ")

        sql_insert = """
    		INSERT INTO {location} (id_target, rank, plant_capacity, live_methane_req, crop_methane_req, methane_required, geom)
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

def Step_05_select_Farms (links, location, residual, plant_capacity, minimum_value):


    sql_links = """
        WITH
        last_record AS (
            SELECT id_target, live_methane_req, crop_methane_req
            FROM {location}
            ORDER BY id_order DESC
            LIMIT 1
        ),
        manure AS (
            SELECT *
            FROM (
                SELECT a.id_aggregate, a.manure, a.live_methane, b.live_methane_req,
                SUM (a.live_methane) OVER (PARTITION BY a.id_target ORDER BY a.length ASC) AS live_methane_aggregated
                FROM {residual} AS a, last_record AS b
                WHERE a.id_target = b.id_target AND a.live_methane > 0
                ) AS f
            WHERE live_methane_aggregated <= (live_methane_req)
        ),
        crop AS (
            SELECT *
            FROM (
                SELECT a.id_aggregate, a.id_building, a.id_target, a.length, a.crop_production, a.crop_methane, b.crop_methane_req,
                SUM (a.crop_methane) OVER (PARTITION BY a.id_target ORDER BY a.length ASC) AS crop_methane_aggregated
                FROM {residual} AS a, last_record AS b
                WHERE a.id_target = b.id_target AND a.crop_production > 0
                ) AS f
            WHERE crop_methane_aggregated <= (crop_methane_req)
        ),
        cost_total AS (
            SELECT a.id_aggregate, a.cost_total
            FROM {residual} AS a, crop AS b
            WHERE a.id_aggregate = b.id_aggregate
        )
        INSERT INTO {links} (
            id_aggregate, id_target, id_building, plant_capacity, length,
            manure_used, crop_used, live_methane_used, crop_methane_used, methane_used, cost_total
            )
        SELECT
            p.id_aggregate, p.id_target, p.id_building,
            {plant_capacity} AS plant_capacity,  p.length,
            r.manure AS manure_used, p.crop_production AS crop_used,
            r.live_methane AS live_methane_used, p.crop_methane AS crop_methane_used,
            COALESCE(r.live_methane,0) + COALESCE(p.crop_methane,0) AS methane_used,
            s.cost_total
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

    sql_custom (table=links, sql=sql_links)

def Step_06_update_Plant_Capacity (location, links):

    sql_update = """
        WITH
        aggretate AS (
            SELECT id_target,
            SUM (live_methane_used) AS live_methane_used,
            SUM (crop_methane_used) AS crop_methane_used,
            SUM (methane_used) AS methane_used,
            SUM (cost_total) AS cost_total
            FROM {links}
            GROUP BY id_target
            ORDER BY id_target
        )
        UPDATE {location} AS a
        SET live_methane_used = b.live_methane_used,
            crop_methane_used = b.crop_methane_used,
            methane_used = b.methane_used,
            cost_total = b.cost_total
        FROM aggretate AS b
        WHERE a.id_target = b.id_target
        ;
    """.format (
        location = location,
        links = links,
        )

    sql_custom (table=location, sql=sql_update)

def Step_07_update_Resources (allocation, residual, links):

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

    sql_custom (table=allocation, sql=sql_allocation)

    # __________________________ update residuals
    sql_residual= """
        UPDATE {residual} AS a
        SET
            manure = a.manure - b.manure,
            crop_production = a.crop_production - b.crop_production,
            live_methane = a.live_methane - b.live_methane,
            crop_methane = a.crop_methane - b.crop_methane,
            methane_total = a.methane_total - b.methane_total,
            cost_total = a.cost_total - b.cost_total
        FROM {links} AS b
        WHERE a.id_building = b.id_building
        ;
    """.format (
        residual = residual,
        links = allocation,
        )

    sql_custom (table=residual, sql=sql_residual)

    # __________________________ remove empty data from residuals
    sql_remove = """
        DELETE FROM {residual}
        WHERE methane_total = 0 or methane_total is null
        ;
    """.format (residual = residual)

    sql_custom (table=residual, sql=sql_remove)


def Step_08_map_Route_Plants (map_routes, location, links):

    sql_map = """
        {create_table} AS
        SELECT a.*, b.geom AS farms, c.geom AS route
        FROM {links} AS a
        LEFT JOIN {farms} AS b  ON a.id_building = b.id_building
        LEFT JOIN {route} AS c  ON a.id_building = c.id_building AND a.id_target = c.id_target
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
                manure_used, crop_used, live_methane_used, crop_methane_used, methane_used, cost_total,
                farms, route)
            SELECT a.*, b.geom AS farms, c.geom AS route
            FROM {links} AS a
            LEFT JOIN {farms} AS b  ON a.id_building = b.id_building
            LEFT JOIN {route} AS c  ON a.id_building = c.id_building AND a.id_target = c.id_target
            ;
        """.format (
                map_routes = map_routes,
                location = location,
                links = links,
                farms = SQL_farms['biomass'].name,
                route = SQL_route_distance[distance].name,
                )

        sql_custom (table = map_routes, sql=sql_merge)



def extract_plants_all ():

    global n_plants, n_rank, select_fist_plant, first_plant

    allocation = "{0}_all".format(SQL_optmization['allocation'].name)
    residual = "{0}_all".format(SQL_optmization['residual'].name)
    location = "{0}_all".format(SQL_optmization['location'].name)
    links = "{0}_all".format(SQL_optmization['links'].name)
    residual_aggr = "{0}_all".format(SQL_optmization['residual_aggr'].name)
    map_routes = "{0}_all".format(SQL_optmization['map_routes'].name)
    plant_costs = SQL_plant_costs['cost'].name


    Step_01_create_Residuals(allocation, residual)

    Step_02_initialize_Plant(location, links)

    for plant_capacity in [500, 100]:
    # for plant_capacity in [750, 500]:
#
        select_fist_plant = True
        first_plant = 113
        count = 0
        n_plants = 1
        n_rank = 3

        minimum_value = SQL_plant_capacity[str(plant_capacity)]

        while n_rank > 0:

            Step_03_aggregate_Costs(residual, residual_aggr, plant_capacity)
            Step_04_select_Plant(links, location, residual_aggr, plant_capacity, n_rank, minimum_value)

            if n_rank == 0:
                error ("__________CHANGED PLANT CAPACITY HERE_________")

            Step_05_select_Farms(links, location, residual, plant_capacity, minimum_value)
            Step_06_update_Plant_Capacity (location, links)
            Step_07_update_Resources(allocation, residual, links)



            count += 1

            debug ("plant capacity: {0} \t iteration: {1} \t rank: {2}".format(plant_capacity, count, n_rank))

            if count >= 100:
                exit()
                break

    Step_08_map_Route_Plants(map_routes, location, links)
