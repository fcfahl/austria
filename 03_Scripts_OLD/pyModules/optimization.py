from variables import *
from pyModules.postGIS import *
from pg_queries import *


def Step_01_create_Residuals (residual):

    sql_residual = """
    {create_table} AS
    SELECT a.id_aggregate, a.id_target, a.id_building, a.length, b.rank, a.plant_capacity, a.manure, a.crop_production, a.live_methane, a.crop_methane, a.methane_total, a.cost_total
    FROM {cost} AS a
    LEFT JOIN {cost_aggr} AS b
    ON a.id_target = b.id_target
    ;
    """.format (
        create_table = create_table(residual),
        cost = SQL_plant_costs['cost'].name,
        cost_aggr = SQL_plant_costs['cost_aggr'].name,
    )

    sql_custom (table=residual, sql=sql_residual)

def Step_02_initialize_Plant (location, links, plant_capacity):

    # ______________ copy table structure (empty)
    sql_plants = """
        {create_table} AS
    		SELECT  id_target, rank, methane_total, cost_total, geom
            FROM {cost_aggr}
            WHERE 0 > 1
            ;
    """.format(
        create_table = create_table(location),
        cost_aggr = SQL_plant_costs['cost_aggr'].name,
        )

    sql_custom (table=location, sql=sql_plants)

    add_column (table = location, column = 'plant_capacity int')
    update_column (table = location, column = 'plant_capacity', value=plant_capacity)
    add_column (table = location, column = 'id_order SERIAL PRIMARY KEY')

    # ______________ tble to hold the links between target and farms
    sql_links = "{0} (id_aggregate int, id_target int, id_building int, plant_capacity int);".format(create_table(links))
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
        SELECT a.id_target, b.rank, {plant_capacity} AS plant_capacity, a.methane_total_aggr, a.cost_total_aggr,b.geom
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

def Step_04_select_Plant (location, residual_aggr, plant_capacity, rank, minimum_value):

    global n_plants, n_rank, select_fist_plant, first_plant

    if select_fist_plant:
        first_plant = "AND id_target = {0}".format(first_plant)
        select_fist_plant = False
    else:
        first_plant = ""

    sql_select = """
		SELECT id_target, rank, methane_total_aggr, cost_total_aggr, {plant_capacity}, geom
        FROM {residual_aggr}
        WHERE methane_total_aggr >= {minimum_value} AND cost_total_aggr > 0 AND rank = {rank}
        {first_plant}
        ORDER BY cost_total_aggr ASC
        LIMIT 1
            ;
    """.format(
        residual_aggr = residual_aggr,
        minimum_value = minimum_value,
        rank = rank,
        plant_capacity = plant_capacity,
        first_plant = first_plant,
        )

    sql_custom (table="", sql=sql_select)

    # get the remaining number of plants
    n_plants = db_PostGIS['cursor'].rowcount

    if n_plants > 0:

        info ("found plant ")

        sql_insert = """
    		INSERT INTO {location} (id_target, rank,  methane_total, cost_total, plant_capacity, geom)
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

def Step_05_update_ID_Links (links, location, residual, plant_capacity, minimum_value):

    sql_links = """
        WITH
        last_record AS (
            SELECT id_target
            FROM {location}
            ORDER BY id_order DESC
            LIMIT 1
        )
        INSERT INTO {links} (id_aggregate, id_target, id_building, plant_capacity)
        SELECT b.id_aggregate, a.id_target, b.id_building, b.plant_capacity
        FROM last_record AS a, {residual} AS b
        WHERE a.id_target = b.id_target AND methane_total <= {minimum_value}
        ;
    """.format (
        links = links,
        location = location,
        residual = residual,
        plant_capacity = plant_capacity,
        minimum_value = minimum_value,
        )

    sql_custom (table=links, sql=sql_links)

def Step_06_remove_Resources (residual, location, links, plant_capacity):

    sql_remove = """
        DELETE FROM {residual} AS a
        WHERE EXISTS  (
            SELECT *
            FROM {links} AS b
            WHERE a.id_building = b.id_building
        )
        ;
    """.format (
        residual = residual,
        location = location,
        links = links,
        plant_capacity = plant_capacity,
        )

    sql_custom (table=residual, sql=sql_remove)

def Step_07_redo_Cost_Analysis (residual, plant_capacity):

    cost_harvest = "(crop_production * {0})".format(SQL_costs['harvest'])
    cost_ensiling = "((length / 1000) * crop_production * {0})".format(SQL_costs['ensiling'])
    cost_manure = "((length / 1000) * manure * {0})".format(SQL_costs['manure'])

    sql_cost = """
        WITH
        costs AS
        (
            SELECT id_aggregate, id_target, id_building, rank, length, plant_capacity, manure, crop_production, live_methane, crop_methane,
            {harvest} AS cost_harvest,
            {ensiling} AS cost_ensiling,
            {manure} AS cost_manure
            FROM {residual}
        ),
        total AS (
            SELECT *,
            CASE
                WHEN cost_manure is not null THEN cost_manure + cost_harvest + cost_ensiling
                ELSE cost_harvest + cost_ensiling
            END AS cost_total,
            COALESCE (live_methane, 0) + crop_methane AS methane_total
            FROM costs
        )
        UPDATE {residual} AS a
        SET methane_total = b.methane_total,
            cost_total = b.cost_total
        FROM total AS b
        WHERE a.id_aggregate = b.id_aggregate
            ;
    """.format (
        residual = residual,
        harvest = cost_harvest,
        ensiling = cost_ensiling,
        manure = cost_manure,
        plant_capacity=plant_capacity
        )

    sql_custom (table=SQL_optmization['residual'].name, sql=sql_cost)

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
            INSERT INTO {map_routes} (id_aggregate, id_target, id_building, plant_capacity, farms, route)
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



def extract_plants_by_capacity ():

    global n_plants, n_rank, select_fist_plant, first_plant, cost_column, methane_column

    # for plant_capacity in [750, 500, 250, 100]:
    for plant_capacity in [750, 500]:

        select_fist_plant = True
        first_plant = 113
        count = 0
        n_plants = 1
        n_rank = 3

        cost_column = "cost_total"
        methane_column = "methane_total"
        minimum_value = SQL_plant_capacity[str(plant_capacity)] * 0.95

        residual = "{0}_{1}kw".format(SQL_optmization['residual'].name, plant_capacity)
        location = "{0}_{1}kw".format(SQL_optmization['location'].name, plant_capacity)
        links = "{0}_{1}kw".format(SQL_optmization['links'].name, plant_capacity)
        residual_aggr = "{0}_{1}kw".format(SQL_optmization['residual_aggr'].name, plant_capacity)
        map_routes = "{0}_{1}kw".format(SQL_optmization['map_routes'].name, plant_capacity)
        plant_costs = "{0}_{1}kw".format(SQL_plant_costs['cost'].name, plant_capacity)


        Step_01_create_Residuals(residual)
        Step_02_initialize_Plant(location, links, plant_capacity)

        while n_rank > 0:

            Step_03_aggregate_Costs(residual, residual_aggr, plant_capacity)
            Step_04_select_Plant(location, residual_aggr, plant_capacity, n_rank, minimum_value)
            Step_05_update_ID_Links(links, location, residual, plant_capacity, minimum_value)
            Step_06_remove_Resources(residual, location, links, plant_capacity)
            Step_07_redo_Cost_Analysis(residual, plant_capacity)

            count += 1

            debug ("plant capacity: {0} \t iteration: {1} \t rank: {2}".format(plant_capacity, count, n_rank))

            if count >= 20:
                # exit()
                break

        Step_08_map_Route_Plants(map_routes, location, links)
