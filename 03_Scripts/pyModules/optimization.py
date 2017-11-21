from variables import *
from pyModules.postGIS import *
from pg_queries import *


def Step_01_create_Residuals ():

    plant_capacity = 100

    sql_residual = """
    {create_table} AS
    SELECT a.id_aggregate, a.id_target, a.id_building, b.rank, a.length, a.plant_capacity,
        a.manure, a.crop_production, a.live_methane, a.crop_methane,
        a.methane_total_100kw AS methane_100kw, a.cost_total_100kw AS cost_100kw
    FROM {cost} AS a
    LEFT JOIN {costs_total} AS b
    ON a.id_target = b.id_target
    WHERE a.plant_capacity <= {plant_capacity}
    ;
    """.format (
        create_table = create_table(SQL_optmization['residual'].name),
        cost = SQL_plant_costs['cost'].name,
        costs_total = SQL_plant_costs['cost_total'].name,
        plant_capacity = plant_capacity,
    )

    sql_custom (table=SQL_optmization['residual'].name, sql=sql_residual)

def Step_02_initialize_Plant ():

    cost_column = "cost_100kw"
    methane_column = "methane_100kw"

    plant_capacity = 100

    # ______________ copy table structure (empty)
    sql_empty = """
        {create_table} AS
    		SELECT  id_target, rank, {methane_column}, {cost_column}, geom
            FROM {cost_total}
            WHERE 0 > 1
            ;
    """.format(
        create_table = create_table(SQL_optmization['location'].name),
        cost_total = SQL_plant_costs['cost_total'].name,
        methane_column = methane_column,
        cost_column = cost_column,
        )

    sql_custom (table=SQL_optmization['location'].name, sql=sql_empty)

    add_column (table = SQL_optmization['location'].name, column = 'plant_capacity int')
    update_column (table = SQL_optmization['location'].name, column = 'plant_capacity', value=plant_capacity)


    # ______________ tble to hold the links between target and farms
    sql_empty2 = "{0} (id_aggregate int, id_target int, id_building int, plant_capacity int);".format(create_table(SQL_optmization['links'].name))
    sql_custom (table=SQL_optmization['links'].name, sql=sql_empty2)

    add_column (table = SQL_optmization['location'].name, column = 'id_order SERIAL PRIMARY KEY')

def Step_03_aggregate_Residuals ():

    cost_column = "cost_100kw"
    methane_column = "methane_100kw"

    plant_capacity = 100

    sql_cost = """
        {create_table} AS
        WITH
        aggregated AS (
            SELECT id_target,
            SUM ({methane_column}) AS {methane_column},
            SUM ({cost_column}) AS {cost_column}
            FROM {residual}
            WHERE  plant_capacity <=  {plant_capacity}
            GROUP BY id_target
            ORDER BY id_target
        )
        SELECT a.id_target, c.rank, a.{methane_column}, a.{cost_column}, {plant_capacity} AS plant_capacity, c.geom
        FROM aggregated AS a
        LEFT JOIN {target} AS c ON a.id_target = c.id_target
            ;
    """.format (
        create_table = create_table(SQL_optmization['residual_aggr'].name),
        residual = SQL_optmization['residual'].name,
        target = SQL_target['site_clean'].name,
        methane_column = methane_column,
        cost_column = cost_column,
        plant_capacity = plant_capacity,
        )

    sql_custom (table=SQL_optmization['residual_aggr'].name, sql=sql_cost)

def Step_04_select_Plant ():

    cost_column = "cost_100kw"
    methane_column = "methane_100kw"
    minimum_value = SQL_plant_capacity['100'] * 0.9
    rank = 3

    plant_capacity = 100

    sql_insert = """
		INSERT INTO {location} (id_target, rank,  methane_100kw, cost_100kw, plant_capacity, geom)
		SELECT id_target, rank, {methane_column}, {cost_column}, {plant_capacity}, geom
        FROM {residual_aggr}
        WHERE {methane_column} > {minimum_value} AND {cost_column} > 0 AND rank = {rank}
        ORDER BY {cost_column} ASC
        LIMIT 1
            ;
    """.format(
        location = SQL_optmization['location'].name,
        residual_aggr = SQL_optmization['residual_aggr'].name,
        cost_column = cost_column,
        methane_column = methane_column,
        minimum_value = minimum_value,
        rank = rank,
        plant_capacity = plant_capacity,
        )

    sql_custom (table=SQL_optmization['location'].name, sql=sql_insert)

def Step_05_update_ID_Links ():

    cost_column = "cost_100kw"
    methane_column = "methane_100kw"
    minimum_value = SQL_plant_capacity['100'] * 0.9
    rank = 3

    plant_capacity = 100

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
        WHERE a.id_target = b.id_target AND b.plant_capacity <= {plant_capacity}
        ;
    """.format (
        links = SQL_optmization['links'].name,
        location = SQL_optmization['location'].name,
        residual = SQL_optmization['residual'].name,
        cost_column = cost_column,
        plant_capacity = plant_capacity,
        )

    sql_custom (table=SQL_optmization['links'].name, sql=sql_links)

def Step_06_remove_Resources ():

    cost_column = "cost_100kw"
    methane_column = "methane_100kw"
    minimum_value = SQL_plant_capacity['100'] * 0.9
    rank = 3

    plant_capacity = 100

    sql_remove = """
        UPDATE {residual} AS a
        SET manure = 0, crop_production = 0, live_methane = 0, crop_methane = 0
        FROM
        (
            SELECT c.id_building FROM {residual} AS c, {links} AS d
            WHERE c.id_target = d.id_target
            AND c.plant_capacity <= {plant_capacity}
        ) AS b
        WHERE a.id_building = b.id_building

        ;
    """.format (
        residual = SQL_optmization['residual'].name,
        location = SQL_optmization['location'].name,
        links = SQL_optmization['links'].name,
        plant_capacity = plant_capacity,
        )

    sql_custom (table=SQL_optmization['residual'].name, sql=sql_remove)

def Step_07_redo_Cost_Analysis ():

    cost_column = "cost_100kw"
    methane_column = "methane_100kw"
    minimum_value = SQL_plant_capacity['100'] * 0.9

    cost_harvest = "(crop_production * {0})".format(SQL_costs['harvest'])
    cost_ensiling = "((length / 1000) * crop_production * {0})".format(SQL_costs['ensiling'])
    cost_manure = "((length / 1000) * manure * {0})".format(SQL_costs['manure'])

    plant_capacity = 100

    sql_cost = """
        {create_table} AS
        WITH
        costs AS
        (
            SELECT id_aggregate, id_target, id_building, rank, length, plant_capacity, manure, crop_production, live_methane, crop_methane,
            {harvest} AS cost_harvest,
            {ensiling} AS cost_ensiling,
            {manure} AS cost_manure
            FROM {residual}
            WHERE plant_capacity <= {plant_capacity}
        ),
        total AS (
            SELECT *,
            CASE
                WHEN cost_manure is not null THEN cost_manure + cost_harvest + cost_ensiling
                ELSE cost_harvest + cost_ensiling
            END AS {cost_column},
            COALESCE (live_methane, 0) + crop_methane AS {methane_column}
            FROM costs
        )
        SELECT id_aggregate, id_target, id_building, rank, length, plant_capacity, manure, crop_production, live_methane, crop_methane, methane_100kw, cost_100kw
        FROM total
        WHERE plant_capacity <= {plant_capacity}
            ;
    """.format (
        create_table = create_table(SQL_optmization['cost'].name),
        residual = SQL_optmization['residual'].name,
        harvest = cost_harvest,
        ensiling = cost_ensiling,
        manure = cost_manure,
        plant_capacity=plant_capacity,
        farms = SQL_farms['biomass'].name,
        target = SQL_target['site_clean'].name,
        methane_column = methane_column,
        cost_column = cost_column,
        )

    sql_custom (table=SQL_optmization['cost'].name, sql=sql_cost)

def Step_05_aggregate_Costs ():

    plant_capacity = 100

    sql_cost = """
        {create_table} AS
        WITH
        aggregated AS (
            SELECT id_target,
            SUM (methane_{plant_capacity}kw) AS methane_{plant_capacity}kw,
            SUM (cost_{plant_capacity}kw) AS cost_{plant_capacity}kw
            FROM {costs}
            GROUP BY id_target
            ORDER BY id_target
        )
        SELECT a.id_target, c.rank, a.cost_{plant_capacity}kw, a.methane_{plant_capacity}kw, c.geom
        FROM aggregated AS a
        LEFT JOIN {target} AS c ON a.id_target = c.id_target
            ;
    """.format (
        create_table = create_table(SQL_optmization['cost_aggr'].name),
        costs = SQL_optmization['cost'].name,
        plant_capacity=plant_capacity,
        target = SQL_target['site_clean'].name,
        )

    sql_custom (table=SQL_optmization['cost_aggr'].name, sql=sql_cost)

def Step_06_next_Plant ():

    column = "cost_100kw"

    sql_insert = """
		INSERT INTO {location} (id_target, rank,  methane_100kw, cost_100kw, geom)
		SELECT id_target, rank, methane_100kw, cost_100kw, geom
        FROM {cost}
        WHERE methane_100kw > 200000 and cost_100kw > 0 and rank = 3
        ORDER BY cost_100kw ASC
        LIMIT 1
        ;
    """.format(
        location = SQL_optmization['location'].name,
        cost = SQL_optmization['cost_aggr'].name,
        )

    sql_custom (table=SQL_optmization['location'].name, sql=sql_insert)


def Step_09_test_Route_Plants ():


    sql_test = """
        {create_table} AS
        SELECT a.*, b.geom as farms, c.geom as route
        FROM {cost} AS a
        LEFT JOIN {farms} AS b ON a.id_building = b.id_building
        LEFT JOIN {route} AS c ON a.id_building = c.id_building
        WHERE a.id_target = {plant_id}
        AND c.id_target = {plant_id}
            ;
    """.format (
        create_table = create_table('test_optimization_route'),
        plant_id = plant_id,
        targets = SQL_target['site_clean'].name,
        farms = SQL_farms['biomass'].name,
        capacity = SQL_plants['capacity'].name,
        cost = SQL_plant_costs['cost'].name,
        route = route,
        )

    sql_custom (table='', sql=sql_test)
