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
    # add_geometry (scheme = 'public', table = SQL_plants['initial'].name, column = 'geom', srid = 3035, type_='POINT', dimension=2)

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

    add_column (table = SQL_plants['capacity'].name, column = 'id_plants SERIAL PRIMARY KEY')


def Step_04_calculate_Demands_OLD ():

    for key in SQL_plant_capacity:

        manure_demand = SQL_plant_capacity[key] * SQL_methane_ratio['manure']
        crop_demand = SQL_plant_capacity[key] * SQL_methane_ratio['crop']

        tmp_table = "tmp_{0}".format(SQL_plant_costs[key].name)

        print " capacity = {0} \n manure denand = {1} \n crop demand = {2}\n\n".format(key,manure_demand,crop_demand)

        # ________________________ TARGETS
        sql_tmp = "CREATE TEMPORARY TABLE IF NOT EXISTS {0} AS (SELECT * FROM {1}  WHERE id_target < 4 ORDER BY id_target, plant_capacity, crop_capacity_aggr, live_capacity_aggr);".format(tmp_table, SQL_plants['capacity'].name)

        sql_loop_01 = "FOR loop_plants IN SELECT * FROM {0}".format(tmp_table)

        sql_main = """
        DO
        $$
        DECLARE
            capacity int := {key};

            manure double precision;
            manure_residues double precision;
            manure_demand double precision := {manure_demand};

            crop_demand double precision := {crop_demand};

        	loop_plants record;
        	manure_target boolean;
        	crop_target boolean;


        BEGIN
            {sql_tmp}
            -- LOOP 1 (TARGET)
                {loop_01}
            LOOP
                manure := loop_plants.live_capacity_aggr;
                manure_residues := manure_demand - manure;
                RAISE NOTICE 'id_target = % | attr = %', loop_plants.id_target, manure_residues;

            END LOOP;
        END
        $$;

        """.format(
            key = key,
            capacity = SQL_plants['capacity'].name,
            sql_tmp = sql_tmp,
            loop_01 = sql_loop_01,
            manure_demand = manure_demand,
            crop_demand = crop_demand,
            tmp_table = tmp_table,
        )

        sql_custom (table = "",  sql=sql_main)

        # sql_demand = """
        #     {create} AS
        #     SELECT id_plants, id_target, id_building, length, plant_capacity, live_capacity_aggr, crop_capacity_aggr, total_capacity_aggr,
        #     CASE
        #         WHEN live_capacity_aggr <= {manure_demand} THEN live_capacity_aggr ELSE {manure_demand} - live_capacity_aggr
        #     END AS live_residuals
        #     FROM {capacity}
        #     ORDER BY id_target, plant_capacity, crop_capacity_aggr, live_capacity_aggr
        #     ;
        # """.format (
        #         create = create_table(SQL_plant_costs[key].name),
        #         capacity = SQL_plants['capacity'].name,
        #         manure_demand = manure_demand,
        #         )
        #
        # sql_custom (table="", sql=sql_demand)

def Step_04_calculate_Demands ():

    for key in SQL_plant_capacity:

        col_manure = "live_{0}kW".format(key)
        col_crop = "crop_{0}kW".format(key)

        residual_manure = "residual_live_{0}kW".format(key)
        residual_crop = "residual_crop_{0}kW".format(key)

        add_column (table = SQL_plants['capacity'].name, column = "{0} DOUBLE PRECISION".format(col_manure))
        add_column (table = SQL_plants['capacity'].name, column = "{0} DOUBLE PRECISION".format(col_crop))
        add_column (table = SQL_plants['capacity'].name, column = "{0} DOUBLE PRECISION".format(residual_manure))
        add_column (table = SQL_plants['capacity'].name, column = "{0} DOUBLE PRECISION".format(residual_crop))

        manure_demand = SQL_plant_capacity[key] * SQL_methane_ratio['manure']
        crop_demand = SQL_plant_capacity[key] * SQL_methane_ratio['crop']

        print " capacity = {0} \n manure denand = {1} \n crop demand = {2}\n\n".format(key,manure_demand,crop_demand)

        sql_demand = """
            WITH
            livestock AS
            (
                SELECT id_plants, id_target,
                live_capacity_aggr - {manure_demand} AS live_allocation,
	            ROW_NUMBER() OVER (PARTITION BY id_target ORDER BY live_capacity_aggr ASC) AS rank_1
                FROM {capacity}
                WHERE live_capacity_aggr > 0
                ORDER BY id_target, live_capacity_aggr
            ),
            crop AS
            (
                SELECT id_plants, id_target,
                crop_capacity_aggr - {crop_demand} AS crop_allocation,
	            ROW_NUMBER() OVER (PARTITION BY id_target ORDER BY crop_capacity_aggr ASC) AS rank_2
                FROM {capacity}
                WHERE crop_capacity_aggr > 0
                ORDER BY id_target, crop_capacity_aggr
            ),
            residuals AS (
                SELECT
                    -- livestock
                    a.id_plants, a.id_target, a.live_allocation AS {residual_manure},
                    -- crops
                    b.id_plants, b.id_target, b.crop_allocation AS {residual_crop}
                FROM crop b
                LEFT JOIN livestock a
                ON  a.id_plants = b.id_plants
            ),
            allocation AS (
                SELECT a.live_allocation, b.*,
            		case when a.live_allocation <=0 then 1 else 0 end as {col_manure},
            		case when b.crop_allocation <=0 then 1 else 0 end as {col_crop}
                FROM crop b
                LEFT JOIN livestock a
                ON  a.id_plants = b.id_plants
            )

            UPDATE {capacity} AS c
            SET {col_manure} = d.{col_manure},
                {col_crop} = d.{col_crop}
            FROM allocation AS d, livestock AS e, crop AS f
            WHERE c.id_plants = d.id_plants
            ;
        """.format (
                capacity = SQL_plants['capacity'].name,
                table = SQL_plants['capacity'].name,
                col_manure = col_manure,
                col_crop = col_crop,
                manure_demand = manure_demand,
                crop_demand = crop_demand,
                residual_manure = residual_manure,
                residual_crop = residual_crop,
                )

        sql_custom (table="", sql=sql_demand)



def Step_05_calculate_Costs ():

    cost_harvest = "(crop_production * {0})".format(SQL_costs['harvest'])
    cost_ensiling = "((length / 1000) * crop_production * {0})".format(SQL_costs['ensiling'])
    cost_manure = "((length / 1000) * manure * {0})".format(SQL_costs['manure'])

    sql_cost1 = """
        {create_table} AS
        SELECT id_target, id_building, length, live_capacity_aggr, crop_capacity_aggr, plant_capacity,
        0.0 AS cost_100km

        FROM {capacity}
        ORDER BY id_target, plant_capacity, crop_capacity_aggr, live_capacity_aggr
            ;
    """.format (
        create_table = create_table(SQL_plant_costs['cost'].name),
        capacity = SQL_plants['capacity'].name,
        )

    sql_custom (table=SQL_plant_costs['cost'].name, sql=sql_cost1)


    # for key in SQL_plant_capacity:

    # sql_cost = """
    #     {create_table} AS
    #     WITH
    #     costs AS
    #     (
    #         SELECT *,
    #         {harvest} AS cost_harvest,
    #         {ensiling} AS cost_ensiling,
    #         CASE
    #             WHEN live_100kw = 1 THEN {manure}
    #             ELSE 0
    #         END AS cost_manure
    #         FROM {capacity}
    #     )
    #     SELECT *, cost_harvest + cost_ensiling + cost_manure as cost_total
    #     FROM costs
    #         ;
    # """.format (
    #     create_table = create_table(SQL_plant_costs['cost'].name),
    #     capacity = SQL_plants['capacity'].name,
    #     harvest = cost_harvest,
    #     ensiling = cost_ensiling,
    #     manure = cost_manure
    #     )
    #
    # sql_custom (table=SQL_plant_costs['cost'].name, sql=sql_cost)


def Step_05_aggregate_Costs ():

    sql_aggr = """
        {create_table} AS
        WITH
            total AS (
                SELECT *,
                SUM (cost_total) OVER (PARTITION BY plant_capacity ORDER BY length ASC) AS costs
                FROM {cost}
                GROUP BY id_target
                ORDER BY id_target
            )
        SELECT a.*, b.geom
        FROM total AS a, {targets} AS b
        WHERE a.id_target = b.id_target
            ;
    """.format (
        create_table = create_table(SQL_plant_costs['cost_total'].name),
        cost = SQL_plant_costs['cost'].name,
        targets = SQL_topology['targets'].name
        )

    sql_custom (table=SQL_plant_costs['cost_total'].name, sql=sql_aggr)


def Step_09_test_Route_Plants ():

    plant_id = 116

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



    sql_view = """
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
        targets = SQL_topology['targets'].name,
        farms = SQL_farms['biomass'].name,
        capacity = SQL_plants['capacity'].name,
        cost = SQL_plants['cost'].name,
        route = route,
        )

    sql_custom (table='', sql=sql_view)


    sql_target = """
        {create_table} AS
        SELECT a.*, b.geom
        FROM {capacity} AS a
        LEFT JOIN {targets} AS b ON a.id_target = b.id_target
        WHERE a.id_target = {plant}

            ;
    """.format (
        create_table = create_table('test_plants_target'),
        plant = plant_id,
        capacity = SQL_plants['capacity'].name,
        targets = SQL_topology['targets'].name,
        )

    sql_custom (table='', sql=sql_target)


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
