    sql_test = """
        {create_table} AS
        SELECT a.*, b.geom AS farms,
            CASE
                WHEN a.id_target < 250 THEN
                    (SELECT c.geom FROM {route1} AS c WHERE a.id_building = c.id_building AND a.id_target = c.id_target)
                WHEN a.id_target < 500 THEN
                    (SELECT d.geom FROM {route2} AS d WHERE a.id_building = d.id_building AND a.id_target = d.id_target)
                WHEN a.id_target < 750 THEN
                    (SELECT e.geom FROM {route3} AS e WHERE a.id_building = e.id_building AND a.id_target = e.id_target)
                WHEN a.id_target < 1500 THEN
                    (SELECT f.geom FROM {route3} AS f WHERE a.id_building = f.id_building AND a.id_target = f.id_target)
                ELSE
                    (SELECT g.geom FROM {route3} AS g WHERE a.id_building = g.id_building AND a.id_target = g.id_target)
            END AS route
        FROM {links} AS a
        LEFT JOIN {farms} AS b  ON a.id_building = b.id_building
            ;
    """.format (
        create_table = create_table('test_optimization_route'),
        location = SQL_optmization['location'].name,
        links = SQL_optmization['links'].name,
        farms = SQL_farms['biomass'].name,
        route1 = SQL_route_distance['250'].name,
        route2 = SQL_route_distance['500'].name,
        route3 = SQL_route_distance['750'].name,
        route4 = SQL_route_distance['1500'].name,
        route5 = SQL_route_distance['2000'].name,

        )

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

    sql_test = """
        {create_table} AS
        WITH
        plants AS (
            SELECT id_target FROM {location}
        ),
        buildings AS (
            SELECT id_building
            FROM plants AS a, {links} AS b
            WHERE a.id_target = b.id_target
        ),
        points AS (
            SELECT d.*
            FROM buildings AS c, {farms} AS d
            WHERE c.id_building = d.id_building
        ),
        lines AS (
            SELECT f.id_target, f.geom
            FROM plants AS a, points AS e, {route} AS f
            WHERE a.id_target = f.id_target
            AND e.id_building = f.id_building

        )
        SELECT l.id_target, p.id_building,  p.geom AS farms, l.geom AS routes
        FROM points AS p, lines AS l
            ;
    """.format (
        create_table = create_table('test_optimization_route'),
        location = SQL_optmization['location'].name,
        links = SQL_optmization['links'].name,
        farms = SQL_farms['biomass'].name,
        route = SQL_route_distance['250'].name,

        )

    sql_custom (table='', sql=sql_test)



    DO
    $$
    DECLARE
        plant_id int;
        building_id int;
        r record;
    BEGIN
        FOR r IN SELECT * FROM {location}
        LOOP
            SELECT id_building FROM {links} AS b WHERE id_target = r.id_target;
            RAISE NOTICE 'id_target = % | attr = %', r.id_target, b.id_building;
            RETURN NEXT;
        END LOOP;
    END
    $$;


def Step_05_update_Demands ():

    for key in SQL_plant_capacity:

        # __________________________  total demand
        manure_demand = SQL_plant_capacity[key] * SQL_methane_ratio['manure']
        crop_demand = SQL_plant_capacity[key] * SQL_methane_ratio['crop']

        print " capacity = {0} \n manure denand = {1} \n crop demand = {2}\n\n".format(key,manure_demand,crop_demand)

        # __________________________  add columns
        col_manure = "live_{0}kW".format(key)
        col_crop = "crop_{0}kW".format(key)

        # add_column (table = SQL_plants['demand'].name, column = "{0} INTEGER ".format(col_manure))
        # add_column (table = SQL_plants['demand'].name, column = "{0} INTEGER".format(col_crop))


        sql_demand = """
            WITH
            livestock AS
            (
                SELECT id_aggregate, id_target,
                live_capacity_aggr - {manure_demand} AS live_allocation,
                ROW_NUMBER() OVER (PARTITION BY id_target ORDER BY live_capacity_aggr ASC) AS rank_1
                FROM {demand}
                WHERE live_capacity_aggr > 0
                ORDER BY id_target, live_capacity_aggr
            ),
            crop AS
            (
                SELECT id_aggregate, id_target,
                crop_capacity_aggr - {crop_demand} AS crop_allocation,
                ROW_NUMBER() OVER (PARTITION BY id_target ORDER BY crop_capacity_aggr ASC) AS rank_2
                FROM {demand}
                WHERE crop_capacity_aggr > 0
                ORDER BY id_target, crop_capacity_aggr
            ),
            allocation AS (
                SELECT a.live_allocation, b.*,
                    case when a.live_allocation <=0 then 1 else 0 end as {col_manure},
                    case when b.crop_allocation <=0 then 1 else 0 end as {col_crop}
                FROM crop b
                LEFT JOIN livestock a
                ON  a.id_aggregate = b.id_aggregate
            )

            UPDATE {demand} AS c
            SET {col_manure} = d.{col_manure},
                {col_crop} = d.{col_crop}
            FROM allocation AS d, livestock AS e, crop AS f
            WHERE c.id_aggregate = d.id_aggregate
            ;
        """.format (
                demand = SQL_plants['demand'].name,
                col_manure = col_manure,
                col_crop = col_crop,
                manure_demand = manure_demand,
                crop_demand = crop_demand,
                )

        sql_custom (table="", sql=sql_demand)


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
        #     SELECT id_aggregate, id_target, id_building, length, plant_capacity, live_capacity_aggr, crop_capacity_aggr, total_capacity_aggr,
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

def Step_04_calculate_Demands_OLD_2 ():

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
                SELECT id_aggregate, id_target,
                live_capacity_aggr - {manure_demand} AS live_allocation,
	            ROW_NUMBER() OVER (PARTITION BY id_target ORDER BY live_capacity_aggr ASC) AS rank_1
                FROM {capacity}
                WHERE live_capacity_aggr > 0
                ORDER BY id_target, live_capacity_aggr
            ),
            crop AS
            (
                SELECT id_aggregate, id_target,
                crop_capacity_aggr - {crop_demand} AS crop_allocation,
	            ROW_NUMBER() OVER (PARTITION BY id_target ORDER BY crop_capacity_aggr ASC) AS rank_2
                FROM {capacity}
                WHERE crop_capacity_aggr > 0
                ORDER BY id_target, crop_capacity_aggr
            ),
            residuals AS (
                SELECT
                    -- livestock
                    a.id_aggregate, a.id_target, a.live_allocation AS {residual_manure},
                    -- crops
                    b.id_aggregate, b.id_target, b.crop_allocation AS {residual_crop}
                FROM crop b
                LEFT JOIN livestock a
                ON  a.id_aggregate = b.id_aggregate
            ),
            allocation AS (
                SELECT a.live_allocation, b.*,
            		case when a.live_allocation <=0 then 1 else 0 end as {col_manure},
            		case when b.crop_allocation <=0 then 1 else 0 end as {col_crop}
                FROM crop b
                LEFT JOIN livestock a
                ON  a.id_aggregate = b.id_aggregate
            )

            UPDATE {capacity} AS c
            SET {col_manure} = d.{col_manure},
                {col_crop} = d.{col_crop}
            FROM allocation AS d, livestock AS e, crop AS f
            WHERE c.id_aggregate = d.id_aggregate
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



    DO
        $$
        DECLARE
            capacity int := 100;

            manure double precision;
            manure_residues double precision;
            manure_demand double precision := 67200.0;

            crop_demand double precision := 156800.0;

        	loop_plants record;
        	manure_target boolean;
        	crop_target boolean;


        BEGIN
            CREATE TEMPORARY TABLE IF NOT EXISTS tmp_plants_costs_100kw AS (SELECT * FROM plants_capacity  WHERE id_target < 2 ORDER BY id_target, plant_capacity, crop_capacity_aggr, live_capacity_aggr);
            -- LOOP 1 (TARGET)
                FOR loop_plants IN SELECT * FROM tmp_plants_costs_100kw
            LOOP
                manure := loop_plants.live_capacity_aggr;
                manure_residues := manure_demand - manure;
		 IF (manure_residues < 0) THEN
		 RAISE NOTICE 'negative = % ', manure_residues;
		 END IF;

--                 RAISE NOTICE 'id_target = % | attr = %', loop_plants.id_target, manure_residues;

            END LOOP;
        END
        $$;
