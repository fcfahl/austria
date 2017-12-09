from variables import *
from pyModules.postGIS import *
from pg_queries import *
from postGIS import export_CSV

def Step_01_initialize_Files (plants):

    # _________________________ create tables
    for table in [opt_plants, opt_residual, opt_allocation]:

        sql_ = "{create_table} ({columns});".format(
            create_table = create_table(table.name),
            columns=table.field_columns)

        sql_custom (table=table.name, sql=sql_)

    # _________________________ add Keys
    for table in [opt_plants, opt_residual, opt_allocation]:

        add_Pkey (table=table.name, pkey=table.pk)

        if table.fk != '':

            for fkey in table.fk:

                add_Fkey (
                    table=table.name,
                    fkeys=fkey['key'],
                    references=fkey['ref'],
                    columns=fkey['key'],
                    )

    # _________________________ add Keys
    for table in [opt_plants, opt_residual, opt_allocation]:

        if table.geom != '':
            add_geometry (scheme = 'public', table = table.name, column = 'geom', srid = 3035, type_=table.geom, dimension=2)

def Step_02_select_First_Plant (plant_costs_aggr):

    global select_fist_plant, selected_plant, n_rank

    if not select_fist_plant:

        sql_select = """
            SELECT  id_target
            FROM {plant_costs_aggr}
            WHERE rank = {n_rank}
            ORDER BY cost_total ASC
            LIMIT 1
                ;
            """.format(
                plant_costs_aggr = plant_costs_aggr,
                n_rank = n_rank,
                )

        sql_custom (table="", sql=sql_select)
        selected_plant = int(db_PostGIS['cursor'].fetchone()[0])

        debug ("SELECTED PLANT = {0}".format(selected_plant))

def Step_03_initialize_Residues(plant_costs, plant_capacity):

    sql_residual = """
    INSERT INTO {residual} AS b (
        id_residual,
        id_target,
        id_building,
        farm_used,
        length,
        rank,
        plant_capacity,
        manure_available,
        crop_available,
        methane_from_manure,
        methane_from_crop,
        methane_total_produced
    )
    SELECT
        nextval('serial'),
        a.id_target,
        a.id_building,
        0,
        a.length,
        a.rank,
        0,
        CASE
            WHEN a.length > {distance_manure} THEN 0
            ELSE a.manure
        END AS manure,
        a.crop_production,
        a.manure * {manure_yield},
        a.crop_production * {crop_yield},
        a.manure * {manure_yield} +  a.crop_production * {crop_yield}
    FROM {plant_costs} AS a
    ;
    """.format (
        residual = opt_residual.name,
        plant_costs = plant_costs,
        resources = SQL_plants['resources'].name,
        distance_manure=SQL_distances['manure'],
        manure_yield = SQL_methane_yield['manure'],
        crop_yield = SQL_methane_yield['crop'],
    )

    sql_custom (table=opt_residual.name, sql=sql_residual)

def Step_04_calculate_Manure (plant_capacity):

    global selected_plant, found_plant

    sql_manure = """
        WITH
        current_plant AS (
            SELECT {selected_plant} AS id_target
        ),
        parameters AS (
            SELECT
            c.id_target,
            {manure_demand} AS manure_required
            FROM current_plant AS c
        ),
        farm_selection AS (
            SELECT r.*
            FROM current_plant AS c, {residual} AS r
            WHERE c.id_target = r.id_target
        ),
        manure_columns AS (
            -- it get the last row of the sequence of farms
            -- this is necessary to grab the next value of the query, not retrieve without it
            SELECT f.id_target, max(manure_row_1) as manure_row_1
            FROM (
                SELECT s.id_target, s.manure_available, s.length,
                SUM (COALESCE(s.manure_available,0)) OVER (PARTITION BY s.id_target ORDER BY s.length ASC) AS manure_aggregated,
                row_number () OVER (ORDER BY s.id_target, s.length ASC) AS manure_row_1
                FROM farm_selection AS s
                WHERE s.length < {manure_distance}
                ) AS f, parameters AS p
            WHERE f.manure_aggregated  <= p.manure_required
            GROUP BY f.id_target
            ORDER BY f.id_target
        ),
        manure_farms AS (
            SELECT
                f.id_target, f.id_building, f.length,
                f.manure_available, 2 AS farm_used, f.length AS length_manure
            FROM (
                SELECT s.*,
                row_number () OVER (ORDER BY s.id_target, s.length ASC) AS manure_row
                FROM farm_selection AS s
                WHERE s.length < {manure_distance}
                ) AS f, manure_columns AS m
            WHERE f.id_target = m.id_target AND f.manure_row <= m.manure_row_1 + 1-- grab the next value of the sequence
        ),
        manure_used AS (
            SELECT id_target, id_building, length_manure, farm_used,
                manure_available AS manure_used
            FROM manure_farms
        )
        UPDATE {residual} AS r
        SET manure_available = 0,
            manure_used = CASE WHEN r.id_target = p.id_target THEN u.manure_used ELSE 0 END,
            length_manure = CASE WHEN r.id_target = p.id_target THEN u.length_manure ELSE 0 END,
            farm_used = u.farm_used
        FROM manure_used AS u, parameters AS p
        WHERE r.id_building = u.id_building
        ;
    """.format (
        plants = opt_plants.name,
        residual = opt_residual.name,
        selected_plant = selected_plant,
        manure_demand = SQL_manure_demand[plant_capacity],
        manure_distance = SQL_distances['manure'],
        )

    if found_plant:
        sql_custom (table=opt_residual.name, sql=sql_manure)

def Step_05_calculate_Manure_Methane (plant_capacity):

    global selected_plant, found_plant

    sql_methane= """
        WITH
        current_plant AS (
            SELECT {selected_plant} AS id_target
        ),
        parameters AS (
            SELECT
            c.id_target,
            {manure_demand} AS manure_required,
            {crop_demand} AS crop_required,
            {methane_demand} AS methane_required
            FROM current_plant AS c
        ),
        farm_selection AS (
            SELECT r.*
            FROM current_plant AS c, {residual} AS r
            WHERE c.id_target = r.id_target
        ),
        manure_available AS (
            SELECT id_target,
                SUM (COALESCE(manure_available,0)) AS manure_available,
                SUM (COALESCE(crop_available,0)) AS crop_available,
                SUM (COALESCE(manure_used,0)) AS manure_used
            FROM farm_selection
            GROUP BY id_target
            ORDER BY id_target
        ),
        manure_required AS (
            SELECT
                a.id_target, a.manure_available, a.manure_used, p.manure_required,
                a.manure_available - p.manure_required AS manure_residual,
                manure_used * {manure_yield} AS methane_from_manure,
                a.crop_available
            FROM manure_available AS a, parameters AS p
        ),
        manure_methane AS (
            SELECT
                id_target, manure_available, manure_required, manure_used,
                manure_residual, methane_from_manure, crop_available,
                CASE
        			WHEN manure_residual < 0 THEN (manure_residual * {manure_yield} * -1)
        			ELSE 0
                END AS methane_lacking_from_manure
            FROM manure_required
        ),
        crop_methane_missing AS (
            SELECT
                t.*,
        		t.methane_lacking_from_manure / {crop_yield} AS crop_additional
            FROM manure_methane AS t
        ),
        crop_required AS (
            SELECT m.*,
        		p.crop_required + m.crop_additional AS crop_required
            FROM crop_methane_missing AS m, parameters AS p
            WHERE m.id_target = p.id_target
        )
        INSERT INTO {plants}
            (id_plant, id_target,
            manure_required, manure_available, manure_used, manure_residual,
            crop_required, crop_available, crop_additional,
            methane_from_manure)
        SELECT
            nextval('plants'), c.id_target,
            c.manure_required, c.manure_available, c.manure_used, c.manure_residual,
            c.crop_required, c.crop_available, c.crop_additional,
            c.methane_from_manure
        FROM crop_required AS c
        ;
    """.format (
        plants = opt_plants.name,
        residual = opt_residual.name,
        selected_plant = selected_plant,
        manure_demand = SQL_manure_demand[plant_capacity],
        crop_demand = SQL_crop_demand[plant_capacity],
        methane_demand = SQL_methane_capacity[plant_capacity],
        crop_distance = SQL_distances['max_travel'],
        manure_yield = SQL_methane_yield['manure'],
        crop_yield = SQL_methane_yield['crop'],
        )

    if found_plant:
        sql_custom (table=opt_residual.name, sql=sql_methane)

def Step_06_calculate_Crop (plant_capacity):

    global selected_plant, found_plant

    sql_crop= """
        WITH
        current_plant AS (
            SELECT id_target, crop_required, crop_additional
            FROM {plants}
            ORDER BY id_plant DESC
            LIMIT 1
        ),
        farm_selection AS (
            SELECT r.*
            FROM current_plant AS c, {residual} AS r
            WHERE c.id_target = r.id_target
        ),
        crop_columns AS (
            -- it get the last row of the sequence of farms
            -- this is necessary to grab the next value of the query, not retrieve without it
            SELECT f.id_target, max(crop_row_1) as crop_row_1
            FROM (
                SELECT s.id_target, s.crop_available, s.length,
                SUM (s.crop_available) OVER (PARTITION BY s.id_target ORDER BY s.length ASC) AS crop_aggregated,
                row_number () OVER (ORDER BY s.id_target, s.length ASC) AS crop_row_1
                FROM farm_selection AS s
                WHERE s.length < {crop_distance}
                ) AS f, current_plant AS c
            WHERE f.crop_aggregated  <= c.crop_required
            GROUP BY f.id_target
            ORDER BY f.id_target
        ),
        crop_farms AS (
            SELECT
                f.id_target, f.id_building, f.length,
                f.crop_available, f.length AS length_crop,
                CASE
                    WHEN COALESCE(f.manure_used,0) = 0 THEN 3
                    WHEN COALESCE(f.manure_used,0) > 0 THEN 1
                    ELSE 0
                END AS farm_used
            FROM (
                SELECT s.*,
                row_number () OVER (ORDER BY s.id_target, s.length ASC) AS crop_row
                FROM farm_selection AS s
                WHERE s.length < {crop_distance}
                ) AS f, crop_columns AS m
            WHERE f.id_target = m.id_target AND f.crop_row <= m.crop_row_1 + 1-- grab the next value of the sequence
        ),
        crop_used AS (
            SELECT id_target, id_building, length_crop, farm_used,
                crop_available AS crop_used
            FROM crop_farms
        )
        UPDATE {residual} AS r
        SET
            crop_available = 0,
            crop_used = CASE WHEN r.id_target = c.id_target THEN u.crop_used ELSE 0 END,
            length_crop = CASE WHEN r.id_target = c.id_target THEN u.length_crop ELSE 0 END,
            farm_used = u.farm_used,
            plant_capacity = {plant_capacity}
        FROM crop_used AS u, current_plant AS c
        WHERE r.id_building = u.id_building
        ;
    """.format (
        plants = opt_plants.name,
        residual = opt_residual.name,
        selected_plant = selected_plant,
        crop_demand = SQL_crop_demand[plant_capacity],
        methane_demand = SQL_methane_capacity[plant_capacity],
        crop_distance = SQL_distances['max_travel'],
        plant_capacity = plant_capacity,
        )

    if found_plant:
        sql_custom (table=opt_residual.name, sql=sql_crop)

def Step_07_calculate_Crop_Methane (plant_capacity):

    global selected_plant, n_rank, found_plant

    sql_methane= """
        WITH
        current_plant AS (
            SELECT id_plant, id_target, crop_required, methane_from_manure
            FROM {plants}
            ORDER BY id_plant DESC
            LIMIT 1
        ),
        parameters AS (
            SELECT
            c.id_plant, c.id_target, c.methane_from_manure,
            {manure_demand} AS manure_required,
            {crop_demand} AS crop_required,
            {methane_demand} AS methane_required
            FROM current_plant AS c
        ),
        farm_selection AS (
            SELECT c.id_plant, r.*
            FROM current_plant AS c, {residual} AS r
            WHERE c.id_target = r.id_target
        ),
        crop_available AS (
            SELECT id_target,
                SUM (crop_available) AS crop_available,
                SUM (manure_used) AS manure_used,
                SUM (crop_used) AS crop_used
            FROM farm_selection
            GROUP BY id_target
            ORDER BY id_target
        ),
        crop_methane AS (
            SELECT
                p.id_plant, a.id_target, a.crop_available, a.crop_used, a.manure_used,
                p.crop_required, p.methane_required, p.methane_from_manure,
                crop_used * {crop_yield} AS methane_from_crop
            FROM crop_available AS a, parameters AS p
        ),
        total_methane AS (
            SELECT
                id_plant, id_target, crop_used, methane_from_manure, methane_from_crop, methane_required,
                COALESCE(methane_from_manure,0) + COALESCE(methane_from_crop,0)  AS methane_total_produced,
                COALESCE(manure_used,0) + COALESCE(crop_used,0)  AS resources_total
            FROM crop_methane
        ),
        lengths AS (
            SELECT
                id_target,
                AVG(length_manure) AS length_manure_avg,
                AVG(length_crop) AS length_crop_avg
            FROM {residual}
            GROUP BY id_target
        )
        UPDATE {plants} AS p
        SET plant_capacity = {plant_capacity},
            rank = {n_rank},
            length_manure_avg = l.length_manure_avg,
            length_crop_avg = l.length_crop_avg,
            crop_used = t.crop_used,
            methane_required = t.methane_required,
            methane_from_crop = t.methane_from_crop,
            methane_total_produced = t.methane_total_produced,
            ratio_manure = ROUND((cast(t.methane_from_manure / t.methane_total_produced  AS numeric)),2),
            ratio_crop = ROUND((cast(t.methane_from_crop/ t.methane_total_produced  AS numeric)),2),
            geom  = tg.geom
        FROM total_methane AS t
        LEFT JOIN lengths AS l ON t.id_target = l.id_target
        LEFT JOIN {target} AS tg ON t.id_target = tg.id_target
        WHERE p.id_plant = t.id_plant
        ;
    """.format (
        plants = opt_plants.name,
        residual = opt_residual.name,
        target = SQL_target['site_clean'].name,
        manure_demand = SQL_manure_demand[plant_capacity],
        crop_demand = SQL_crop_demand[plant_capacity],
        methane_demand = SQL_methane_capacity[plant_capacity],
        crop_distance = SQL_distances['max_travel'],
        manure_yield = SQL_methane_yield['manure'],
        crop_yield = SQL_methane_yield['crop'],
        n_rank = n_rank,
        plant_capacity = plant_capacity,
        )

    if found_plant:
        sql_custom (table=opt_residual.name, sql=sql_methane)

def Step_08_transfer_Allocation ():

    global selected_plant, found_plant

    sql_allocation= """
        INSERT INTO {allocation}
        SELECT *
        FROM {residual}
        wHERE id_target = {selected_plant}
        AND farm_used > 0
        ;
    """.format (
        plants = opt_plants.name,
        residual = opt_residual.name,
        allocation = opt_allocation.name,
        selected_plant = selected_plant,
        )

    sql_remove = """
        DELETE FROM {residual}
        WHERE farm_used = 1
        OR (
            COALESCE(manure_available,0) = 0
            AND COALESCE(crop_available,0) = 0
        )       ;
        """.format (
            residual = opt_residual.name,
            )

    if found_plant:
        sql_custom (table=opt_allocation.name, sql=sql_allocation)
        sql_custom (table=opt_residual.name, sql=sql_remove)

def Step_09_update_Residues ():

    global selected_plant, n_rank, found_plant

    sql_methane= """
        WITH
        methane AS (
            SELECT
                id_target, id_building, manure_available, crop_available,
                COALESCE(manure_available,0) * {manure_yield} AS methane_from_manure,
                COALESCE(crop_available,0) * {crop_yield} AS methane_from_crop,
                COALESCE(manure_used,0) + COALESCE(crop_used,0)  AS resources_total
            FROM {residual}
        )
        UPDATE {residual} AS r
        SET methane_from_manure = t.methane_from_manure,
            methane_from_crop = t.methane_from_crop,
            resources_total = t.resources_total,
            methane_total_produced = COALESCE(t.methane_from_manure,0) + COALESCE(t.methane_from_crop,0)
        FROM methane AS t
        WHERE r.id_target = t.id_target AND r.id_building = t.id_building
        ;
    """.format (
        residual = opt_residual.name,
        manure_yield = SQL_methane_yield['manure'],
        crop_yield = SQL_methane_yield['crop'],
        )

    if found_plant:
        sql_custom (table=opt_residual.name, sql=sql_methane)

def Step_10_calculate_Plant_Costs ():

    global selected_plant, found_plant

    manure='COALESCE(r.manure_used,0)'
    crop='COALESCE(r.crop_used,0)'
    distance1='r.length / 1000'
    distance2='r.length / 1000'
    harvest=SQL_costs['harvest']
    ensiling=SQL_costs['ensiling']
    km=SQL_costs['manure']
    fixed=SQL_costs['manure_fixed']

    cost_harvest = "({crop} * {harvest})".format(crop=crop, harvest=harvest)
    cost_ensiling = "({crop} * {ensiling} * {distance})".format(crop=crop, ensiling=ensiling, distance=distance2)
    cost_manure = "({manure} * ({fixed} + ({km}  * ({distance}))) )".format(manure=manure, fixed=fixed, km=km, distance=distance1)

    # __________________________ update residuals
    sql_cost= """
        WITH
        costs AS (
            SELECT
                r.id_target,
                SUM({cost_harvest}) AS cost_harvest,
                SUM({cost_ensiling})AS cost_ensiling,
                SUM({cost_manure}) AS cost_manure,
                SUM(COALESCE(r.manure_used,0)) AS manure_used,
                SUM(COALESCE(r.crop_used,0)) AS crop_used
            FROM {allocation} AS r
            GROUP BY r.id_target
        )
        UPDATE {plants} AS a
        SET
            cost_harvest = b.cost_harvest,
            cost_ensiling = b.cost_ensiling,
            cost_manure = b.cost_manure,
            cost_total = COALESCE(b.cost_harvest,0) + COALESCE(b.cost_ensiling,0) +  COALESCE(b.cost_manure,0),
            resources_total = COALESCE(b.manure_used,0) + COALESCE(b.crop_used,0)
        FROM costs AS b
        WHERE a.id_target = b.id_target
        ;
    """.format (
        plants = opt_plants.name,
        residual = opt_residual.name,
        allocation = opt_allocation.name,
        cost_harvest = cost_harvest,
        cost_ensiling = cost_ensiling,
        cost_manure = cost_manure,
        )

    if found_plant:
        sql_custom (table=opt_plants.name, sql=sql_cost)

def Step_11_select_Next_Plant (minimum_value, plant_capacity):

    global found_plant, selected_plant, n_rank, target_exclusion

    manure='COALESCE(manure_available,0)'
    crop='COALESCE(crop_available,0)'
    distance1='length / 1000'
    distance2='length / 1000'
    harvest=SQL_costs['harvest']
    ensiling=SQL_costs['ensiling']
    km=SQL_costs['manure']
    fixed=SQL_costs['manure_fixed']

    cost_harvest = "({crop} * {harvest})".format(crop=crop, harvest=harvest)
    cost_ensiling = "({crop} * {ensiling} * {distance})".format(crop=crop, ensiling=ensiling, distance=distance2)
    cost_manure = "({manure} * ({fixed} + ({km}  * ({distance}))) )".format(manure=manure, fixed=fixed, km=km, distance=distance1)

    sql_residual= """
        WITH
        costs AS (
            SELECT
                id_target,
                SUM(COALESCE(manure_available,0)) AS manure_available,
                SUM(COALESCE(crop_available,0)) AS crop_available,
                SUM(COALESCE(crop_additional,0)) AS crop_additional,
                SUM({cost_harvest}) AS cost_harvest,
                SUM({cost_ensiling})AS cost_ensiling,
                SUM({cost_manure}) AS cost_manure,
                SUM({cost_harvest}) + SUM({cost_ensiling}) + SUM({cost_manure}) AS cost_total,
                SUM(COALESCE(methane_total_produced,0)) AS methane_total_produced
            FROM {residual}
            WHERE rank = {n_rank}
            GROUP BY id_target
            ORDER BY cost_total ASC
        )
        SELECT id_target, methane_total_produced, manure_available, crop_available  FROM costs
        WHERE methane_total_produced >= {minimum_value}
        AND crop_available + crop_additional >= {crop_demand}
        AND id_target NOT IN  (
          SELECT DISTINCT id_target FROM {plants}
        )
        {target_exclusion}
        ;
    """.format (
        plants = opt_plants.name,
        residual = opt_residual.name,
        cost_harvest = cost_harvest,
        cost_ensiling = cost_ensiling,
        cost_manure = cost_manure,
        n_rank = n_rank,
        minimum_value = minimum_value,
        crop_demand = SQL_crop_demand[plant_capacity],
        target_exclusion = target_exclusion,
        )


    sql_custom (table='', sql=sql_residual)

    n_plants = db_PostGIS['cursor'].rowcount

    if n_plants == 0 :

        error ("no more plants for the rank {0}".format(n_rank))
        n_rank -= 1
        found_plant = False

    else:

        result = db_PostGIS['cursor'].fetchone()
        id_target = int(result[0])
        methane = int(result[1])
        manure_available = int(result[2])
        crop_available = int(result[3])

        # if id_target == 146:
        #     exit()


        if methane >= minimum_value:

            message = """\n
                methane demand:     {0}
                methane produced:   {1}
                manure_available:   {2}
                crop_available:     {3}

                """.format(minimum_value, methane, manure_available, crop_available)

            error (message)

            found_plant = True

            selected_plant = id_target

            debug ("found plant: id_target = {0}".format(selected_plant))


        else:
            error ("no more plants for the rank {0}".format(n_rank))
            n_rank -= 1
            found_plant = False



def Step_12_map_Route_Plants (map_routes, location):

    sql_plants = """
        {create_table} AS
        SELECT *
        FROM {plants}
        ;
    """.format (
        create_table = create_table(location),
        plants = opt_plants.name,
        )

    sql_custom (table=location, sql=sql_plants)



    sql_map = """
        {create_table} AS
        SELECT a.*, b.geom AS farms, c.geom AS route
        FROM {allocation} AS a
        LEFT JOIN {farms} AS b  ON a.id_building = b.id_building
        LEFT JOIN {route} AS c  ON a.id_building = c.id_building AND a.id_target = c.id_target
        WHERE a.id_building = b.id_building AND a.id_building = c.id_building
        ;
    """.format (
        create_table = create_table(map_routes),
        plants = opt_plants.name,
        residual = opt_residual.name,
        allocation = opt_allocation.name,
        farms = SQL_farms['biomass'].name,
        route = SQL_route_distance['250'].name,
        )

    sql_custom (table=map_routes, sql=sql_map)


    for distance in ['500', '750', '1000', '1250', '1500']:

        sql_merge= """
            INSERT INTO {map_routes} (
                {columns}, farms ,route
                )
            SELECT a.*, b.geom AS farms, c.geom AS route
            FROM {allocation} AS a
            LEFT JOIN {farms} AS b  ON a.id_building = b.id_building
            LEFT JOIN {route} AS c  ON a.id_building = c.id_building AND a.id_target = c.id_target
            WHERE a.id_building = b.id_building AND a.id_building = c.id_building
            ;
        """.format (
                map_routes = map_routes,
                plants = opt_plants.name,
                residual = opt_residual.name,
                allocation = opt_allocation.name,
                farms = SQL_farms['biomass'].name,
                route = SQL_route_distance[distance].name,
                columns = opt_allocation.field_names,
                )

        sql_custom (table = map_routes, sql=sql_merge)

def pause_script (pause_, step ):

#
    if pause_  :
        debug ("\n######################## {0} ##################\n".format(step))
        programPause = raw_input("Press the <ENTER> key to continue...")


def extract_plants_all ():

    global n_plants, n_rank, select_fist_plant, first_plant, found_plant, target_exclusion

    capacities = [500, 250, 100]

    select_fist_plant = False
    selected_plant = 113
    first_capacity = capacities [0]
    found_plant = True
    n_rank = 3

    exclusion = [1444, 1445, 1446]
    target_exclusion = "AND NOT ({0})".format(' or '.join( "id_target =" + str(x) for x in exclusion))


    pause_ = False

    plants = "{0}_all".format(opt_plants.name)
    residual = "{0}_all".format(opt_residual.name)

    map_routes = "{0}_all".format(opt_allocation.name)

    plant_costs = "{0}_{1}kw".format(SQL_plant_costs['cost'].name, first_capacity)
    plant_costs_aggr = "{0}_{1}kw".format(SQL_plant_costs['cost_aggr'].name, first_capacity)



    Step_01_initialize_Files(plants)

    Step_02_select_First_Plant (plant_costs_aggr)

    pause_script(pause_, "after step 2")

    Step_03_initialize_Residues (plant_costs, str(first_capacity))

    pause_script(pause_, "after step 3")

    for plant_capacity in capacities:

        count = 0
        n_plants = 1
        n_rank = 3

        minimum_value = SQL_methane_capacity[str(plant_capacity)]

        while n_rank > 0:

            Step_04_calculate_Manure (str(plant_capacity))

            pause_script(pause_, "after step 4")

            Step_05_calculate_Manure_Methane (str(plant_capacity))

            pause_script(pause_, "after step 5")

            Step_06_calculate_Crop (str(plant_capacity))

            pause_script(pause_, "after step 6")

            Step_07_calculate_Crop_Methane (str(plant_capacity))

            pause_script(pause_, "after step 7")

            Step_08_transfer_Allocation ()

            pause_script(pause_, "after step 8")

            Step_09_update_Residues ()

            pause_script(pause_, "after step 9")

            Step_10_calculate_Plant_Costs ()

            pause_script(pause_, "after step 10")

            Step_11_select_Next_Plant (minimum_value, str(plant_capacity))

            pause_script(pause_,  "after step 11")

            count += 1

            debug ("plant capacity: {0} \t iteration: {1} \n\t rank: {2} \t current plant: {3}".format(plant_capacity, count, n_rank, selected_plant))


            if count >= 150:
                print ">>>>>>>>>>>>>>>>>>>>>>>>> EXIT <<<<<<<<<<<<<<<<<<<<<<"
                exit()
                break

    Step_12_map_Route_Plants(map_routes, plants)


def export_Tables_CSV ():


    outDir = folder['FARM'].outDir

    plants = "{0}_all".format(opt_plants.name)

    columns = opt_plants.field_names

    export_CSV (outDir, plants, columns)
