import xlrd, xlwt, csv
from variables import *
from pyModules.postGIS import *

def convert_XLS_CSV (inXLS, outCSV, inSheet):

    # convert xls to csv
    wb = xlrd.open_workbook(inXLS, 'utf-8')
    sh = wb.sheet_by_name(inSheet)
    csv_File = open(outCSV, 'wb')
    wr = csv.writer(csv_File, quoting=csv.QUOTE_ALL)

    for rownum in xrange(sh.nrows):
        wr.writerow    ([unicode(val).encode('utf8') for val in sh.row_values(rownum)])

    csv_File.close()

def Step_00_dissolve_Roads ():

    # ______________ dissolve osm roads - for snapping points later on
    sql_create_table (
        table = SQL_roads['roads_dissolved'].name,
        select = 'ST_Union(geom) as geom',
        from_ = SQL_roads['roads_dissolved'].from_,
        where = ""
    )

def Step_01_import_Farm_Tables ():

    for key in FARM:

        csv_file = FARM[key].outFile + '.csv'
        convert_XLS_CSV (inXLS=FARM[key].inFile_Full, outCSV=csv_file, inSheet=FARM[key].outFile)

        drop_table (table=FARM[key].name)

        import_CSV_PostGIS (table=FARM[key].name, csv=csv_file, sep=',')

        add_Pkey (table=FARM[key].name, pkey='id_farm')

        os.remove (csv_file)

def Step_01b_clean_Manure_Tables ():

    for key in FARM:

        if key == 'heads' or  key == 'manure' or  key == 'methane' or  key == 'lsu' :

            delete_records (table=FARM[key].name, where="bt_500 = 0")

def Step_02_rank_Farm_Tables ():

    for key in FARM:

        if key != 'parameter':

            table =  "rank_" + FARM[key].name
            sequence = 'serial'

            create_index (table=FARM[key].name, column='total')

            # ______________ order farms by total column
            sql_rank = """
                {create_table} AS
                SELECT {select}
                FROM {from_}
                ORDER BY {order};
            """.format (
                    create_table = create_table(table),
                    select = "*, 0 as rank1, 0 AS row1, '0'::text AS index1, 0 as id_order",
                    from_ = FARM[key].name,
                    sequence = sequence,
                    order = 'id_mun ASC, total DESC'
                    )

            sql_custom (table=table, sql=sql_rank)

            # ______________ add IDS for each farm
            restart_sequence (sequence)
            update_column (table="rank_" + FARM[key].name, column="id_order", value="nextval('serial')")

            # ______________ rank the farms according to municipal ID
            sql_rank2 = """
                WITH a AS
                    (
                        SELECT
                            id_order, id_mun,
                            row_number() OVER (ORDER BY id_mun) AS row1,
                            rank() OVER (ORDER BY id_mun) AS rank1
                        FROM {table}
                    )
                UPDATE {table} AS b
                   SET rank1 =  a.rank1, row1 = a.row1, index1 = concat (a.id_mun, '_', (1 + a.row1 - a.rank1))
                   FROM  a
                   WHERE  a.id_order = b.id_order;
            """.format (table = table)

            sql_custom (table=table, sql=sql_rank2)

            drop_column (table = table, column = 'rank1')
            drop_column (table = table, column = 'row1')

            add_Pkey (table=table, pkey='id_farm')

            # ______________ replace tables
            drop_table (FARM[key].name)
            rename_table (old_table=table, new_table=FARM[key].name)

def Step_03_create_Farm_Roads ():

    # ______________ create main roads
    sql_create_table (
        table = SQL_roads['roads_main'].name,
        select = '*',
        from_ = SQL_roads['roads_main'].from_,
        where = "value = 'primary' OR value = 'secondary' OR value = 'tertiary'"
    )

    add_column (table = SQL_roads['roads_main'].name, column = 'id_road SERIAL PRIMARY KEY')

def Step_04_create_Target_Points ():

    # ______________ create points over the roads every 500 m
    sql_points = """
        (WITH
            line AS
                (SELECT (ST_Dump(geom)).geom AS geom FROM {table}),
            linemeasure AS
                (SELECT
                    ST_AddMeasure(line.geom, 0, ST_Length(line.geom)) AS linem,
                    generate_series(0, ST_Length(line.geom)::int, {distance}) AS {id}
                FROM line),
            geometries AS (
                SELECT
                    {id},
                    (ST_Dump(ST_GeometryN(ST_LocateAlong(linem, {id}), 1))).geom AS geom
                FROM linemeasure)
        SELECT
            {id},
            ST_SetSRID(ST_MakePoint(ST_X(geom), ST_Y(geom)), {proj}) AS geom
        FROM geometries)
    """.format (
            id = 'id_target',
            table = SQL_roads['roads_main'].name,
            proj = db_PostGIS['proj'],
            distance = SQL_distances['target']
            )

    sql_create_table_with (
        table = SQL_target['site_targets'].name,
        with_ = sql_points,
        where = ""
        )

    drop_column (table = SQL_target['site_targets'].name, column = 'id_target')
    add_column (table = SQL_target['site_targets'].name, column = 'id_target SERIAL PRIMARY KEY')

def Step_05_clean_Target_Points ():

    # ______________ remove points outside comunnes
    sql_create_table (
        table = SQL_target['site_clean'].name,
        select = "a.*",
        from_ = SQL_target['site_targets'].name + " a, " + ADM['communes'].name + " b",
        where = "ST_intersects (a.geom, b.geom)"
    )

    # ______________ clean points based on proximity
    sql_clean= """
        DELETE FROM {table} a
        WHERE EXISTS (
            SELECT {select}
            FROM {table} b
            WHERE  a.{id} < b.{id}
            AND st_dwithin(a.geom, b.geom, {distance})
        );
    """.format (
            table = SQL_target['site_clean'].name,
            select = '1',
            id = 'id_target',
            distance = (SQL_distances['target'] / 2)
            )

    sql_custom (table=SQL_target['site_clean'].name, sql=sql_clean)

    drop_column (table = SQL_target['site_clean'].name, column = 'id_target')
    add_column (table = SQL_target['site_clean'].name, column = 'id_target SERIAL PRIMARY KEY')

def Step_06_rank_Target_Points ():

    print ""

    add_column (table = SQL_target['site_clean'].name, column = 'land_code VARCHAR')
    add_column (table = SQL_target['site_clean'].name, column = 'land_label VARCHAR')
    add_column (table = SQL_target['site_clean'].name, column = 'land_color VARCHAR')
    add_column (table = SQL_target['site_clean'].name, column = 'land_rank integer')

    sql_landcover = """
        UPDATE {table} a
        SET land_code = b.code_12,
            land_label = b.label,
            land_color = b.qgis_color
        FROM {lulc} b
        WHERE ST_intersects (a.geom, b.geom);
    """.format (
            table = SQL_target['site_clean'].name,
            lulc = LULC['corine_adm'].name
            )

    sql_rank = """
        UPDATE {table}
        SET {column} =
        CASE {criteria}
            WHEN '111' THEN 13
            WHEN '112' THEN 12
            WHEN '121' THEN 11
            WHEN '122' THEN 10
            WHEN '131' THEN 9
            WHEN '211' THEN 8
            WHEN '231' THEN 7
            WHEN '242' THEN 6
            WHEN '243' THEN 5
            WHEN '311' THEN 4
            WHEN '312' THEN 3
            WHEN '313' THEN 2
            WHEN '511' THEN 1
        ELSE 0
        END;""".format (
            table = SQL_target['site_clean'].name,
            column = 'land_rank',
            criteria = 'land_code'
            )


    sql_custom (table=SQL_target['site_clean'].name, sql=sql_landcover)
    sql_custom (table=SQL_target['site_clean'].name, sql=sql_rank)

def Step_07_create_Farm_Buildings ():

    location = "ST_intersects (a.geom, b.geom)"

    shape = 'ST_NRings (a.geom) > 1'
    area = '(ST_Area(a.geom) > 700 AND ST_Area(a.geom) < 5000)'

    inclusion = "a.value = 'farm'"

    exclusion = """
        (
        a.value <> 'commercial'
        AND a.value <> 'apartments'
        AND a.value <> 'manufacture'
        AND a.value <> 'supermarket'
        AND a.value <> 'train_station'
        AND a.value <> 'house'
        AND a.value <> 'school'
        AND a.value <> 'residential'
        AND a.value <> 'greenhouse'
        )
    """

    condition = """
        {location} AND (
            {inclusion} OR (
                {exclusion} AND (
                    {shape} OR
                    {area}
        )))
    """.format(
        location = location,
        inclusion = inclusion,
        exclusion = exclusion,
        shape = shape,
        area = area
        )

    # ______________ create building table (distinct to avoid duplicates)
    sql_create_table (
        table = SQL_buildings['buildings'].name,
        select = 'DISTINCT a.value as value, a.geom',
        from_ = "osm_buildings a, " + LULC['corine_crop'].name + " b ",
        where = condition
    )

    add_column (table = SQL_buildings['buildings'].name, column = 'bld_area double precision')
    update_column (table = SQL_buildings['buildings'].name, column = 'bld_area', value='ST_Area(geom)')

def Step_08_add_communes_to_Buildings ():

    add_column (table = SQL_buildings['buildings'].name, column = 'id_mun integer')

    sql_update_table (
        table = SQL_buildings['buildings'].name,
        column = 'id_mun',
        value = 'b.mun_id',
        from_ = ADM['communes'].name + ' b ',
        where = "ST_Within(ST_Centroid(a.geom), b.geom)"
         )

def Step_09_extract_centroids_Farm_Buildings ():

    # ______________ extract centroids
    sql_create_table (
        table = SQL_buildings['centroids'].name,
        select = "value, id_mun, bld_area, ST_Centroid(geom) as geom",
        from_ = SQL_buildings['buildings'].name,
        where = ""
    )

    add_column (table = SQL_buildings['centroids'].name, column = 'id_centroid SERIAL PRIMARY KEY')

def Step_10_cluster_Centroids ():

    # ______________ cluster centroids
    # -- https://gis.stackexchange.com/questions/11567/spatial-clustering-with-postgis
    sql_cluster= """
        row_number() over () AS {column},
        ST_NumGeometries(gc),
        gc AS geom_collection,
        ST_Centroid(gc) AS centroid,
        ST_MinimumBoundingCircle(gc) AS circle,
        sqrt(ST_Area(ST_MinimumBoundingCircle(gc)) / pi()) AS radius
    """.format (column = 'id_centroid')

    sql_from = """(
        SELECT unnest(ST_ClusterWithin(geom, {distance})) gc
        FROM {table}) f
    """.format(
        distance = SQL_distances['cluster'],
        table = SQL_buildings['centroids'].name,
        )

    sql_create_table (
        table = SQL_buildings['centroids_cluster'].name,
        select = sql_cluster,
        from_ = sql_from,
        where = ""
    )

    add_column (table = SQL_buildings['centroids_cluster'].name, column = 'id_cluster SERIAL PRIMARY KEY')

def Step_11_cluster_Buildings ():

    sql_select = """
		CASE WHEN radius = 0
		THEN ST_Buffer(centroid,{distance})
		ELSE circle
		END
        AS geom
    """.format (distance = SQL_distances['cluster'] / 2 )# distance should be half of the cluster distance)

    # ______________  buffer around cluster centroids
    sql_create_table (
        table = 'tmp_buffer',
        select = sql_select,
        from_ = SQL_buildings['centroids_cluster'].name,
        where = ""
    )

    add_column (table = 'tmp_buffer', column = 'id_buffer SERIAL PRIMARY KEY')


    # ______________  update cluster id on buildings
    add_column (table = SQL_buildings['buildings'].name, column = 'id_buffer integer')

    sql_id = """
        UPDATE {table} a
        SET id_buffer = b.id_buffer
        FROM {buffer} b
        WHERE ST_intersects (a.geom, b.geom);
    """.format (
            table = SQL_buildings['buildings'].name,
            buffer = 'tmp_buffer'
            )

    sql_custom (table=SQL_buildings['buildings'].name, sql=sql_id)
    drop_table ('tmp_buffer')

    # ______________  union clusted buildings
    sql_union = """
        {create_table} AS
        SELECT {select}
        FROM {from_}
        GROUP BY {columns_1}
        ORDER BY {columns_2};
    """.format (
            create_table = create_table(SQL_buildings['buildings_cluster'].name),
            select = "id_buffer, id_mun, ST_Multi(ST_Union(geom)) as geom, sum(ST_Area(geom)) AS total_area",
            from_ = SQL_buildings['buildings'].name,
            columns_1 = 'id_buffer, id_mun',
            columns_2 = 'total_area'
            )

    sql_custom (table=SQL_buildings['buildings_cluster'].name, sql=sql_union)
    create_index (table=SQL_buildings['buildings_cluster'].name, column='total_area')

def Step_12_extract_location_Buildings ():

    # ______________  extract centroids of clustered buildings (NO SNAP)
    tmp_table = 'tmp_building_cluster_poionts'

    sql_points = """
        {create_table} AS
        SELECT {select}
        FROM {from_}
        ORDER BY {order};
    """.format (
            create_table = create_table(tmp_table),
            select = "id_mun, total_area, 0 AS rank, 0 AS row, '0'::text AS index, ST_Centroid(ST_Envelope(geom)) as geom",
            from_ = SQL_buildings['buildings_cluster'].name,
            order = 'id_mun ASC, total_area DESC'
            )

    sql_custom (table=tmp_table, sql=sql_points)

    # ______________  SNAP to roads
    sql_snap = """
        {create_table} AS
        SELECT {select}
        FROM {from_}
        GROUP BY {group}
        ORDER BY {order};
    """.format (
            create_table = create_table(SQL_buildings['location'].name),
            select = "a.id_mun, a.total_area, ST_Closestpoint(ST_Collect(b.geom), a.geom) AS geom",
            from_ = tmp_table + ' AS a, ' + SQL_roads['roads_dissolved'].name + ' AS b',
            group = 'a.total_area, a.id_mun, a.geom, b.geom',
            order = 'a.total_area DESC'
            )

    sql_custom (table=SQL_buildings['location'].name, sql=sql_snap)
    add_column (table = SQL_buildings['location'].name, column = 'id_building SERIAL PRIMARY KEY')

    # drop_table (tmp_table)

def Step_13_rank_Buildings ():

        drop_column (table = SQL_buildings['location'].name, column = 'index')
        add_column (table = SQL_buildings['location'].name, column = 'index text')

        # ______________ rank the farms according to municipal ID
        sql_rank2 = """
            WITH a AS
            (
                SELECT
                    id_building, id_mun,
                    row_number() OVER (ORDER BY id_mun) AS row1,
                    rank() OVER (ORDER BY id_mun) AS rank1
                FROM {table}
            )

            UPDATE {table} AS b
               SET index = concat (a.id_mun, '_', (1 + a.row1 - a.rank1))
               FROM  a
               WHERE  a.id_building = b.id_building;
        """.format (
                table = SQL_buildings['location'].name
                )

        sql_custom (table=SQL_buildings['location'].name, sql=sql_rank2)

def Step_14_join_Farm_Data ():

    # ______________ order farms by total column
    sql_join_left = """
        -- manure
    	LEFT JOIN {head} AS b ON a.index = b.index1
    	LEFT JOIN {lsu} AS c ON a.index = c.index1
    	LEFT JOIN {manure} AS d ON a.index = d.index1
    	LEFT JOIN {methane} AS e ON a.index = e.index1
        -- crop areas
    	LEFT JOIN {crop_area} AS f ON a.index = f.index1
    	LEFT JOIN {crop_production} AS g ON a.index = g.index1
    	LEFT JOIN {crop_methane} AS h ON a.index = h.index1
    """.format(
            head = FARM['heads'].name,
            lsu = FARM['lsu'].name,
            manure = FARM['manure'].name,
            methane = FARM['methane'].name,
            crop_area = FARM['crop_area'].name,
            crop_production = FARM['crop_production'].name,
            crop_methane = FARM['crop_methane'].name
            )

    select = """
        a.id_mun, a.id_building, a.index,
        b.id_farm as id_manure, f.id_farm as id_crop,
        b.total as heads, c.total as lsu, d.total as manure, e.total as live_methane,
        f.total as crop_area, g.total as crop_production, h.total as crop_methane,
        a.geom
    """

    sql_join = """
        {create_table} AS
        SELECT {select}
        FROM {from_}
        {join};
    """.format (
            create_table = create_table(SQL_farms['biomass'].name),
            select = select,
            from_ = SQL_buildings['location'].name + " AS a ",
            join = sql_join_left
            )

    sql_custom (table=SQL_farms['biomass'].name, sql=sql_join)
    add_Pkey (table=SQL_farms['biomass'].name, pkey = 'id_building')
