from variables import *
from pyModules.postGIS import *
from pg_queries import *

def Step_01_create_Farm_Roads ():

    # ______________ create main roads
    sql_create_table (
        table = SQL_roads['roads_main'].name,
        select = '*',
        from_ = SQL_roads['roads_main'].from_,
        where = "value = 'primary' OR value = 'secondary' OR value = 'tertiary'"
    )

    add_column (table = SQL_roads['roads_main'].name, column = 'id_road SERIAL PRIMARY KEY')

def Step_02_create_Target_Points ():

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

def Step_03_clean_Target_Points ():

    # ______________ select points based on lulc zones
    sql_points = """
        {create} AS
        WITH zones AS ( SELECT * FROM {lulc} WHERE rank != 0 ORDER BY rank DESC )
        SELECT a.*, b.rank, b.zone, 0 as id_mun
        FROM {target} a, zones b
        WHERE ST_Within (a.geom, b.geom)
    """.format (
            create = create_table(SQL_target['site_clean'].name),
            target = SQL_target['site_targets'].name,
            lulc = SQL_target['lulc_zones'].name
        )

    sql_custom (table=SQL_target['site_clean'].name, sql=sql_points)


    # ______________ remove points outside comunnes
    sql_communes = """
        UPDATE {table}
        SET id_mun = b.mun_id
        FROM {adm} b
        WHERE ST_intersects ({table}.geom, b.geom);
    """.format (
            table = SQL_target['site_clean'].name,
            adm = ADM['communes'].name
            )

    sql_custom (table=SQL_target['site_clean'].name, sql=sql_communes)

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
