import os, sys, csv, subprocess

from pyModules.variables import *
from postGIS import *
from pg_queries import *

def Step_01_import_Vectors ():

    print db_PostGIS['cursor']

    # shape =   ADM['bbox_3035']
    for shape in [ ADM['bbox_3035'], ADM['communes'], LULC['corine_bbox']  ]:
        import_SHP (shapefile=shape.inFile_Full, table=shape.name)

def Step_02_clip_Corine (old_LULC, new_LULC, clip_table):

    print ""

    attribute = "code_12"

    clip_Corine = """
        {new_LULC} AS
        (SELECT ST_Intersection (a.geom, b.geom) AS geom, a.{attribute}
        FROM {old_LULC} AS a, {clip_table} AS b
        WHERE ST_Intersects(a.geom, b.geom));
    """.format (
            new_LULC = create_table(new_LULC),
            old_LULC = old_LULC,
            attribute = attribute,
            clip_table = clip_table
        )

    sql_custom (table=new_LULC, sql=clip_Corine)

    drop_table (table=old_LULC)

def Step_03_join_Legend (lulc, legend):

    import_CSV_PostGIS (table=legend.name, csv=legend.inFile_Full, sep=';')

    add_column (table = lulc, column = 'label character varying (255)')
    add_column (table = lulc, column = 'grass_color character varying (100)')
    add_column (table = lulc, column = 'qgis_color character varying (100)')


    lulc_join=  """
        UPDATE {lulc}
        SET label = b.label3,
            grass_color = b.color,
            qgis_color = replace(b.color , ':', ',')
        FROM {legend} b
        WHERE code_12 = b.code::text;
    """.format (
            lulc = lulc,
            legend = legend.name
        )

    sql_custom (table=lulc, sql=lulc_join)

def Step_04_extract_crop_Areas ():

    for key in ['corine_crop', 'corine_No_crop']:

        if key == 'corine_No_crop':
            criteria  = " NOT "
        else:
            criteria  = " "

        crop = sql_create_table (
            table = LULC[key].name,
            select = '*',
            from_ = LULC['corine_adm'].name,
            where = criteria + "(code_12 = '243' or code_12 = '211' or code_12 = '231' or code_12 = '321' or code_12 = '242')"
            )

        sql_custom (table=LULC[key].name, sql=crop)

def Step_05_rank_LULC_Classes ():

    add_column (table = LULC['corine_adm'].name, column = 'rank integer')
    add_column (table = LULC['corine_adm'].name, column = 'zone text')

    sql_zones = """
        UPDATE {table}
        SET {column} =
        CASE {criteria}
            WHEN '111' THEN -1 -- Continuous urban fabric
            WHEN '112' THEN -1 -- Discontinuous urban fabric
            WHEN '121' THEN 5 -- Industrial or commercial units
            WHEN '122' THEN 5 -- Road and rail networks and associated land
            WHEN '131' THEN 4 -- Mineral extraction sites
            WHEN '142' THEN 0 -- Sport and leisure facilities
            WHEN '211' THEN 3 -- Non irrigated arable land
            WHEN '231' THEN 3 -- Pasture
            WHEN '242' THEN 2 -- Complex cultivation patterns
            WHEN '243' THEN 1 -- Land principally occupied by agriculture with significant areas of natural vegetation
            WHEN '311' THEN 0 -- Broad leaved forest
            WHEN '312' THEN 0 -- Coniferous forest
            WHEN '313' THEN 0 -- Mixed forest
            WHEN '321' THEN 0 -- Natural grasslands
            WHEN '322' THEN 0 -- Moors and heathland
            WHEN '511' THEN 0 -- Water courses
        ELSE 0
        END;""".format (
            table = LULC['corine_adm'].name,
            column = 'rank',
            criteria = 'code_12'
        )

    sql_custom (table=LULC['corine_adm'].name, sql=sql_zones)

def Step_06_create_LULC_Zones ():

    sql_zones = """
        {create} AS
        WITH
            boundary AS (
                SELECT ST_Union(geom) as geom
                FROM {amd}
            ),
            urban AS (
                SELECT ST_SimplifyVW(ST_Union(ST_SnapToGrid(geom,0.01)),1) as geom
                FROM {lulc}
                WHERE code_12 = '111' OR code_12 = '112'
            ),
            industrial AS (
                SELECT ST_SimplifyVW(ST_Union(ST_SnapToGrid(geom,0.01)),1) as geom
                FROM {lulc}
                WHERE code_12 = '121' OR code_12 = '122' OR code_12 = '131'
            ),
            buffer AS (
                SELECT ST_Union(ST_Buffer(ST_Union(a.geom, b.geom), 1000)) as geom
          		FROM urban a, industrial b
            ),
    	    buffer_clip AS (
                SELECT ST_SimplifyVW(ST_Difference(c.geom, ST_Union(a.geom, b.geom)),1) AS geom
                FROM urban a, industrial b, buffer c
            ),
            forest AS (
                SELECT ST_SimplifyVW(ST_Union(ST_SnapToGrid(geom,0.0001)),1) as geom
                FROM {lulc}
                WHERE code_12 = '311' OR code_12 = '312' OR code_12 = '313'
                OR code_12 = '321' OR code_12 = '322' OR code_12 = '511'
            ),
	        forest_clip AS (
                SELECT ST_SimplifyVW(ST_Difference(a.geom, b.geom),1) AS geom
                FROM forest a, buffer_clip b
            ),
            agriculture AS (
                SELECT ST_SimplifyVW(ST_Union(ST_SnapToGrid(geom,0.01)),1) as geom
                FROM {lulc}
                WHERE code_12 = '211' OR code_12 = '231' OR code_12 = '242' OR code_12 = '243'
            ),
            agriculture_clip AS (
                SELECT ST_SimplifyVW(ST_Difference(a.geom, b.geom),1) AS geom
                FROM agriculture a, buffer_clip b
            ),
            merge_all AS (
                SELECT 2 as rank, 'buffer zone' as zone, geom FROM buffer_clip
                    UNION ALL
                SELECT 3 as rank, 'industrial zone' as zone, geom FROM industrial
                    UNION ALL
                SELECT 0 as rank, 'urban area' as zone, geom FROM urban
                    UNION ALL
                SELECT 1 as rank, 'agriculture area' as zone, geom FROM agriculture_clip
                    UNION ALL
                SELECT 0 as rank, 'forest area' as zone, geom FROM forest_clip
            )
            SELECT a.rank, a.zone, ST_Intersection (a.geom, b.geom) AS geom
            FROM merge_all AS a, boundary AS b
        ;
    """.format (
            create = create_table(SQL_target['lulc_zones'].name),
            lulc = LULC['corine_adm'].name,
            buffer = SQL_distances['lulc_zones'],
            amd = ADM['communes'].name,
            )

    sql_custom (table = SQL_target['lulc_zones'].name, sql=sql_zones)
    add_column (table = SQL_target['lulc_zones'].name, column = 'id_zones SERIAL PRIMARY KEY')
