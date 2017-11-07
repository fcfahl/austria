from variables import *
from pyModules.logs import *
from pg_queries import *
import pandas as pd
import sqlalchemy as sa
import subprocess, os, psycopg2, urllib2, pprint, csv, osgeo.ogr
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT # <-- ADD THIS LINE

global cursor

def execute_Query (query, table):

    print "\n______________________________\n\n{0}\n______________________________\n".format(query)

    cursor = db_PostGIS['cursor']

    try:
        if query:
            info (query)
            cursor.execute(query)
    except:
        if query:
            error (query)
        print "error {0}".format(sys.exc_info()[0])

    if (table != "") or (query == 'VACUUM'):
        try:
            cursor.execute("VACUUM ANALYZE %s  ;" % table)
            print ("VACUUM ANALYZE %s  ;" % table)
        except:
            pass

def connect_PostGIS (db=db_PostGIS['dbname']):

    global connect

    try:
        connect = psycopg2.connect(
            dbname=db,
            user=db_PostGIS['user'],
            host=db_PostGIS['host'],
            password=db_PostGIS['pwd'],
            sslmode='disable')
        print "connected to the database \t->\t {db}".format(db=db)

    except psycopg2.OperationlError as e:
        print "unable to connect to the database {db} due to \n{error}".format(db=db, error=e)
        sys.exit(1)

    connect.autocommit = True
    return connect.cursor()

def create_DB (db=db_PostGIS['dbname']):

    print "\n__________________ creating DB ______________________\n"

    connect_PostGIS (db='postgres')

    # try:
    cursor.execute("DROP DATABASE IF EXISTS  %s  ;" % db)
    cursor.execute("CREATE DATABASE %s  ;" % db)
    print "created the database \t\t->\t {db}".format(db=db)

    # except:
    #     print "\n#######################################################"
    #     print "\t\tunable to create the database \t->\t {db}".format(db=db)
    #     print "#######################################################\n"

    connect_PostGIS ()

    # try:
    cursor.execute("CREATE EXTENSION hstore;")
    cursor.execute("CREATE EXTENSION postgis;")
    cursor.execute("CREATE EXTENSION pgrouting;")
    print "created postgis extensions in the database \t->\t {db}".format(db=db)
    # except:
    #     print "\n#######################################################"
    #     print "\t\tunable to create extensions \t->\t {db}".format(db=db)
    #     print "#######################################################\n"

def reproject_OSM (tables):

    print "\n__________________ reprojecting DB ______________________\n"

    for key in tables:

        geometry = OSM_tables[key]
        prefix = 'osm_'
        name = prefix + key
        query = None
        osm_proj = '3857'
        error = False

        reproject = "ALTER TABLE {name} ALTER COLUMN geom "\
            "TYPE geometry ({geometry}, {reproj}) " \
            "USING ST_Transform (geom, {reproj})".format(
            name = name,
            geometry = geometry,
            reproj = db_PostGIS['proj'])

        fix_Geometry = "ALTER TABLE {name} ALTER COLUMN geom " \
            "TYPE geometry ({geometry}, {osm_proj}) "\
            "USING ST_Multi(geom); {reproject}".format(
            name = name,
            geometry = geometry,
            osm_proj = osm_proj,
            proj = db_PostGIS['proj'],
            reproject = reproject)

        # print fix_Geometry

        try:
            cursor.execute(fix_Geometry)
            cursor.execute("VACUUM ANALYZE %s  ;" % name)
        except:
            error = True
        #
        if error:
        #     # try:
            print "geometry ok"
            execute_Query (reproject, name)
            # except:
            #     print "\n#######################################################"
            #     print "\t\tunable to reproject the table \t->\t {table}\nusing:\n{query}".format(
            #             table=name, query=query)
            #     print "#######################################################\n"

def fix_Geometry (table, geometry):

    print "\n__________________ fixing Geometry ______________________\n"

    query = """
        ALTER TABLE {table}
        ALTER COLUMN geom
        SET DATA TYPE geometry ({geometry}, {proj})
        USING ST_Force_2D(geom);""".format(
            table = table,
            geometry = geometry,
            proj = db_PostGIS['proj']
            )

    execute_Query (query, table)

def set_Primary_Key (table, primary_key):

    print "\n__________________ Setting Primary Key ______________________\n"
    print "table ->" + table

    drop_key(table)
    add_Pkey(table, primary_key)

def test_DB (table):

    print "\n__________________ Testing DB ______________________\n"

    try:

        query = "SELECT value FROM {table} limit 10".format(table=table)

        # execute our Query
        cursor.execute(query)

        # retrieve the records from the database
        records = cursor.fetchall()

        # print out the records using pretty print
        pprint.pprint(records)

    except:
        print "\n#######################################################"
        print "\t\tunable to query the table"
        print "#######################################################\n"

def export_SHP (osm,  proj, tables):

    print "\n__________________ Exporting Shapefiles ______________________\n"

    for key in tables:

        outFile = "{folder}/{db}_{key}_EPSG{proj}.shp".format(
            folder=osm.outDir,
            db=db_PostGIS['dbname'],
            key=key,
            proj=proj
        )

        command = "pgsql2shp -h {host} -p {port} -u {user} -P {pwd} -f {outFile} {db} 'SELECT * FROM public.osm_{table}'" .format(
            host=db_PostGIS['host'],
            port=db_PostGIS['port'],
            db=db_PostGIS['dbname'],
            user=db_PostGIS['user'],
            pwd=db_PostGIS['pwd'],
            table=key,
            outFile=outFile)

        # print command

        subprocess.call(command, shell=True)

def import_SHP (shapefile, table):

    print "\n__________________ Importing Shapefiles ______________________\n"

    drop_table (table)

    sql_file = "{0}.sql".format(table)

    command = "shp2pgsql -s {proj} -I -c {inFile} {table}> {sql}" .format(
        proj=db_PostGIS['proj'],
        table=table,
        sql=sql_file,
        inFile=shapefile)

    command2 = "psql --no-password --dbname={dbname} --username={user} --host={host} --table={table} < {sql}" .format(
        dbname=db_PostGIS['dbname'],
        host=db_PostGIS['host'],
        pwd=db_PostGIS['pwd'],
        user=db_PostGIS['user'],
        sql=sql_file,
        table=table
        )

    subprocess.call(command, shell=True)
    subprocess.call(command2, shell=True)


    execute_Query("VACUUM", table)
    os.remove (sql_file)

def import_CSV (table, csv, columns):

    print "\n__________________ Importing CSV ______________________\n"

    execute_Query (drop_table (table), table)

    command = "psql --no-password --dbname={dbname} --username={user} --host={host} -c \"\copy {table} from '{csv}' with delimeter as ';' csv;\" " .format(
        table=table,
        csv=csv,
        dbname=db_PostGIS['dbname'],
        host=db_PostGIS['host'],
        pwd=db_PostGIS['pwd'],
        user=db_PostGIS['user'],
        )

    print command

    subprocess.call(command, shell=True)

def import_CSV_PostGIS (table, csv, sep=','):

    print "\n__________________ Importing CSV ______________________\n"
    print "table ->" + table

    engine = "postgresql+psycopg2://{user}:{pwd}@{host}/{db}".format(
        db=db_PostGIS['dbname'],
        user=db_PostGIS['user'],
        pwd=db_PostGIS['pwd'],
        host=db_PostGIS['host'])
    con = sa.create_engine(engine)

    #_________read csv:
    df = pd.read_csv(csv, sep=sep, chunksize=100000)

    try:
        for row in df:
            row.to_sql(name=table, con=con, if_exists='replace')

    except:
        print "\n#######################################################"
        print "\t\tunable to import csv"
        print "#######################################################\n"


    execute_Query("VACUUM", table)

def export_PostGIS (db, outFile):

    print "\n__________________ Dumping DB ______________________\n"

    # try:

    command = "pg_dump -F t --no-password --host={host} --port={port} --dbname={db} --username={user} --file={outFile}.tar.gz".format(
        host=db_PostGIS['host'],
        port=db_PostGIS['port'],
        db=db,
        user=db_PostGIS['user'],
        outFile=outFile)

    subprocess.call(command, shell=True)


    # except:
    #     print "\n#######################################################"
    #     print "\t\tunable to export the database"
    #     print "#######################################################\n"
