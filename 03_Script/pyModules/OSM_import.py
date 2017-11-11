from variables import *
from pyModules.postGIS import *


#==============================================================================
#   there are 3 ways to extract and upload OSM data into PostGIS
#   see:
#       http://mike.teczno.com/notes/osm-and-postgres.html
#
#   It was choosen the imposm due to the fact it creates tables according to specific tags.
#   Functions based on the other libraries were created but not used here
#==============================================================================


def Step_01_download_OSM (raw, osm):

    os.chdir(osm.inDir)

    url = raw.url

    print "\n{url}\n".format(url=url)

    f = urllib2.urlopen(url)
    with open(raw.osm_name, "wb") as code:
        code.write(f.read())

def Step_02_import_PostGIS (osm, cache, limit_AOI):

    osm_file = osm.inFile_Full
    mapping = yml_folder + "/mapping.yml"

    create_cache = "{command} {mapping} {cache} {read}".format(
        command = "imposm3 import -overwritecache",
        mapping = "-mapping " + mapping,
        read = "-read "  + osm_file,
        cache = "-cachedir " + imposm3_cache)

    if limit_AOI:
        restrict_AOI = "-limitto {file} -limittocachebuffer {buffer} ".format(
            file=bbox_OSM['JSON'].inFile_Full,
            buffer = 10000)
    else:
        restrict_AOI = ""


    # ___________________ run Imposm
    outfile = "-write -connection postgis://{user}:{pwd}@{host}:{port}/{database}".format(
        user = db_PostGIS['user'],
        pwd = db_PostGIS['pwd'],
        host = db_PostGIS['host'],
        port = db_PostGIS['port'],
        database = db_PostGIS['dbname']
    )


    load_postgis = "{command} {mapping} {cache} {limitto} {outfile} ".format(
        command = "imposm3 import -optimize -deployproduction",
        mapping = "-mapping " + mapping,
        cache = "-cachedir " + imposm3_cache,
        limitto = restrict_AOI,
        outfile = outfile)

    if cache:
        print "\n__________________ creating CACHE ______________________\n"
        subprocess.call(create_cache, shell=True)

    print "\n__________________ loading OSM ______________________\n"
    subprocess.call(load_postgis, shell=True)

    # test_DB (db=db, table='osm_power')

def Step_03_reproject_Tables (tables):

    reproject_OSM (tables=tables)
