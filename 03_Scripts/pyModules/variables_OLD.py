#!/usr/bin/env python
import os, sys, time



#_________Check OS
if sys.platform.startswith('linux'):
	home = os.getcwd()

elif sys.platform.startswith('win'):
	raise OSError('Not working in Windows OS')

else:
    raise OSError('Platform not configured.')

script = home + '03_Scripts/'
imposm3_cache = script + 'cache'

def do_Mapping ():

            folder = "{folder}styles".format(folder=script)
            mapping_File = "mapping.yml"
            head_File = "head.yml"

            os.chdir(folder)

            with open (mapping_File, 'w') as outFile:

                with open (head_File) as head:
                    outFile.write(head.read())

                for table in tables:

                    yml_File = "{file}.yml".format(file=table)
                    with open (yml_File) as inFile:
                        outFile.write(inFile.read())

            os.chdir(script)

            return "{folder}/{mapping}".format(folder=folder, mapping=mapping_File)


#_________ classes
class Mapsets:

    def __init__(self, name, number):

        folder = number + '_' + name
        mapset = "M" + folder

        self.folder = folder
        self.mapset = mapset
        self.inDir = home + '01_Input/' + folder + "/"
        self.outDir = home + '02_Output/' + folder + "/"

        if not os.path.exists(self.inDir):
            os.makedirs(self.inDir)

        if not os.path.exists(self.outDir):
            os.makedirs(self.outDir)

class Files:

    class_counter= 0

    def getName(self):
        return self.__class__.__name__

    def __init__(self, inFile, name, outFile, proj, inDir, outDir, *parameter):


        self.inFile = inFile
        self.outFile = outFile

        self.inDir = inDir
        self.outDir = outDir
        self.inFile_Full = inDir + inFile
        self.outFile_Full = outDir + outFile

        self.parameter = parameter


class OSM_obj:

    def getName(self):
        return self.__class__.__name__

    def __init__(self, continent, country, region, proj, tables):

        def set_Att ():

            prefix = 'http://download.geofabrik.de'
            sufix = 'latest.osm.pbf'

            if region != '':
                name = "{region}-{sufix}".format(region=region, sufix=sufix)
                db_file = "{country}_{region}".format(country=country,region=region)
                url = "{prefix}/{continent}/{country}/{name}".format(
                        prefix=prefix,
                        continent=continent,
                        country=country,
                        name=name)

            elif country != '':
                name = "{country}-{sufix}".format(country=country, sufix=sufix)
                db_file = country
                url = "{prefix}/{continent}/{name}".format(
                        prefix=prefix,
                        continent=continent,
                        name=name)

            else:
                name = "{continent}-{sufix}".format(continent=continent, sufix=sufix)
                db_file = continent
                url = "{prefix}/{name}".format(
                        prefix=prefix,
                        name=name)

            return {'name' : name, 'db_file' : db_file, 'url' : url }


        # ____________ define variables
        self.name = set_Att()['name']
        self.url = set_Att()['url']
        self.db = set_Att()['db_file'].replace("-", "_")
        self.inFile_Full = inDir + self.name
        self.outFile_Full = outDir + self.db

        self.mapping = do_Mapping ()
        self.db_prefix = 'osm'
        self.db_tables = tables

        self.outDB_Full = "{folder}BK_{file}_{time}".format(
            folder = outDir,
            time = time.strftime("%Y_%m_%d_%H_%M"),
            file = self.db
        )



#         General parameters
#==============================================================================
db_PostGIS = {
    "dbname" :  'postgres',
    "user"  :   'postgres',
    "host"  :   'localhost',
    "pwd"   :   'rosana',
    "port"  :   '5432',
    'sslmode':  'allow',
    'prefix' :  'osm_new_',
    'proj'  :   '3857',
}

folder = {
    'OSM':  Mapsets ('OSM', '01'),

}


OSM = {
    # 'lines':Files('linestring', 'lines', '', folder['FARM'].mapset, folder['FARM'].inDir, folder['FARM'].outDir),
    # 'points':Files('point', 'points', '', folder['FARM'].mapset, folder['FARM'].inDir, folder['FARM'].outDir),
    # 'areas':Files('MultiPolygon', 'areas', '', folder['FARM'].mapset, folder['FARM'].inDir, folder['FARM'].outDir),
    # 'landuse':Files('MultiPolygon', 'landuse', '', folder['FARM'].mapset, folder['FARM'].inDir, folder['FARM'].outDir),
    # 'buildings':Files('MultiPolygon', 'buildings', '', folder['FARM'].mapset, folder['FARM'].inDir, folder['FARM'].outDir),
    'power':Files('linestring', 'power', '', folder['OSM'].mapset, folder['OSM'].inDir, folder['OSM'].outDir),
    'roads':Files('linestring', 'roads', '', folder['OSM'].mapset, folder['OSM'].inDir, folder['OSM'].outDir),
}

raw_OSM = {
    # 'burkina': OSM_obj (continent='africa', country='burkina-faso', region='', proj='4326', tables=OSM.keys()),
    'ethiopia': OSM_obj (continent='africa', country='ethiopia', region='', proj='4326', tables=OSM.keys()),


}
