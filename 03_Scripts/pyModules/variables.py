#!/usr/bin/env python
import os, sys, time
from pathlib import Path
from collections import OrderedDict

script_Folder = os.getcwd() + '/'
location = str(Path(script_Folder).parent).replace('\\', '/') + '/'
project = os.getcwd().split(os.sep)[2].replace('\\', '/')
inDir = location + '01_Input'
outDir = location + '02_Output'
imposm3_cache = script_Folder + 'cache'
yml_folder = script_Folder + 'styles'
log_folder = script_Folder + 'log'
pg_win = "C:/Program Files/PostgreSQL/9.5/bin/"


#_________Check OS
if sys.platform.startswith('linux'):
    platform = 'linux'
elif sys.platform.startswith('win'):
    platform = 'windows'
else:
    raise OSError('Platform not configured.')

print "platform = {0}".format(platform)

def get_DB (continent, country, region):

    prefix = 'http://download.geofabrik.de'
    sufix = 'latest.osm.pbf'

    if region != '':
        table_name = "{region}-{sufix}".format(region=region, sufix=sufix)
        url = "{prefix}/{continent}/{country}/{name}".format(
                prefix=prefix,
                continent=continent,
                country=country,
                name=table_name)

    elif country != '':
        table_name = "{country}-{sufix}".format(country=country, sufix=sufix)
        url = "{prefix}/{continent}/{name}".format(
                prefix=prefix,
                continent=continent,
                name=table_name)

    else:
        table_name = "{continent}-{sufix}".format(continent=continent, sufix=sufix)
        url = "{prefix}/{name}".format(
                prefix=prefix,
                name=table_name)

    return {'osm_name' : table_name, 'url' : url }

def do_Mapping ():

	mapping_File = "mapping.yml"
	head_File = "head.yml"

	os.chdir(yml_folder)

	with open (mapping_File, 'w') as outFile:

	    with open (head_File) as head:
	        outFile.write(head.read())

	    for table in OSM_tables:

	        yml_File = "{file}.yml".format(file=table)
	        with open (yml_File) as inFile:
	            outFile.write(inFile.read())

	os.chdir(script_Folder)

	return "{folder}/{mapping}".format(folder=yml_folder, mapping=mapping_File)

class Mapsets:

    def __init__(self, name, number):

        folder = number + '_' + name
        mapset = "M" + folder

        self.folder = folder
        self.mapset = mapset
        self.inDir = inDir + '/' + self.folder + '/'
        self.outDir = outDir + '/' + self.folder + '/'

        if not os.path.exists(self.inDir):
            os.makedirs(self.inDir)

        if not os.path.exists(self.outDir):
            os.makedirs(self.outDir)

class Files:

    class_counter= 0

    def getName(self):
        return self.__class__.__name__

    def __init__(self, ID, inFile, nameFile, outFile, mapset, inDir, outDir, **kargs):

        if Files.class_counter < 0:    #skip numbering
            self.ID = ID
        else:
            self.ID = ID + '_' + str(Files.class_counter)

        self.inFile = inFile
        self.name = self.ID + '_' + nameFile
        self.nameAt = self.name  + '@' + mapset
        self.outFile = outFile
        self.mapset = mapset
        self.inDir = inDir
        self.outDir = outDir
        self.inFile_Full = inDir + inFile
        self.outFile_Full = outDir + outFile
        self.parameter = kargs

        Files.class_counter += 1

class OSM_obj:

    def __init__(self, continent, country, region, proj, **parameter):

		db_Parameters = get_DB (continent, country, region)

		self.osm_name = db_Parameters ['osm_name']
		self.url = db_Parameters ['url']
		self.outFile =  db_PostGIS['dbname'] + '_' + time.strftime("%Y_%m_%d_%H_%M")
		self.proj = proj
		self.mapping = do_Mapping ()
		self.parameter = parameter

class SQL_obj:

    def __init__(self, name, from_, description, *parameter):

		self.name = name
		self.from_ = from_
		self.description = description


folder = {
    'DB':  Mapsets ('DB', '00'),
    'OSM':  Mapsets ('OSM', '01'),
    'ADM':  Mapsets ('ADM', '02'),
    'LULC':  Mapsets ('LULC', '03'),
    'FARM':  Mapsets ('FARM', '04'),
    'DEM':  Mapsets ('DEM', '05'),

}

db_PostGIS = {
    "dbname" :  'amstetten_biogas',
    "host"  :   'localhost',
    "user"  :   'postgres',
    "pwd"   :   'rosana',
    # "pwd"   :   'postgres',
    "port"  :   '5432',
    'proj'  :   '3035',
    'cursor'  :  '',
    'log_sql'  :  'PG_queries.sql',
}

OSM_tables = {
    'lines':'linestring',
    'points':'point',
    'areas':'MultiPolygon',
    'landuse':'MultiPolygon',
    'buildings':'MultiPolygon',
    'power':'linestring',
    'roads':'linestring',
}

OSM_raw = {
    'austria': OSM_obj (continent='europe', country='austria', region='', proj='3035'),
}

bbox_OSM = {
    'JSON':Files('', 'BBOX_EPSG4326.geojson', '', '', folder['ADM'].mapset, folder['ADM'].inDir, folder['ADM'].outDir),
}

SQL_roads = {
	'osm': SQL_obj ('osm_roads', '', 'osm roads'),
	'roads_dissolved' : SQL_obj ('osm_roads_dissolved', 'osm_roads', 'osm roads dissolved - important for snapping the points'),
	'roads_main' : SQL_obj ('roads_main', 'osm_roads', 'selection of main roads'),
}

SQL_distances = {
    'target': 500,
    'cluster': 100,
    'lulc_zones': 1000,
    'osm': 100,
    'tolerance': 0.01,
    'max_travel': 50000,
    'manure': 10000,
    'features': '2000',
    'criteria': 'id_target > 1500 AND id_target <= 2000',
}


SQL_target = {
	'site_targets' : SQL_obj ('site_targets', '', 'points equally dristributed over the main roads'),
	'site_clean' : SQL_obj ('site_clean', '', 'points equally dristributed over the main roads'),
	'lulc_zones' : SQL_obj ('lulc_zones', '', 'preferenciably zones for biogas plants based on lulc'),
}

SQL_buildings = {
	'buildings' : SQL_obj ('farm_buildings', '', 'buildings extracted from OSM'),
	'centroids' : SQL_obj ('farm_buildings_centroids', '', 'building centroids'),
	'centroids_cluster' : SQL_obj ('farm_buildings_centroids_cluster', '', 'cluster of centroids'),
	'buildings_cluster' : SQL_obj ('farm_buildings_cluster', '', 'cluster of centroids'),
	'location' : SQL_obj ('farm_buildings_location', '', 'final location of the clustered buildings'),
}

SQL_farms = {
	'rank' : SQL_obj ('farm_rank_by_mun', '', 'rank of farms by id_mun'),
	'resources' : SQL_obj ('farm_resources', '', 'create  of farms based'),
}



SQL_topology= {
	'targets' : SQL_obj ('topo_targets', '', 'targets and farms merged'),
	'roads' : SQL_obj ('topo_roads', '', 'segmented OSM roads'),
	'noded' : SQL_obj ('topo_roads_noded', '', 'topology of main roads'),
	'roads_ver' : SQL_obj ('topo_roads_vertices_pgr', '', 'output of pgr_createTopology'),
	'noded_ver' : SQL_obj ('topo_roads_noded_vertices_pgr', '', 'output of pgr_createTopology'),
}

SQL_route= {
	'route' : SQL_obj ('route_distance', '', 'shortest distance'),
	'targets' : SQL_obj ('route_targets', '', 'targets to be used on routing'),
	'nodes' : SQL_obj ('route_node_ids', '', 'node ids'),
}

SQL_plants= {
	'initial' : SQL_obj ('plants_initial', '', 'merge of route tables '),
	'resources' : SQL_obj ('plants_resources', '', 'join of resources '),
	'methane' : SQL_obj ('plants_methane_available', '', 'methane available on each farm '),
	'capacity' : SQL_obj ('plants_capacity', '', 'capacity calculation '),
	'cost' : SQL_obj ('plants_costs', '', 'cost calculation '),
	'manure' : SQL_obj ('plants_manure_demand', '', 'manure for feeding the plants '),
	'crop' : SQL_obj ('plants_crop_demand', '', 'crop for feeding the plants '),
	'cost_total' : SQL_obj ('plants_costs_total', '', 'total cost '),
	# 'grouped' : SQL_obj ('plants_resources_grouped', '', 'join of resoureces '),

}

SQL_plant_capacity = {
    '250': 560000,
    '500': 1070000,
    '750': 1560000,
}

SQL_methane_ratio = {
    'manure': 0.3,
    'crop': 0.7,
}

SQL_costs = {
    'harvest': 5,
    'ensiling': 6,
    'manure': 0.2,
}

prefix = 'osm'
Files.class_counter = -99
OSM_Files = OrderedDict({
    'pbf':Files(prefix, OSM_raw['austria'].osm_name, OSM_raw['austria'].osm_name, OSM_raw['austria'].osm_name, folder['OSM'].mapset, folder['OSM'].inDir, folder['OSM'].outDir),
})


prefix = 'adm'
Files.class_counter = -99
ADM = OrderedDict({
    'bbox_3035':Files(prefix, 'BBOX_EPSG3035.shp', 'bbox', 'bbox', folder['ADM'].mapset, folder['ADM'].inDir, folder['ADM'].outDir),
    'bbox_4326_json':Files(prefix, 'BBOX_EPSG4326.geojson', 'bbox_4326', 'bbox_4326', folder['ADM'].mapset, folder['ADM'].inDir, folder['ADM'].outDir),
    'communes':Files(prefix, 'Amstetten_Comunes_20170101_EPSG_3035.shp', 'communes', 'communes', folder['ADM'].mapset, folder['ADM'].inDir, folder['ADM'].outDir),
})


prefix = 'lulc'
Files.class_counter = -99
LULC = OrderedDict({
    'corine_bbox':Files(prefix, 'Corine_2012_BBOX_EPSG3035.shp', 'corine_12_bbox', 'corine_12_bbox', folder['LULC'].mapset, folder['LULC'].inDir, folder['LULC'].outDir),
    'corine_adm':Files(prefix, '', 'corine_12_adm', 'corine_12_adm', folder['LULC'].mapset, folder['LULC'].inDir, folder['LULC'].outDir),
    'corine_crop':Files(prefix, '', 'corine_crop', 'corine_crop', folder['LULC'].mapset, folder['LULC'].inDir, folder['LULC'].outDir),
    'corine_No_crop':Files(prefix, '', 'corine_No_crop', 'corine_No_crop', folder['LULC'].mapset, folder['LULC'].inDir, folder['LULC'].outDir),
    'legend':Files(prefix, 'clc_legend.csv', 'legend_12', 'legend_12', folder['LULC'].mapset, folder['LULC'].inDir, folder['LULC'].outDir, columns='num,code,label3,color'),

})

prefix = 'farm'
Files.class_counter = -99
FARM = OrderedDict({
    'parameter':Files(ID=prefix, inFile='biogas_potential.xlsx', nameFile='parameters', outFile='parameters', mapset=folder['FARM'].mapset, inDir=folder['FARM'].inDir, outDir=folder['FARM'].outDir, pk = None, fk= None  ),
    'id_livestock':Files(prefix, 'biogas_potential.xlsx', 'id_livestock', 'id_livestock', folder['FARM'].mapset, folder['FARM'].inDir, folder['FARM'].outDir, pk = 'id_livestock', fk= None),
    'id_agriculture':Files(prefix, 'biogas_potential.xlsx', 'id_agriculture', 'id_agriculture', folder['FARM'].mapset, folder['FARM'].inDir, folder['FARM'].outDir, pk = 'id_agriculture', fk= None),

    'heads':Files(prefix, 'biogas_potential.xlsx', 'heads', 'heads', folder['FARM'].mapset, folder['FARM'].inDir, folder['FARM'].outDir, pk = 'id_heads', fk= 'id_livestock'),
    'lsu':Files(prefix, 'biogas_potential.xlsx', 'lsu', 'lsu', folder['FARM'].mapset, folder['FARM'].inDir, folder['FARM'].outDir, pk = 'id_heads', fk= 'id_livestock'),
    'manure':Files(prefix, 'biogas_potential.xlsx', 'manure', 'manure', folder['FARM'].mapset, folder['FARM'].inDir, folder['FARM'].outDir, pk = 'id_heads', fk= 'id_livestock'),
    'methane':Files(prefix, 'biogas_potential.xlsx', 'methane', 'methane', folder['FARM'].mapset, folder['FARM'].inDir, folder['FARM'].outDir, pk = 'id_heads', fk= 'id_livestock'),

    'crop_area':Files(prefix, 'biogas_potential.xlsx', 'crop_area', 'crop_area', folder['FARM'].mapset, folder['FARM'].inDir, folder['FARM'].outDir, pk = 'id_crops', fk= 'id_agriculture'),
    'crop_production':Files(prefix, 'biogas_potential.xlsx', 'crop_production', 'crop_production', folder['FARM'].mapset, folder['FARM'].inDir, folder['FARM'].outDir, pk = 'id_crops', fk= 'id_agriculture'),
    'crop_methane':Files(prefix, 'biogas_potential.xlsx', 'crop_methane', 'crop_methane', folder['FARM'].mapset, folder['FARM'].inDir, folder['FARM'].outDir, pk = 'id_crops', fk= 'id_agriculture'),

})
