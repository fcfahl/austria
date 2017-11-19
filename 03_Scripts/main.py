# -*- coding: utf-8 -*-
"""@author: fernando fahl"""
import os, sys, csv, subprocess, time, logging

from pyModules.variables import *
from pyModules.pg_queries import *
from pyModules.logs import *
from pyModules.postGIS import *

import pyModules.OSM_import as OSM
import pyModules.adm_lulc as ADM_LULC
import pyModules.farms as FARM
import pyModules.targets as TRG
import pyModules.topology as TOPO
import pyModules.plants as PLT



if __name__ == "__main__":

    print ""

    initialize_Log(fileName='OSM', mode='w') # w for overwrite log, a for append

    #==============================================================================
    #           PostGIS Database
    #==============================================================================

    # create_DB ()
    connect_PostGIS ()


    # create_sequence (sequence='serial')

    #==============================================================================
    #           OSM Import
    #==============================================================================

    # OSM.Step_01_download_OSM (raw=OSM_raw['austria'], osm=OSM_Files['pbf'])
    # OSM.Step_02_import_PostGIS (osm=OSM_Files['pbf'], cache=True, limit_AOI=True)
    # OSM.Step_03_reproject_Tables (tables=OSM_tables)

    #==============================================================================
    #           ADM and LULC
    #==============================================================================

    # ADM_LULC.Step_01_import_Vectors ()
    # ADM_LULC.Step_02_clip_Corine(old_LULC=LULC['corine_bbox'].name, new_LULC=LULC['corine_adm'].name, clip_table=ADM['communes'].name )
    # ADM_LULC.Step_03_join_Legend (lulc=LULC['corine_adm'].name, legend=LULC['legend'])
    # ADM_LULC.Step_04_extract_crop_Areas ()
    # ADM_LULC.Step_05_rank_LULC_Classes ()
    # ADM_LULC.Step_06_create_LULC_Zones ()

    #==============================================================================
    #           FARMS
    #==============================================================================
    # FARM.Step_00_dissolve_Roads ()
    # FARM.Step_01_import_Farm_Tables ()
    # FARM.Step_02_set_Keys ()

    # FARM.Step_03_create_Farm_Roads ()
    # FARM.Step_04_create_Target_Points ()
    # FARM.Step_05_clean_Target_Points ()
    # FARM.Step_06_rank_Target_Points ()
    # FARM.Step_07_create_Farm_Buildings ()
    # FARM.Step_08_add_communes_to_Buildings ()
    # FARM.Step_09_extract_centroids_Farm_Buildings ()
    # FARM.Step_10_cluster_Centroids ()
    # FARM.Step_11_cluster_Buildings ()
    # FARM.Step_12_extract_location_Buildings ()
    # FARM.Step_13_rank_Buildings ()

    # FARM.Step_14_rank_Farm_Tables ()
    FARM.Step_15_join_Farm_Data ()


    #==============================================================================
    #           TARGET Points
    #==============================================================================

    # TRG.Step_01_create_Farm_Roads ()
    # TRG.Step_02_create_Target_Points ()
    # TRG.Step_03_clean_Target_Points ()
    #

    #==============================================================================
    #           TOPOLOGY
    #==============================================================================

    # TOPO.Step_01_create_Topo_Nodes()
    # TOPO.Step_02_segmentize_OSM_Roads()
    # TOPO.Step_03_create_Road_Topology()
    # TOPO.Step_04_update_Topology()
    # TOPO.Step_05_create_PG_Functions()
    # TOPO.Step_06_extract_Routes()

    #==============================================================================
    #           PLANTS
    #==============================================================================

    # PLT.Step_01_merge_Routes()
    # PLT.Step_02_join_Farm_Resources()
    # PLT.Step_03_calculate_Methane()
    # PLT.Step_04_sum_Resources()
    # PLT.Step_05_calculate_Costs()
    # PLT.Step_06_aggregate_Demands()
    # PLT.Step_06_aggregate_Costs()

    #==============================================================================
    #           FINAL DATABASE
    #==============================================================================

    # outFile = folder['OSM'].outDir + OSM_raw['austria'].outFile
    # export_PostGIS(db_PostGIS['dbname'], outFile)

    # export tables instead of the whole db
    # export_PostGIS_Tables()
    # restore_PostGIS_Tables()

    # size_DB()

    log_close()
