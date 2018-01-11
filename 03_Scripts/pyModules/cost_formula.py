import os
from variables import *

def transportation_costs(manure_resources, crop_resources, manure_distance, crop_distance):

    manure_tonnes="COALESCE({0},0)".format(manure_resources)
    crop_tonnes="COALESCE({0},0)".format(crop_resources)
    distance_1="COALESCE({0},0) / 1000".format(manure_distance)
    distance_2="COALESCE({0},0) / 1000".format(crop_distance)
    harvest=SQL_costs['harvest']
    ensiling_km=SQL_costs['ensiling_km']
    ensiling_loading=SQL_costs['ensiling_loading']
    manure_km=SQL_costs['manure_km']
    manure_loading=SQL_costs['manure_loading']

    cost_harvest = "({crop_tonnes} * {harvest})".format(crop_tonnes=crop_tonnes, harvest=harvest)

    cost_ensiling = "({crop_tonnes} * {ensiling_loading}) + ({crop_tonnes} * {ensiling_km} * {distance})".format(
        crop_tonnes=crop_tonnes,
        ensiling_loading=ensiling_loading,
        ensiling_km=ensiling_km,
        distance=distance_1)

    cost_manure = "({manure_tonnes} * {manure_loading}) + ({manure_tonnes} * {manure_km} * {distance})".format(
        manure_tonnes=manure_tonnes,
        manure_loading=manure_loading,
        manure_km=manure_km,
        distance=distance_2)

    return {
        "cost_harvest": cost_harvest,
        "cost_ensiling": cost_ensiling,
        "cost_manure": cost_manure
    }


def transportation_costs_OLD (manure_resources, crop_resources, manure_distance, crop_distance):

    """
    to be deleted
    """

    # First parameters
    costs_ = {
        'harvest': 5,
        'ensiling': 1,
        'manure': 0.5,
        'manure_fixed': 2,
    }

    manure="COALESCE({0},0)".format(manure_resources)
    crop="COALESCE({0},0)".format(crop_resources)
    distance_manure="{0} / 1000".format(manure_distance)
    distance_crop="{0} / 1000".format(crop_distance)
    harvest=costs_['harvest']
    ensiling=costs_['ensiling']
    km=costs_['manure']
    fixed=costs_['manure_fixed']

    cost_harvest = "({crop} * {harvest})".format(crop=crop, harvest=harvest)

    cost_ensiling = "({crop} * {ensiling} * {distance})".format(crop=crop, ensiling=ensiling, distance=distance_crop)

    cost_manure = "({manure} * ({fixed} + ({km}  * ({distance}))) )".format(manure=manure, fixed=fixed, km=km, distance=distance_manure)

    return {
        "cost_harvest": cost_harvest,
        "cost_ensiling": cost_ensiling,
        "cost_manure": cost_manure
    }
