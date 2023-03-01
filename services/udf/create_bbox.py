import json

import shapely
from openeo.udf import UdfData, FeatureCollection
from shapely.geometry.base import BaseGeometry, CAP_STYLE, JOIN_STYLE
from shapely.geometry import box


def create_bbox(geometry: BaseGeometry):
    buffer = 0.001
    bounds = geometry.bounds
    result = box(bounds[0], bounds[1],
               bounds[2], bounds[3]).buffer(distance=buffer, join_style=JOIN_STYLE.mitre)
    return result
def fct_buffer(udf_data: UdfData):
    """
    Execution of the UDF on a vector collection
    :param udf_data: Data containing the vector information
    :return:
    """
    feature_collection = udf_data.get_feature_collection_list()[0]
    feature_collection.data['geometry'] = feature_collection.data['geometry'].apply(lambda g: create_bbox(g))

    udf_data.set_feature_collection_list([feature_collection])

    return udf_data
