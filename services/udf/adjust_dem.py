
import pyproj
from openeo.udf import XarrayDataCube
import numpy as np
from shapely.geometry import Point
from shapely.ops import transform
import xarray as xr


def reproject_field(field: Point, from_proj: str, to_proj: str) -> Point:
    """
    Reproject a field from a source projection system to a destination projection system
    :param field: Field to reproject
    :param from_proj: Source projection system
    :param to_proj: Destination projection system
    :return:
    """
    from_crs = pyproj.CRS(from_proj)
    to_crs = pyproj.CRS(to_proj)
    project = pyproj.Transformer.from_crs(from_crs, to_crs, always_xy=True).transform
    return transform(project, field)


def get_diff_with_gcp(array: xr.DataArray, gcp: dict):
    """
    Get the difference between the actual DEM and a GCP point
    :param array: Raster that represents the DEM values
    :param gcp: GCP point geometry
    :return:
    """
    point = reproject_field(Point(gcp['coordinates'][:2]), 'epsg:4326', 'epsg:3857')
    values = array.sel(x=point.x, y=point.y, method='nearest').values
    if values.size == 0:
        raise Exception('Point: (' + str(point.x) + ',' + str(point.y) + ') has no value ' + str(values))
    return gcp['coordinates'][-1] - values[0]


def get_average_diff(array: xr.DataArray, gcps: dict) -> np.array:
    """
    Get the average difference between the DEM values and a list of GCP points
    :param array: Raster that represents the DEM values
    :param gcps: List of GCP points
    :return:
    """
    diffs = list(map(lambda x: get_diff_with_gcp(array, x), gcps['geometries']))
    return np.nanmedian(diffs)


def apply_datacube(cube: XarrayDataCube, context: dict) -> XarrayDataCube:

    gcp = context.get('gcp')
    array = cube.get_array()
    diff = get_average_diff(array, gcp)
    array.values = array.values + diff
    return cube
