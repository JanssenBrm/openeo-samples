from openeo import Connection
from openeo.api.process import Parameter
import argparse
import json
from openeo.rest.vectorcube import VectorCube
from openeo.rest.datacube import DataCube
from openeo.internal.graph_building import PGNode
from openeo.rest.udp import build_process_dict
from pathlib import Path
import logging
######
import openeo
import openeo.processes
import pandas as pd
import geopandas as gpd
import rasterio as ra
import fiona
from rasterio.plot import show
import numpy as np
import os
import xarray as xr

fiona.drvsupport.supported_drivers['KML'] = 'rw'
current_subprocs = set()
sighandlerset = False
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)-8s %(message)s')
_log = logging.getLogger("adjustdem")
GEOJSON_TYPE = {"type": "object", "subtype": "geojson"}


def read_udf(name: str):
    data = None
    with open(Path('udf') / name, 'r') as input:
        data = input.read()
        input.close()
    return data


def read_file(path) -> dict:
    """
    Read a GeoJSON file from disk
    :param path: Path to the GeoJSON file
    :return: A dictionary representing the contents of the GeoJSON file
    """
    with open(path) as f:
        geom = json.load(f)
        f.close()
    return geom


def get_dem_input_datacube(conn: Connection, features: VectorCube) -> DataCube:
    """
    Get input DEM datacube from Terrascope
    :param conn: Existing connection to OpenEO backend
    :param bbox: Feature collection for corners of bounding box to limit field size
    :return: DataCube of DEM inside area of bounding box
    """
    dem_dc = conn \
        .load_collection('COPERNICUS_30') \
        .max_time() \
        .mask_polygon(features)

    return dem_dc


def get_geoid_input_datacube(conn: Connection, features: VectorCube) -> DataCube:
    """
    Get input geoid datacube from Terrascope
    :param conn: Existing connection to OpenEO backend
    :param bbox: Feature collection for corners of bounding box to limit field size
    :return: DataCube of WGS84 to EGM2008 geoid inside area of bounding box
    """
    geoid_dc = conn \
        .load_collection('EGM2008') \
        .max_time() \
        .mask_polygon(features)

    return geoid_dc


def get_dem(conn: Connection, features: dict, res: float) -> DataCube:
    """
    Fetch and merge datacubes, resampling to local UTM
    :param conn: Existing connection to OpenEO backend
    :param bbox: Feature collection for corners of bounding box to limit field size
    :param utm: UTM zone of field
    :param res: desired resolution of final DEM
    :return: merged DataCube in local UTM coordinates inside area of bounding box
    """

    _log.debug('Loading input DataCubes.')
    dem = get_dem_input_datacube(conn=conn, features=features)
    geoid = get_geoid_input_datacube(conn=conn, features=features)

    # resample geoid to match resolution of DEM
    geoid = geoid.resample_cube_spatial(target=dem, method='bilinear')

    # merge DataCubes and sum the layers
    merged_dem = dem.merge_cubes(geoid)
    merged_dem = merged_dem.reduce_dimension(dimension='bands', reducer='sum')
    merged_dem = merged_dem.add_dimension(name='bands', type='bands', label='DEM')

    # resample to higher resolution, CRS 3857 for metre-based calculations
    merged_dem = merged_dem.resample_spatial(resolution=res, projection=3857, method='bilinear')

    return merged_dem


def extract_point_alts(merged_dem, gcp):
    # extract merged DEM coordinates
    merged_extract = merged_dem.aggregate_spatial(geometries=gcp, target_dimension='gpc_median', reducer='median')
    # merged_extract = merged_extract.execute()
    return merged_extract


def generate_dem(conn: Connection, field: dict, gcp: dict, publish: bool = False):
    _log.info('Starting the generation of adjusted DEM.')

    _log.debug('Generating bounding box from field boundary.')
    field_bbox = create_bbox(conn, field, publish)

    _log.debug('Getting merged DEM.')
    merged_dem = get_dem(conn=conn, features=field_bbox, res=1.0)

    _log.debug('Applying UDF to merged DEM.')
    geoms = merged_dem.apply(process=openeo.UDF(read_udf('adjust_dem.py'),
                                                context={
                                                    'gcp': gcp if not publish else {
                                                        "from_parameter": "gcp",
                                                    }}))
    # geoms = geoms.resample_spatial(projection=4326,method='bilinear')
    return geoms


def publish_processing_graph(conn: Connection):
    field_param = Parameter(name="field", description="field", schema=GEOJSON_TYPE)

    gcp_param = Parameter(name="gcp", description="gcp", schema=GEOJSON_TYPE)

    process_id = 'test_DEM_gen'
    description = 'test_desc'
    process = generate_dem(conn=conn, field=field_param, gcp=gcp_param, publish=True)

    conn.save_user_defined_process(
        process_id,
        process.flat_graph(),
        description=description,
        parameters=[field_param, gcp_param],
        public=False
    )

    process_graph = build_process_dict(
        process_id=process_id,
        description=description,
        process_graph=process,
        parameters=[field_param, gcp_param]
    )

    return process_graph


def get_script_args():
    parser = argparse.ArgumentParser()

    # DEM input parameters
    parser.add_argument("input_field", type=str,
                        help="Path to input GeoJSON file with field polygon")
    parser.add_argument("input_gcp", type=str,
                        help="Path to GeoJSON file with x, y, z coordinates from GCP measurement")
    parser.add_argument("output", type=str, default='.',
                        help="Path to output folder to save result file")

    # General actions of the script
    parser.add_argument("-p", "--publish", action='store_true',
                        help="Publish the yield potential map service")
    parser.add_argument("-b", "--batch", action='store_true',
                        help="Execute the service in batch mode")
    parser.add_argument("-e", "--execute", action='store_true',
                        help="Execute the yield potential map service")
    parser.add_argument("-v", "--verbose", action='store_true',
                        help="Verbose output logging")

    # optional input parameters
    parser.add_argument("-r", "-resolution", type=float, default=1.0,
                        help="Resolution for final output DEM")

    return parser.parse_args()


def setup_openeo():
    return openeo.connect("http://openeo.vito.be").authenticate_oidc()


def create_bbox(connection, features: dict, publish: bool = False) -> VectorCube:
    return VectorCube(connection=connection,
                      graph=PGNode(
                          process_id='run_udf',
                          arguments={
                              'runtime': 'Python',
                              'udf': read_udf('create_bbox.py'),
                              'data': features if not publish else {
                                  "from_parameter": "field"
                              }
                          }
                      ))


if __name__ == '__main__':

    args = get_script_args()
    conn = setup_openeo()

    if args.publish:
        publish_processing_graph(conn)

    else:
        field = read_file(args.input_field)
        gcp = read_file(args.input_gcp)
        # test_dem = conn.datacube_from_process(process_id='test_DEM_gen',
        #                                        field=field, gcp=gcp)
        test_dem = generate_dem(conn, field, gcp, False)
        if args.batch:
            job = test_dem.create_job(out_format="NetCDF", title=f'test_DEM_gen Map',
                                      job_options={
                                          "driver-memory": "5G",
                                          "driver-memoryOverhead": "2G",
                                          "driver-cores": "2",
                                          "executor-memory": "3G",
                                          "executor-memoryOverhead": "4G",
                                          "executor-cores": "4",
                                          "max-executors": "200",
                                          "task-cpus": "4"
                                      })
            job.start_and_wait().get_results().download_files()
        else:
            output = Path(f'./result.tiff')
            test_dem.download(output, format='gtiff')
