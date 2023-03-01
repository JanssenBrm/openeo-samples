import argparse
import logging
from pathlib import Path

import openeo
from openeo.internal.graph_building import PGNode
from openeo.rest.vectorcube import VectorCube

from utils import read_geojson, read_udf

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)-8s %(message)s')

def get_script_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("-f", "--file", required=True,
                        help="GeoJSON based file that contains the input fields to process")
    parser.add_argument("-b", "--batch", action='store_true',
                        help="Generate the results in batch mode")
    parser.add_argument("-o", "--output", default='../files/dem',
                        help="File path where to store the results")
    return parser.parse_args()


def setup_openeo():
    return openeo.connect("http://openeo.vito.be").authenticate_oidc()


def create_bbox(connection, features: dict) -> VectorCube:
    return VectorCube(connection=connection,
                      graph=PGNode(
                          process_id='run_udf',
                          arguments={
                              'runtime': 'Python',
                              'udf': read_udf('create_bbox.py'),
                              'data': features
                          }
                      ))

if __name__ == '__main__':

    args = get_script_args()

    features = read_geojson(Path(args.file))
    connection = setup_openeo()
    features = create_bbox(connection, features)
    dc = connection.load_collection('SENTINEL2_L2A_SENTINELHUB',
                                    bands=["B03", "B04", "B08", "sunAzimuthAngles", "sunZenithAngles", "viewAzimuthMean",
                                           "viewZenithMean", 'SCL']) \
        .filter_temporal(['2023-01-01', '2023-01-10'])
    dc = dc.mask_polygon(features)


    if args.batch:
        dc_job = dc.send_job(out_format="GTiff", title=f'Variability Map',
                                       sample_by_feature=False)
        dc_job.start_and_wait().get_results().download_files(args.output)
    else:
        output = Path(f'{args.output}/result.tiff')
        output.parent.mkdir(parents=True, exist_ok=True)
        dc.download(output, format='GTiff')



