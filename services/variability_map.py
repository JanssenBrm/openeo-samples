import argparse
import logging
from pathlib import Path

import openeo

from utils import read_geojson

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)-8s %(message)s')

def get_script_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("-f", "--file", required=True,
                        help="GeoJSON based file that contains the input fields to process")
    parser.add_argument("-s", "--start", required=True,
                        help="Start date of the interval for which to generate the variability maps")
    parser.add_argument("-e", "--end", required=True,
                        help="End date of the interval for which to generate the variability maps")
    parser.add_argument("-r", "--results", default='../files/variability',
                        help="File path where to store the results")
    return parser.parse_args()


def setup_openeo():
    return openeo.connect("http://openeo.vito.be").authenticate_oidc()

if __name__ == '__main__':

    args = get_script_args()

    features = read_geojson(Path(args.file))
    dc = setup_openeo().datacube_from_process('variability_map', namespace='vito', polygon=features, date = [args.start, args.end])

    dc_job = dc.send_job(out_format="GTiff", title=f'Variability Map',
                                   sample_by_feature=True)
    dc_job.start_and_wait().get_results().download_files(args.results)




