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
    parser.add_argument("-b", "--batch", action='store_true',
                        help="Generate the results in batch mode")
    parser.add_argument("-r", "--raw", action='store_true',
                        help="Generate the raw results for the variability map")
    parser.add_argument("-o", "--output", default='../files/variability',
                        help="File path where to store the results")
    return parser.parse_args()


def setup_openeo():
    return openeo.connect("http://openeo.vito.be").authenticate_oidc()

if __name__ == '__main__':

    args = get_script_args()

    features = read_geojson(Path(args.file))
    dc = setup_openeo().datacube_from_process('variability_map', namespace='https://openeo.vito.be/openeo/1.1/processes/u:bramjanssen/variability_map', polygon=features, date = [args.start, args.end], raw=args.raw)

    if args.batch:
        dc_job = dc.send_job(out_format="GTiff", title=f'Variability Map',
                                       sample_by_feature=False)
        dc_job.start_and_wait().get_results().download_files(args.output)
    else:
        output = Path(f'{args.output}/result.tiff')
        output.parent.mkdir(parents=True, exist_ok=True)
        dc.download(output, format='GTiff')



