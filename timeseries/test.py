import argparse
import json
import logging
import time
from pathlib import Path

import openeo
import requests

from utils import read_geojson

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)-8s %(message)s')

#HOST = 'http://epod-openeo-1.vgt.vito.be:8080'
HOST = 'http://epod-openeo-dev.vgt.vito.be:8080'

def get_script_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("-c", "--collection", required=True,
                        help="Collection for which to test the time series")
    parser.add_argument("-f", "--file", required=True,
                        help="GeoJSON based file that contains the input fields to process")
    parser.add_argument("-s", "--start", required=True,
                        help="Start date of the interval for which to generate the variability maps")
    parser.add_argument("-e", "--end", required=True,
                        help="End date of the interval for which to generate the variability maps")
    return parser.parse_args()


def exec_ts_request(collection, multiband, geometry, start, end):
    url = f'{HOST}/v1.0/ts/{collection}/geometry{"/multiband" if multiband else ""}?startDate={start}&endDate={end}'
    print(url)
    response = requests.post(url, json=geometry)

    if response.status_code != 200:
        raise Exception(f'Could not execute request: {response.status_code} - {response.text}')
    return response.json()


def write_result(filename, values, timing):
    with open(filename, 'w') as output:
        json.dump(fp=output, obj={
            'values': values,
            'duration': f'{timing:0.4f}'
        })
        output.close()

if __name__ == '__main__':

    args = get_script_args()

    features = read_geojson(Path(args.file))

    for feature in features['features']:
        tic = time.perf_counter()
        result = exec_ts_request(args.collection, False, feature['geometry'], args.start, args.end)
        toc = time.perf_counter()
        write_result(f'./{args.collection}.json', result, toc - tic)




