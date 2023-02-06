import json
from pathlib import Path


def read_geojson(path: Path):
    data = None
    with open(path, 'r') as input:
        data = json.load(input)
        input.close()
    return data
