import json
from typing import BinaryIO


def print_dict(dictionary: dict) -> str:
    return json.dumps(dictionary, indent=2)
