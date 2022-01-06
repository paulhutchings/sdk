import gevent.pool
import gevent.queue
from gevent import monkey

monkey.patch_all()

import fnmatch
import json
import os
import pathlib
import re
from typing import Iterable

import boto3

UTF_8 = "utf-8"
HOME = pathlib.Path().home()
SOURCE = HOME

EXCLUDE = [
    ".cache",
    ".local/share/Trash",
    ".local/share/flatpak",
    ".npm/_cacache",
    ".local/bin",
    "Downloads",
    "**/.git",
    ".config/VSCodium/Cache*",
    ".config/VSCodium/logs",
    ".local/lib/python*",
    "**/__pycache__",
    "**/.pytest_cache",
    "**/.coverage",
    "**/node_modules",
    ".local/share/icons",
    ".icons",
]

CACHE_DIR = HOME.joinpath(".cache/backup")
CACHE_FILE = CACHE_DIR.joinpath(f"{'_'.join(SOURCE.parts[1:])}.json")

FIRST_RUN = not CACHE_FILE.exists()
if FIRST_RUN:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache = {}
else:
    with CACHE_FILE.open("r", encoding=UTF_8) as fp:
        cache = json.load(fp)

session = boto3.Session(profile_name="truenas")
s3 = session.resource("s3", endpoint_url="http://truenas.studiop:9000")
bucket = s3.Bucket("test")


def scan_tree(path: pathlib.Path, excludes: Iterable = ()):
    exclude_patterns = [fnmatch.translate(f"{path}/{exp}") for exp in excludes]
    exclude_pattern = re.compile("|".join(exclude_patterns))
    for dirpath, dirnames, filenames in os.walk(path):
        current_folder = pathlib.Path(dirpath)
        for filename in filenames:
            file = current_folder.joinpath(filename)
            if not exclude_pattern.match(str(file)) and file.exists():
                yield file
        for folder in [
            dname for dname in dirnames if exclude_pattern.match(f"{dirpath}/{dname}")
        ]:
            dirnames.remove(folder)


counts = {"new": 0, "changed": 0, "unchanged": 0, "removed": 0}
changes = {"new": [], "changed": [], "removed": []}
new_cache = {}


def handle_file(file: pathlib.Path):
    stats = file.stat()
    key = str(file)
    metadata_hash = hash((stats.st_mtime, stats.st_ctime, stats.st_size, stats.st_ino))
    if key not in cache:
        new_cache[key] = metadata_hash
        print(f"New: {key}")
        counts["new"] += 1
        # if not FIRST_RUN:
        #     changes["new"].append(key)
        bucket.upload_file(Filename=key, Key=key.removeprefix(f"{SOURCE}/"))
    elif metadata_hash != cache[key]:
        new_cache[key] = metadata_hash
        print(f"Changed: {key}")
        counts["changed"] += 1
        # if not FIRST_RUN:
        #     changes["changed"].append(key)
        bucket.upload_file(Filename=key, Key=key.removeprefix(f"{SOURCE}/"))
    else:
        new_cache[key] = cache[key]
        print(f"Unchanged: {key}")
        counts["unchanged"] += 1


pool = gevent.pool.Pool(2048)
for entry in scan_tree(SOURCE, EXCLUDE):
    pool.spawn(handle_file, entry)
    # handle_file(entry)
pool.join()

counts["removed"] = len(cache.keys() - new_cache.keys())
# if not FIRST_RUN:
#     changes["removed"] = [k for k in cache.keys() - new_cache.keys()]
# bucket.delete_objects(Delete={"Objects": changes["removed"]})
with CACHE_FILE.open("w", encoding=UTF_8) as fp:
    json.dump(new_cache, fp)

print(json.dumps(counts, indent=4))
# if not FIRST_RUN:
#     print(json.dumps(changes, indent=4))
