import argparse
import json
import pathlib
from typing import Dict, List

from studiop.sdk import archive, backend, encrypt, tasks


def setup() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config-file",
        type=pathlib.Path,
        default="archive.config.json",
        help="Path to config file to use for archiving",
    )
    args = parser.parse_args()
    if not args.config_file.exists():
        raise FileNotFoundError(args.config_file)
    return args


def create_tasks(config: Dict) -> List[tasks.Task]:
    kwargs = {
        "backend": backend.S3Backend(**config["backend"]),
        "archiver": archive.TarArchiver(**config["archiver"]),
        "decryptor": encrypt.TinkCryptor(**config.get("encryptor", None)),
    }
    tasklist = [tasks.UnarchiveTask(**entry, **kwargs) for entry in config["tasks"]]
    return tasklist


def main():
    args = setup()
    with args.config_file.open("r", encoding="utf-8") as config_file:
        config = json.load(config_file)

    for task in create_tasks(config):
        task.run()


if __name__ == "__main__":
    main()
