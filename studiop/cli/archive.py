import argparse
import json
import pathlib
from typing import Dict, List

from studiop.constants import READ, UTF_8
from studiop.sdk import archive, backend, encrypt, tasks


def setup() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dry-run", action="store_true", help="Enable dry run of operations"
    )
    parser.add_argument("--debug", action="store_true", help="Enable debug logging"),
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
        "encryptor": encrypt.TinkCryptor(**config.get("encryptor", None)),
    }
    # tasklist = []
    # for entry in config["tasks"]:
    # if entry["recursive"]:
    #     entry.pop("recursive")
    #     source = pathlib.Path(entry["source"])
    #     for item in source.iterdir():
    #         if item not in entry["exclude"] and item.is_dir():
    #             tasklist.append(
    #                 tasks.ArchiveTask(
    #                     item, f"{entry['dest']}/{source.name}", **kwargs
    #                 )
    #             )
    # else:
    #     tasklist.append(tasks.ArchiveTask(**entry, **kwargs))
    tasklist = [tasks.ArchiveTask(**entry, **kwargs) for entry in config["tasks"]]
    return tasklist


def main():
    args = setup()
    with args.config_file.open(READ, encoding=UTF_8) as config_file:
        config = json.load(config_file)

    for task in create_tasks(config):
        task.run()


if __name__ == "__main__":
    main()
