import argparse
import json
import pathlib

from studiop.constants import GZIP, READ, UTF_8
from studiop.sdk import archive, encrypt, tasks, upload


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


def main():
    args = setup()
    with args.config_file.open(READ, encoding=UTF_8) as config_file:
        config = json.load(config_file)

    uploader = upload.S3Uploader(**config["uploader"])
    archiver = archive.TarArchiver(**config["archiver"])
    encryptor = encrypt.TinkEncryptor(**config["encryptor"])
    for entry in config["tasks"]:
        task = tasks.ArchiveTask(
            uploader=uploader, archiver=archiver, encryptor=encryptor, **entry
        )
        task.run()


if __name__ == "__main__":
    main()
