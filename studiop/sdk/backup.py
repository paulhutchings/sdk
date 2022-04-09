import pathlib
from typing import Union

import dotenv
import restic
from studiop import logging
from studiop.sdk import utils

logger = logging.getLogger(__name__)

SUMMARY_KEYS = [
    "files_new",
    "file_changed",
    "dirs_new",
    "dirs_changed",
    "data_added",
    "total_duration",
    "snapshot_id",
]


def parse_summary(summary: dict) -> dict:
    return {k: v for k, v in summary.items() if k in SUMMARY_KEYS}


class BackupTask:
    def __init__(
        self,
        src: Union[str, pathlib.Path],
        dest: Union[str, pathlib.Path],
        exclude: list[str] = None,
    ) -> None:
        self.src = src
        self.dest = (dest,)
        self.exclude = exclude
        self._logger = logging.getLogger(self.__class__.__name__)

    def run(self):
        pass


class Backup(BackupTask):
    def __init__(
        self,
        src: Union[str, pathlib.Path],
        dest: Union[str, pathlib.Path],
        exclude: list[str] = None,
    ) -> None:
        super().__init__(src, dest, exclude=exclude)
        restic.repository = self.dest

    def run(self):
        self._logger.info("Starting backup")
        kwargs = {"paths": [self.src]}
        if self.exclude:
            kwargs["exclude_patterns"] = self.exclude
        summary = restic.backup(**kwargs)
        summary = parse_summary(summary)
        self._logger.info("Backup task finished")
        self._logger.info(utils.print_dict(summary))


class BackupSync(BackupTask):
    def __init__(
        self,
        src: Union[str, pathlib.Path],
        dest: Union[str, pathlib.Path],
        exclude: list[str] = None,
    ) -> None:
        super().__init__(src, dest, exclude=exclude)
        restic.repository = self.src

    def run(self):
        self._logger.info("Starting backup sync")
        restic.copy(repo2=self.dest)
