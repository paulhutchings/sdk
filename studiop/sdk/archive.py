import abc
import fnmatch
import os
import pathlib
import re
import tarfile
import tempfile
from typing import BinaryIO, Iterable, List, Union

from studiop import DRY_RUN, logging


def create_filter(exclude: Iterable[str] = ()):
    exclude_patterns = [fnmatch.translate(exp) for exp in exclude]
    exclude_pattern = re.compile("|".join(exclude_patterns))

    def filter_func(item: tarfile.TarInfo) -> tarfile.TarInfo:
        if exclude_pattern.match(item.name):
            return None
        else:
            print(f"Adding item: {item.name}")
            return None if DRY_RUN else item

    return filter_func


class Archiver(metaclass=abc.ABCMeta):
    def archive(
        self, src: Union[str, pathlib.Path], exclude: List[str] = None
    ) -> BinaryIO:
        raise NotImplementedError


class TarArchiver(Archiver):
    def __init__(self, compression: str = "") -> None:
        super().__init__()
        self._mode = f"w|{compression}"
        self._logger = logging.getLogger(self.__class__.__name__)

    def archive(
        self, src: Union[str, pathlib.Path], exclude: List[str] = None
    ) -> BinaryIO:
        self._logger.info(f"Archiving {src}")
        if exclude is None:
            exclude = []
        src = pathlib.Path(src)
        if not src.exists():
            raise FileNotFoundError(f"Source file/folder {src} does not exist")

        tarstream = tempfile.TemporaryFile()
        with tarfile.open(fileobj=tarstream, mode=self._mode) as tar:
            tar.add(src, filter=create_filter(exclude))
        return tarstream
