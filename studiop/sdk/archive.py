import abc
import pathlib
import tarfile
import tempfile
from typing import BinaryIO, Union

from studiop import logging


class Archiver(metaclass=abc.ABCMeta):
    def archive(self, src: Union[str, pathlib.Path]) -> BinaryIO:
        raise NotImplementedError


class TarArchiver(Archiver):
    def __init__(self, compression: str = "") -> None:
        super().__init__()
        self._mode = f"w|{compression}"
        self._logger = logging.getLogger(self.__class__.__name__)

    def archive(self, src: Union[str, pathlib.Path]) -> BinaryIO:
        self._logger.info(f"Archiving {src}")
        src = pathlib.Path(src)
        if not src.exists():
            raise FileNotFoundError(f"Source file/folder {src} does not exist")

        with tempfile.TemporaryFile() as tarstream:
            with tarfile.open(fileobj=tarstream, mode=self._mode) as tar:
                tar.add(src, arcname=src.name)
                pass
            return tarstream
