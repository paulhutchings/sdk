import abc
import fnmatch
import pathlib
import re
import tarfile
import tempfile
from typing import BinaryIO, Callable, Iterable, List, Union

from studiop import DRY_RUN, logging
from studiop.constants import BYTE, KILOBYTE, MEGABYTE
from tqdm import tqdm


def create_filter(exclude: Iterable[str] = ()):
    if exclude:
        exclude_patterns = [fnmatch.translate(exp) for exp in exclude]
        exclude_pattern = re.compile("|".join(exclude_patterns))

    def filter_func(item: tarfile.TarInfo) -> tarfile.TarInfo:
        if exclude and exclude_pattern.match(item.name):
            return None
        else:
            print(f"Adding item: {item.name}")
            return None if DRY_RUN and item.isfile() else item

    return filter_func


class Archiver(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def archive(
        self, src: Union[str, pathlib.Path], exclude: List[str] = None
    ) -> BinaryIO:
        raise NotImplementedError

    @abc.abstractmethod
    def unarchive(self, data: BinaryIO, dest: Union[str, pathlib.Path]):
        raise NotImplementedError


class TarArchiver(Archiver):
    def __init__(self, compression: str = "") -> None:
        super().__init__()
        self.compression = compression
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
        with tarfile.open(fileobj=tarstream, mode=f"w|{self.compression}") as tar:
            tar.add(src, arcname=src.name, filter=create_filter(exclude))
        return tarstream

    def unarchive(self, data: BinaryIO, dest: Union[str, pathlib.Path]):
        self._logger.info(f"Extracting to {dest}")
        size = data.tell()
        data.seek(0)
        with tarfile.open(fileobj=data, mode=f"r|{self.compression}") as tar:
            with tqdm(
                total=size,
                unit=BYTE,
                unit_divisor=KILOBYTE,
                unit_scale=True,
            ) as progress:
                tar.extractall(dest, members=tar_tracker(tar, progress.update))


def tar_tracker(archive: tarfile.TarFile, func: Callable):
    for member in archive:
        yield member
        func(member.size)
