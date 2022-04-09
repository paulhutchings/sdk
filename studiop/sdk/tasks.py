import abc
import pathlib
from typing import List, Union

from studiop import logging
from studiop.sdk import archive, backend, encrypt


class Task(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def run(self):
        raise NotImplementedError


class ArchiveTask(Task):
    def __init__(
        self,
        source: Union[str, pathlib.Path],
        backend: backend.Backend,
        archiver: archive.Archiver,
        dest: str = "",
        exclude: List[str] = None,
        encryptor: encrypt.Cryptor = None,
    ) -> None:
        super().__init__()
        self.src = pathlib.Path(source)
        if not self.src.exists():
            raise FileNotFoundError(f"Source file/folder '{source}' does not exist")
        dest_name = self.src.name.replace(" ", "_")
        self.dest = f"{dest.replace(' ', '_')}/{dest_name}" if dest else dest_name
        self._backend = backend
        self._archiver = archiver
        self.exclude = exclude
        self._encryptor = encryptor
        self._logger = logging.getLogger(self.__class__.__name__)

    def run(self):
        self._logger.info(f"Started archive task: {self.src}")
        archived = self._archiver.archive(self.src, self.exclude)
        if self._encryptor:
            archived = self._encryptor.encrypt(archived, self.dest)
        self._backend.upload(self.dest, archived)
        self._logger.info(f"Completed archive task: {self.src}")


class UnarchiveTask(Task):
    def __init__(
        self,
        source: str,
        backend: backend.Backend,
        archiver: archive.Archiver,
        dest: Union[str, pathlib.Path] = ".",
        decryptor: encrypt.Cryptor = None,
    ) -> None:
        super().__init__()
        self.key = source
        self.dest = pathlib.Path(dest)
        if not self.dest.exists():
            raise FileNotFoundError(self.dest)
        self._uploader = backend
        self._archiver = archiver
        self._decryptor = decryptor
        self._logger = logging.getLogger(self.__class__.__name__)

    def run(self):
        self._logger.info(f"Started unarchive task: {self.key}")
        downloaded = self._uploader.download(self.key)
        if self._decryptor:
            downloaded = self._decryptor.decrypt(downloaded, self.key)
        self._archiver.unarchive(downloaded, self.dest)
        self._logger.info(f"Completed unarchive task: {self.key}")
