import abc
import pathlib
from typing import List, Union

from studiop import logging
from studiop.sdk import archive, encrypt, upload


class Task(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def run(self):
        raise NotImplementedError


class ArchiveTask(Task):
    def __init__(
        self,
        source: Union[str, pathlib.Path],
        dest: str,
        uploader: upload.Uploader,
        archiver: archive.Archiver,
        exclude: List[str] = None,
        encryptor: encrypt.Encryptor = None,
    ) -> None:
        self.src = pathlib.Path(source)
        if not self.src.exists():
            raise FileNotFoundError(f"Source file/folder '{source}' does not exist")
        self.dest = dest
        self._uploader = uploader
        self._logger = logging.getLogger(self.__class__.__name__)
        self.exclude = exclude
        self._archiver = archiver
        self._encryptor = encryptor

    def run(self):
        self._logger.info(f"Started archive task: {self.src}")
        archived = self._archiver.archive(self.src, self.exclude)
        if self._encryptor:
            archived = self._encryptor.encrypt(archived)
        self._uploader.upload(self.src.name, archived)
        self._logger.info(f"Completed archive task: {self.src}")
