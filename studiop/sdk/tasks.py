import abc
import pathlib
from typing import Union

from studiop import logging
from studiop.sdk import archive, encrypt, upload


class Task(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def run(self):
        raise NotImplementedError


class ArchiveTask(Task):
    def __init__(
        self,
        src: Union[str, pathlib.Path],
        dest: str,
        exclude: list[str] = None,
        encrypt_: bool = True,
        uploader: upload.Uploader = upload.S3Uploader,
        archiver: archive.Archiver = archive.TarArchiver,
    ) -> None:
        self.src = pathlib.Path(src)
        if not self.src.exists():
            raise FileNotFoundError(f"Source file/folder '{src}' does not exist")
        self.dest = dest
        self._uploader = uploader
        self.encrypt = encrypt_
        self._logger = logging.getLogger(self.__class__.__name__)
        self.exclude = exclude
        self._archiver = archiver
        if self.encrypt:
            self._encryptor = encrypt.TinkEncryptor("")

    def run(self):
        self._logger.info(f"Started archive task: {self.src}")
        archived = self._archiver.archive(self.src)
        if self.encrypt:
            archived = self._encryptor.encrypt(archived)
        self._uploader.upload(self.src.name, archived)
        self._logger.info(f"Completed archive task: {self.src}")
