import abc
import tempfile
from typing import BinaryIO

import tink
from studiop import logging
from tink import streaming_aead


class Encryptor(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def encrypt(self, data: BinaryIO) -> BinaryIO:
        raise NotImplementedError


class TinkEncryptor(Encryptor):
    def __init__(self, key: str) -> None:
        super().__init__()
        self._key = key
        self._logger = logging.getLogger(self.__class__.__name__)

    def encrypt(self, data: BinaryIO) -> BinaryIO:
        self._logger.info("Encrypting data stream")
        with tempfile.TemporaryFile() as output:
            return output
