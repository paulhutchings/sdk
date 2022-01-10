import abc
import getpass
import pathlib
import tempfile
from typing import BinaryIO, Union

import tink
from cryptography import fernet
from studiop import DRY_RUN, logging
from studiop.constants import BYTE, KILOBYTE, MEGABYTE, READ_B
from tink import TinkError, cleartext_keyset_handle, streaming_aead
from tqdm import tqdm


class Encryptor(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def encrypt(self, data: BinaryIO) -> BinaryIO:
        raise NotImplementedError

    @abc.abstractmethod
    def decrypt(self, data: BinaryIO) -> BinaryIO:
        raise NotImplementedError


class TinkEncryptor(Encryptor):
    def __init__(
        self,
        keyfile: Union[str, pathlib.Path],
        chunk_size: int = MEGABYTE,
        primitive=streaming_aead.StreamingAead,
    ) -> None:
        super().__init__()
        self._logger = logging.getLogger(self.__class__.__name__)
        self.chunk_size = chunk_size

        try:
            streaming_aead.register()
        except tink.TinkError as err:
            self._logger.error(f"Error initializing tink: {err}")
            exit(1)

        with pathlib.Path(keyfile).open(READ_B) as fp:
            try:
                f = fernet.Fernet(
                    getpass.getpass(f"Enter decryption key for {keyfile}:")
                )
                keyset_reader = tink.BinaryKeysetReader(f.decrypt(fp.read()))
                keyset_handle = cleartext_keyset_handle.read(keyset_reader)
            except TinkError as err:
                self._logger.error(f"Error reading keyset: {err}")
                exit(1)
        try:
            self._primitive = keyset_handle.primitive(primitive)
        except TinkError as err:
            self._logger.error(f"Error creating streaming primitive: {err}")
            exit(1)

    def encrypt(self, data: BinaryIO) -> BinaryIO:
        self._logger.info("Encrypting data stream")
        output_stream = tempfile.TemporaryFile()
        with data:
            if not DRY_RUN:
                with self._primitive.new_encrypting_stream(
                    output_stream
                ) as crypt_stream:
                    with tqdm(
                        total=data.tell(),
                        unit=BYTE,
                        unit_divisor=KILOBYTE,
                        unit_scale=True,
                    ) as progress:
                        data.seek(0)
                        while chunk := data.read(self.chunk_size):
                            crypt_stream.write(chunk)
                            progress.update(len(chunk))
        return output_stream

    def decrypt(self, data: BinaryIO) -> BinaryIO:
        self._logger.info("Decrypting data stream")
        output_stream = tempfile.TemporaryFile()
        with data:
            with self._primitive.new_decrypting_stream(output_stream) as crypt_stream:
                with tqdm(
                    total=data.tell(),
                    unit=BYTE,
                    unit_divisor=KILOBYTE,
                    unit_scale=True,
                ) as progress:
                    data.seek(0)
                    while chunk := data.read(self.chunk_size):
                        crypt_stream.write(chunk)
                        progress.update(len(chunk))
        return output_stream
