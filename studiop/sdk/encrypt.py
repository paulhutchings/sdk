import abc
import getpass
import io
import os
import pathlib
import tempfile
from typing import BinaryIO, Union

import tink
from cryptography import fernet
from studiop import DRY_RUN, logging
from studiop.constants import BYTE, KILOBYTE, MEGABYTE, READ_B
from studiop.sdk import utils
from tink import TinkError, cleartext_keyset_handle, streaming_aead
from tqdm import tqdm


class Cryptor(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def encrypt(
        self, data: BinaryIO, associated_data: Union[str, bytes] = b""
    ) -> BinaryIO:
        raise NotImplementedError

    @abc.abstractmethod
    def decrypt(
        self, data: BinaryIO, associated_data: Union[str, bytes] = b""
    ) -> BinaryIO:
        raise NotImplementedError


class TinkCryptor(Cryptor):
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
                if key := os.getenv("FERNET_KEY"):
                    f = fernet.Fernet(key)
                else:
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

    def encrypt(
        self, data: BinaryIO, associated_data: Union[str, bytes] = b""
    ) -> BinaryIO:
        self._logger.info("Encrypting data stream")
        if isinstance(associated_data, str):
            associated_data = associated_data.encode()
        with data:
            if not DRY_RUN:
                with tempfile.NamedTemporaryFile(
                    delete=False
                ) as crypt_file, self._primitive.new_encrypting_stream(
                    crypt_file, associated_data
                ) as crypt_stream:
                    with tqdm(
                        total=data.tell(),
                        unit=BYTE,
                        unit_divisor=KILOBYTE,
                        unit_scale=True,
                    ) as progress:
                        data.seek(0)
                        while chunk := data.read(self.chunk_size):
                            progress.update(crypt_stream.write(chunk))
                crypt_file = pathlib.Path(crypt_file.name)
                output_stream = crypt_file.open(READ_B)
                crypt_file.unlink()
        return output_stream

    def decrypt(
        self, data: BinaryIO, associated_data: Union[str, bytes] = b""
    ) -> BinaryIO:
        self._logger.info("Decrypting data stream")
        if isinstance(associated_data, str):
            associated_data = associated_data.encode()
        output_stream = tempfile.TemporaryFile()
        with data:
            with tqdm(
                total=data.tell(),
                unit=BYTE,
                unit_divisor=KILOBYTE,
                unit_scale=True,
            ) as progress:
                data.seek(0)
                with self._primitive.new_decrypting_stream(
                    data, associated_data
                ) as crypt_stream:
                    while chunk := crypt_stream.read(self.chunk_size):
                        progress.update(output_stream.write(chunk))
        return output_stream
