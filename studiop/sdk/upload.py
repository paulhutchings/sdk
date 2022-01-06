import abc
from typing import BinaryIO

from boto3.exceptions import S3UploadFailedError
from botocore.exceptions import ClientError
from studiop import logging


class Uploader(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def upload(self, filename: str, fileobj: BinaryIO, dest: str):
        raise NotImplementedError


class S3Uploader(Uploader):
    def __init__(self, bucket, storage_class: str = "DEEP_ARCHIVE") -> None:
        super().__init__()
        self.bucket = bucket
        self.storage_class = storage_class
        self._logger = logging.getLogger(self.__class__.__name__)

    def upload(self, filename: str, fileobj: BinaryIO, dest: str = None):
        dest_key = f"{dest}/{filename}" if dest else filename
        self._logger.info(f"Uploading {filename} to s3://{self.bucket.name}/{dest_key}")
        try:
            self.bucket.upload_fileobj(
                fileobj, dest_key, ExtraArgs={"StorageClass": self.storage_class}
            )
            self._logger.info(f"Successfully uploaded {filename}")
        except (S3UploadFailedError, ClientError) as err:
            self._logger.error(err)
