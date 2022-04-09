import abc
import tempfile
from typing import BinaryIO

import boto3
from boto3.exceptions import S3UploadFailedError
from botocore.exceptions import ClientError
from studiop import DRY_RUN, logging
from studiop.constants import BYTE, KILOBYTE
from tqdm import tqdm


class Backend(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def upload(self, key: str, fileobj: BinaryIO):
        raise NotImplementedError

    @abc.abstractmethod
    def download(self, key: str) -> BinaryIO:
        raise NotImplementedError


class S3Backend(Backend):
    def __init__(
        self,
        bucket: str,
        profile: str = "default",
        storage_class: str = "DEEP_ARCHIVE",
    ) -> None:
        super().__init__()
        self.bucket = boto3.Session(profile_name=profile).resource("s3").Bucket(bucket)
        self.storage_class = storage_class
        self._logger = logging.getLogger(self.__class__.__name__)

    def upload(
        self,
        key: str,
        data: BinaryIO,
    ):
        self._logger.info(f"Uploading to s3://{self.bucket.name}/{key}")
        with data:
            if not DRY_RUN:
                try:
                    with tqdm(
                        total=data.tell(),
                        unit=BYTE,
                        unit_scale=True,
                        unit_divisor=KILOBYTE,
                    ) as progress:
                        data.seek(0)
                        self.bucket.upload_fileobj(
                            data,
                            key,
                            ExtraArgs={"StorageClass": self.storage_class},
                            Callback=progress.update,
                        )
                    self._logger.info(f"Successfully uploaded {key}")
                except (S3UploadFailedError, ClientError) as err:
                    self._logger.error(err)

    def download(self, key: str) -> BinaryIO:
        self._logger.info(f"Downloading from s3://{self.bucket.name}/{key}")
        output_stream = tempfile.TemporaryFile()
        item = self.bucket.Object(key)
        with tqdm(
            total=item.content_length,
            unit=BYTE,
            unit_scale=True,
            unit_divisor=KILOBYTE,
        ) as progress:
            self.bucket.download_fileobj(key, output_stream, Callback=progress.update)
        self._logger.info(f"Successfully downloaded {key}")
        return output_stream
