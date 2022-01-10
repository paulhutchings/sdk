import abc
from typing import BinaryIO

import boto3
from boto3.exceptions import S3UploadFailedError
from botocore.exceptions import ClientError
from studiop import DRY_RUN, logging
from studiop.constants import BYTE, KILOBYTE
from tqdm import tqdm


class Uploader(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def upload(self, bucket: str, filename: str, fileobj: BinaryIO, dest: str):
        raise NotImplementedError


class S3Uploader(Uploader):
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
        filename: str,
        data: BinaryIO,
        dest: str = None,
    ):
        dest_key = f"{dest}/{filename}" if dest else filename
        self._logger.info(f"Uploading {filename} to s3://{self.bucket.name}/{dest_key}")
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
                            dest_key,
                            ExtraArgs={"StorageClass": self.storage_class},
                            Callback=progress.update,
                        )
                    self._logger.info(f"Successfully uploaded {filename}")
                except (S3UploadFailedError, ClientError) as err:
                    self._logger.error(err)
