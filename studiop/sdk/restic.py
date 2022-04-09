import json
import os
import pathlib
import subprocess
from typing import Union

from studiop import logging


class ResticRepo:
    def __init__(self, path: str, password: str) -> None:
        self.path = path
        self._password = password

    @property
    def password(self) -> str:
        return self._password


class Restic:
    def __init__(self, bin_path: str = "restic", output: str = "json") -> None:
        self.bin = bin_path
        self.cmd = None
        self.flags = []
        self.output = f"--{output}"
        self._logger = logging.getLogger(self.__class__.__name__)

    def _reset(self):
        self.cmd = None
        self.flags = []

    def _gather_args(self) -> list[str]:
        args = [self.bin]
        args.append(self.cmd)
        args += self.flags
        args.append(self.output)
        return args

    def _run(self) -> subprocess.CompletedProcess:
        try:
            return subprocess.run(
                self._gather_args(), check=True, capture_output=True, encoding="utf-8"
            )
        except subprocess.CalledProcessError as err:
            self._logger.error(err)
            self._logger.debug(f"Return code: {err.returncode}")
            self._logger.debug(f"Subprocess args: {err.cmd}")
        finally:
            self._reset()

    def backup(
        self,
        src: Union[str, pathlib.Path],
        dest: ResticRepo,
        exclude: list[str] = None,
    ) -> dict:
        src = pathlib.Path(src)
        if not src.exists():
            raise FileNotFoundError(f"Source path {src} does not exist")

        self.cmd = "backup"
        os.putenv("RESTIC_REPOSITORY", dest.path)
        os.putenv("RESTIC_PASSWORD", dest.password)
        if exclude:
            self.flags += sum([["--exclude", item] for item in exclude], [])
        self.flags.append(str(src))
        raw_result = self._run()
        lines = [line.strip() for line in raw_result.stdout.split("\n")]

        try:
            return json.loads(lines[-1])
        except json.decoder.JSONDecodeError as err:
            self._logger.error(err)

    def copy(
        self,
        src: ResticRepo,
        dest: ResticRepo,
        snapshots: list[str] = None,
    ) -> str:
        self.cmd = "copy"
        os.putenv("RESTIC_REPOSITORY", src.path)
        os.putenv("RESTIC_PASSWORD", src.password)
        os.putenv("RESTIC_REPOSITORY2", dest.path)
        os.putenv("RESTIC_PASSWORD2", dest.password)
        if snapshots:
            self.flags += snapshots
        return self._run().stdout

    def forget(
        self,
        repo: ResticRepo,
        snapshots: list[str] = None,
        policies: dict[str, int] = None,
        prune: bool = True,
    ):
        self.cmd = "forget"
        os.putenv("RESTIC_REPOSITORY", repo.path)
        os.putenv("RESTIC_PASSWORD", repo.password)
        if policies:
            self.flags += sum(
                [[f"--{policy}", count] for policy, count in policies.items()], []
            )
        if snapshots:
            self.flags += snapshots
        if prune:
            self.flags.append("--prune")
        return self._run().stdout


# r = restic.Restic()
# r.backup(src, dest, exclude=[])
# r.copy(src, dest, tags=[])
