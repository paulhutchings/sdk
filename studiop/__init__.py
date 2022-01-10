import logging
import os
import pathlib
import sys
from datetime import datetime

from studiop.constants import UTF_8, WRITE

TIMESTAMP = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

LEVEL = logging.DEBUG if "--debug" in sys.argv else logging.INFO
PRIMARY_LOG_DIR = pathlib.Path("/var/log/studiop")
SECONDARY_LOG_DIR = pathlib.Path().home().joinpath(".log")
LOG_DIR = PRIMARY_LOG_DIR if os.access(PRIMARY_LOG_DIR, os.W_OK) else SECONDARY_LOG_DIR
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR.joinpath(f"{TIMESTAMP}.log")
DRY_RUN = "--dry-run" in sys.argv

HANDLERS = [
    logging.StreamHandler(sys.stdout),
    logging.FileHandler(LOG_FILE, WRITE, UTF_8),
]

logging.basicConfig(level=LEVEL, handlers=HANDLERS)
logging.debug("Logging initialized")
logging.debug(f"Writing logs to {LOG_FILE}")
