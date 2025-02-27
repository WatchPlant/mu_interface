import sys
import logging
import pathlib
from datetime import datetime

#These are the sequences need to get colored ouput
RESET_SEQ = "\033[0m"
BOLD_SEQ = "\033[1m"

COLORS = {
    'WARNING': "\033[38;5;130m",
    'INFO': "",
    'DEBUG': "\033[38;5;2m",
    'CRITICAL': "\033[31m",
    'ERROR': "\033[31m",
}

log_DBGX = 15

class ColoredFormatter(logging.Formatter):
    def __init__(self, fmt=None, datefmt=None):
        logging.Formatter.__init__(self, fmt, datefmt)

    def format(self, record):
        skip_line = False
        if record.msg and record.msg[0] == '\n':
            skip_line = True
            record.msg = record.msg[1:]
        result = logging.Formatter.format(self, record)
        result = COLORS[record.levelname] + result + RESET_SEQ
        if skip_line:
            result = '\n' + result
        return result

def setup_logger(name, level=logging.INFO):
    logging.addLevelName(log_DBGX, "DEBUG")

    pathlib.Path('logs').mkdir(exist_ok=True)

    logFormatter = logging.Formatter("[%(asctime)s] [%(levelname)s]: %(message)s", '%d.%m.%Y. %H:%M:%S')
    colorFormatter = ColoredFormatter("[%(asctime)s] [%(threadName)s] [%(levelname)s]: %(message)s", '%d.%m.%Y. %H:%M:%S')
    rootLogger = logging.getLogger()

    fileHandler = logging.FileHandler(f"logs/{name}-{datetime.now().strftime('%d_%m_%Y-%H_%M_%S')}.log")
    fileHandler.setFormatter(logFormatter)
    rootLogger.addHandler(fileHandler)

    consoleHandler = logging.StreamHandler(sys.stdout)
    consoleHandler.setFormatter(colorFormatter)
    rootLogger.addHandler(consoleHandler)

    rootLogger.setLevel(level)


if __name__ == "__main__":
    setup_logger("test", logging.DEBUG)
    logging.debug("test")
    logging.info("test")
    logging.warning("test")
    logging.error("test")
    logging.critical("test")
    logging.log(log_DBGX, "test")