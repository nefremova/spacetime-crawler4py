import os
import logging
from hashlib import sha256
from urllib.parse import urlparse
import re

def get_logger(name, filename=None):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    if not os.path.exists("Logs"):
        os.makedirs("Logs")
    fh = logging.FileHandler(f"Logs/{filename if filename else name}.log")
    fh.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter(
       "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    # add the handlers to the logger
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


def get_urlhash(url):
    parsed = urlparse(url)
    # everything other than scheme.
    return sha256(
        f"{parsed.netloc}/{parsed.path}/{parsed.params}/"
        f"{parsed.query}/{parsed.fragment}".encode("utf-8")).hexdigest()

def normalize(url):
    if url.endswith("/"):
        return url.rstrip("/")
    return url

def split_url(url):
    mapping = {
        ".ics.uci.edu": 1,
        ".cs.uci.edu": 2,
        ".informatics.uci.edu": 3,
        ".stat.uci.edu": 4,
        "today.uci.edu/department/information_computer_sciences": 5
    }
    split_domain = re.split(
                    r"(.*//)(www\.?)?(.*)(today\.uci\.edu\/department\/information_computer_sciences"
                    + r"|\.ics\.uci\.edu|\.cs\.uci\.edu"
                    + r"|\.informatics\.uci\.edu|\.stat\.uci\.edu)(.*)", url.lower(), maxsplit=5)
    return (mapping[split_domain[4]], split_domain[3], split_domain[5])
