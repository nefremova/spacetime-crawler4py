import re


class Config(object):
    def __init__(self, config):
        self.user_agent = config["IDENTIFICATION"]["USERAGENT"].strip()
        print (self.user_agent)
        assert self.user_agent != "DEFAULT AGENT", "Set useragent in config.ini"
        assert re.match(r"^[a-zA-Z0-9_ ,]+$", self.user_agent), "User agent should not have any special characters outside '_', ',' and 'space'"
        self.threads_count = int(config["LOCAL PROPERTIES"]["THREADCOUNT"])
        self.save_file = config["LOCAL PROPERTIES"]["SAVE"]

        self.host = config["CONNECTION"]["HOST"]
        self.port = int(config["CONNECTION"]["PORT"])

        self.seed_urls = config["CRAWLER"]["SEEDURL"].split(",")
        self.time_delay = float(config["CRAWLER"]["POLITENESS"])

        self.cache_server = None
        self.db_name = config["SQLITE"]["DBNAME"].strip()
        self.cache_capacity = int(config["LOCAL PROPERTIES"]["CACHE_CAPACITY"])
        self.domain_mapping = {".ics.uci.edu": 1,
                                ".cs.uci.ed": 2,
                                ".informatics.uci.edu": 3,
                                ".stat.uci.edu": 4,
                                "today.uci.edu/department/information_computer_sciences": 5
                                }