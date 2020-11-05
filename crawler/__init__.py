from utils import get_logger
from crawler.frontier import Frontier
from crawler.worker import Worker
from database import Database

class Crawler(object):
    def __init__(self, config, restart, frontier_factory=Frontier, worker_factory=Worker):
        self.config = config
        self.logger = get_logger("CRAWLER")
        self.frontier = frontier_factory(config, restart)
        self.workers = list()
        self.worker_factory = worker_factory

        db = Database(config.db_name)
        db.connect()
        db.create_db()
        print("CREATED DATABASE")
        db.close_connection()

    def start_async(self):
        self.workers = [
            self.worker_factory(worker_id, self.config, self.frontier)
            for worker_id in range(self.config.threads_count)]
        for worker in self.workers:
            worker.start()

    def start(self):
        self.start_async()
        self.join()
        self.generate_report()

    def join(self):
        for worker in self.workers:
            worker.join()

    def generate_report(self):
        print("GENERATING REPORT....")
        db = Database(self.config.db_name)
        db.connect()
        try:
            with open("report.txt", "w") as report:
                report.write("==========REPORT==========\n")
                report.write("UNIQUE PAGES: " + str(db.get_num_visited()[0]) + "\n")
                report.write("==========================\n")
                report.write("MAX WORD COUNT: " + str(self.frontier.max_webpage_len) + "\n")
                report.write("==========================\n")
                report.write("50 MOST COMMON WORDS:\n")
                i = 1
                for word, freq in db.get_top_50_words():
                    report.write(str(i) + ". " + word + ", " + str(freq) + "\n")
                    i += 1
                report.write("==========================\n")
                report.write("ics.uci.edu SUBDOMAINS:\n")
                for subdomain, count in db.get_ics_subdomains():
                    report.write(subdomain + ", " + str(count) + "\n")
        except Exception as e:
            print("REPORT ERROR: ", e)

        db.close_connection()
        print("REPORT FINISHED")
    