import os
import shelve

from threading import Thread, RLock
from queue import Queue, Empty

from utils import get_logger, get_urlhash, normalize, split_url
from scraper import is_valid
from urllib.parse import urlparse
import re
from database import Database

class Frontier(object):
    def __init__(self, config, restart):
        self.logger = get_logger("FRONTIER")
        self.config = config
        self.to_be_downloaded = list()
        self.visited_cache = set()
        self.fingerprint_cache = set()
        
        if not os.path.exists(self.config.save_file) and not restart:
            # Save file does not exist, but request to load save.
            self.logger.info(
                f"Did not find save file {self.config.save_file}, "
                f"starting from seed.")
        elif os.path.exists(self.config.save_file) and restart:
            # Save file does exists, but request to start from seed.
            self.logger.info(
                f"Found save file {self.config.save_file}, deleting it.")
            os.remove(self.config.save_file)
        # Load existing save file, or create one if it does not exist.
        self.save = shelve.open(self.config.save_file)
        if restart:
            db = Database(self.config.db_name)
            db.connect()
            db.clear_db()
            db.close_connection()
            for url in self.config.seed_urls:
                self.add_url(url)
        else:
            # Set the frontier state with contents of save file.
            self._parse_save_file()
            if not self.save:
                for url in self.config.seed_urls:
                    self.add_url(url)

    def _parse_save_file(self):
        ''' This function can be overridden for alternate saving techniques. '''
        total_count = len(self.save)
        tbd_count = 0
        for url, completed in self.save.values():
            if not completed and is_valid(url):
                self.to_be_downloaded.append(url)
                tbd_count += 1
        self.logger.info(
            f"Found {tbd_count} urls to be downloaded from {total_count} "
            f"total urls discovered.")

    def get_tbd_url(self):
        try:
            return self.to_be_downloaded.pop()
        except IndexError:
            return None

    def add_url(self, url):
        url = normalize(url)
        urlhash = get_urlhash(url)
        if urlhash not in self.save:
            self.save[urlhash] = (url, False)
            self.save.sync()
            self.to_be_downloaded.append(url)
    
    def insert_visited_cache(self, url):
         self.visited_cache.add(url)
         if len(self.visited_cache) >= self.config.cache_capacity:
             to_insert = []
             for link in self.visited_cache:
                to_insert.append(split_url(link))
             print("Inserting into DB:", to_insert)
             try:
                db = Database(self.config.db_name)
                db.connect()
                db.insert_urls(to_insert)
                db.close_connection()
             except Exception as e:
                 print("DB ERROR:", e)
             self.visited_cache.clear()
    
    def insert_fingerprint_cache(self, fingerprint):
         self.fingerprint_cache.add(fingerprint)
         if len(self.fingerprint_cache) >= self.config.cache_capacity:
             print("Inserting into DB:", self.fingerprint_cache)
             try:
                db = Database(self.config.db_name)
                db.connect()
                db.insert_fingerprints(list(self.fingerprint_cache))
                db.close_connection()
             except Exception as e:
                 print("DB ERROR:", e)
             self.fingerprint_cache.clear()
         
    def mark_url_complete(self, url):
        urlhash = get_urlhash(url)
        if urlhash not in self.save:
            # This should not happen.
            self.logger.error(
                f"Completed url {url}, but have not seen it before.")

        self.save[urlhash] = (url, True)
        self.save.sync()
