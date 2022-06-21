# coding=utf-8
# @Time: 2022/6/20 12:06
# @Author: forevermessi@foxmail.com
import toml
from datetime import datetime


class Config(object):
    def __init__(self, config_file, log_file=None):
        # toml config file
        self.__config_file = config_file
        # basic config
        self.host, self.port, self.user, self.password, self.db, self.table = None, None, None, None, None, None
        self.max_workers, self.batch_size = None, None
        # flashback config
        self.until_time, self.override = None, None
        # dml config
        self.sql, self.execute, self.savepoint = None, None, None
        # by id
        self.start_rowid, self.end_rowid = None, None
        # by time
        self.split_column, self.split_column_precision, self.split_interval = None, None, None
        self.start_time, self.end_time = None, None
        # log file
        self.log_file = log_file

    def parse(self):
        with open(self.__config_file, encoding='utf8') as f:
            config = toml.load(f)
        # basic config
        self.host = config["basic"]["host"]
        self.port = config["basic"]["port"]
        self.user = config["basic"]["user"]
        self.password = config["basic"]["password"]
        self.db = config["basic"]["db"]
        self.table = config["basic"]["table"]
        self.max_workers = config["basic"].get("max_workers", 50)
        self.batch_size = config["basic"].get("batch_size", 1000)
        # flashback config
        self.until_time = config["flashback"]["until_time"]
        self.override = config["flashback"].get("override", False)
        # dml config
        self.sql = config["dml"]["sql"]
        self.savepoint = config["dml"].get("savepoint", None)
        if not self.savepoint:
            self.savepoint = f"{self.host}.{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')}.savepoint"
        self.execute = config["dml"].get("execute", False)
        # by id
        self.start_rowid = config["dml"]["by_id"].get("start_rowid", None)
        self.end_rowid = config["dml"]["by_id"].get("end_rowid", None)
        # by time
        self.start_time = config["dml"]["by_time"].get("start_time", None)
        self.end_time = config["dml"]["by_time"].get("end_time", None)
        self.split_column = config["dml"]["by_time"]["split_column"]
        self.split_column_precision = config["dml"]["by_time"].get("split_column_precision", 0)
        self.split_interval = config["dml"]["by_time"].get("split_interval", 3600)
        # log_file
        if not self.log_file:
            self.log_file = f"{self.host}.log.{datetime.now().strftime('%Y-%m-%dT%H:%M:%S')}"
