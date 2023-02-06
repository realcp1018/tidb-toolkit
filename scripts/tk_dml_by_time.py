# coding=utf-8
# @Time: 2021/8/11 11:10
# @Author: forevermessi@foxmail.com
"""
Usage:
    Following sql types supported：
        1.delete from <table> where <...>
        2.update <table> set <...> where <...>
        3.insert into <table_target> select <...> from <table> where <...>
    1.Make sure there's index on the split column.
    2.Split column type should be int/bingint/date/datetime, if numerical, split_column_precision should also be specified.
    3.Make sure your sql has a where condition(even where 1=1)
task/batch split points：
    SQL will be splitted into multiple tasks by split_column & split_interval(new sqls with between statement on split column)
    Every task will run batches serially, default batch size is 1000(which means task sql will be suffixed by `limit 1000` and run multiple times until affected rows=0)
    There will be <max_workers> taskes run simultaneously.
    Run `grep Finished <log-name> | tail` to find out how many tasks finished.
"""
import os
import signal
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from queue import Queue, Empty
from threading import Thread
from time import sleep
from traceback import format_exc

import pymysql
import sqlparse
import sqlparse.tokens as T

from utils.logger import FileLogger
from utils.config import Config

# Const
SUPPORTED_SQL_TYPES = ["DELETE", "UPDATE", "INSERT"]


def argParse():
    parser = argparse.ArgumentParser(description="TiDB Massive DML Tool(by time).")
    parser.add_argument("-f", dest="config", type=str, required=True, help="config file")
    parser.add_argument("-l", dest="log", type=str, help="Log File Name, Default <host>.log.<now>")
    args = parser.parse_args()
    return args


class MySQLConnectionPool(object):
    def __init__(self, host=None, port=None, user=None, password=None, db=None, pool_size=5):
        self.host = host
        self.port: int = port
        self.user = user
        self.password = password
        self.db = db
        self.pool_size = pool_size
        self.pool: Queue = Queue(maxsize=self.pool_size)

    def init(self):
        log.info("Initializing MySQL Connection Pool...")
        for i in range(self.pool_size):
            try:
                conn = pymysql.connect(host=self.host, port=self.port, user=self.user, password=self.password,
                                       database=self.db, charset="utf8mb4")
                conn.autocommit(True)
                self.pool.put(conn)
            except Exception as e:
                log.fatal("Create mysql connections failed, please check database connectivity! Exit!\n%s", e)
                raise e
        log.info("Initializing MySQL Connection Pool Finished...")

    def close(self):
        log.info("Closing MySQL Connection Pool...")
        for i in range(self.pool_size):
            try:
                conn: pymysql.Connection = self.pool.get(timeout=5)
                conn.close()
            except Empty:
                log.info("Connection Pool is empty, exit...")
                return
            except Exception as e:
                log.info(f"Connection Pool close with Exception: {e}")
        log.info("Closing MySQL Connection Pool Finished...")

    def get(self):
        conn = self.pool.get()
        try:
            conn.ping()
        except Exception as e:
            logger.error(e)
        return conn

    def put(self, conn: pymysql.Connection):
        self.pool.put(conn)

    def start_monitor(self):
        # start a monitor to report the pool size
        def report_size():
            while True:
                log.info(f"ConnectionPool Monitor: Size {self.pool.qsize()}")
                sleep(5)

        thd = Thread(target=report_size, daemon=True)
        thd.start()


# Table which tasks division based on
class Table(object):
    def __init__(self, table_name=None, db=None, split_column=None, conn=None):
        self.name = table_name
        self.db = db
        self.conn: pymysql.Connection = conn
        self.split_column = split_column
        self.split_column_has_index: bool = False
        self.split_column_datatype = None
        self.split_column_min = None
        self.split_column_max = None

    def load(self):
        log.info(f"Loading Table Info of {self.db}.{self.name} ...")
        with self.conn.cursor() as c:
            sql = f"select DATA_TYPE from information_schema.COLUMNS where table_schema='{self.db}' " \
                  f"and table_name='{self.name}' and column_name='{self.split_column}';"
            c.execute(sql)
            self.split_column_datatype = c.fetchone()[0]

            sql = f"select count(1) from information_schema.TIDB_INDEXES where TABLE_SCHEMA='{self.db}' " \
                  f"and TABLE_NAME='{self.name}' and COLUMN_NAME='{self.split_column}' and SEQ_IN_INDEX=1"
            c.execute(sql)
            r = c.fetchone()[0]
            self.split_column_has_index = True if r >= 1 else False
            if not self.split_column_has_index:
                log.error(f"Split column {self.split_column} has no index, exit...")
                raise Exception(f"Split column {self.split_column} has no index, exit...")

            sql = f"select min({self.split_column}),max({self.split_column}) from {self.db}.{self.name};"
            c.execute(sql)
            self.split_column_min, self.split_column_max = c.fetchone()
        log.info(f"Load Table Info of {self.db}.{self.name} Done.")


class SQLOperator(object):
    def __init__(self, pool: MySQLConnectionPool = None, sql=None, table: Table = None, split_interval=None,
                 split_column_precision=None,
                 start_time=None, end_time=None, batch_size=None, max_workers=None, execute=False):
        self.table: Table = table
        self.sql = sql.strip(";")
        self.concat_table_name = None
        self.split_interval = int(split_interval) if split_interval else 86400
        self.split_column_precision = int(split_column_precision) if split_column_precision else 0
        self.start_time = start_time
        self.end_time = end_time
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.execute = execute
        self.connction_pool: MySQLConnectionPool = pool

    def validate(self):
        log.info("Validating SQL Start...")
        """
        1.use sqlparse.format to format sql
        2.only SUPPORTED_SQL_TYPES are supported
        3.exit when no where condition
        4.check split_column data type,should be int/bigint/date/datetime
            。if int/bigint, then start_time/end_time will be converted to unix timestamp
            。if date/datetime/timestamp, then do nothing
            。if others, exit with error
        5.if no given start_time/end_time，then set start_time/end_time to min(split_column)/max(split_column)
        """
        # 1
        self.sql = sqlparse.format(self.sql, reindent_aligned=True, use_space_around_operators=True,
                                   keyword_case="upper")
        log.info(f"SQL will be batched: \n{self.sql}")
        # 2
        parsed_sql = sqlparse.parse(self.sql)[0]
        sql_type = parsed_sql.get_type()
        if sql_type not in SUPPORTED_SQL_TYPES:
            raise Exception(f"Unsupported SQL type: {sql_type}!")
        # 3
        sql_tokens = parsed_sql.tokens
        for token in sql_tokens:
            if isinstance(token, sqlparse.sql.Identifier) and (token.value.split(" ")[0].lower() == self.table.name.lower()):
                self.concat_table_name = token.get_alias() if token.get_alias() else self.table.name
                break
        where_token = list(filter(lambda token: isinstance(token, sqlparse.sql.Where), sql_tokens))
        if len(where_token) == 0:
            raise Exception("No where condition in SQL, exit...")
        # 4 & 5
        if self.table.split_column_datatype in ('int', 'bigint'):
            try:
                datetime.fromtimestamp(self.table.split_column_max/(10**self.split_column_precision))
            except OSError as e:  # for windows
                if e.args[0] == 22:
                    raise Exception("Split column timestamp precision is ms or μs, Please specify a new "
                                    "split_column_precision(3 or 6, default 0)!")
                else:
                    raise e
            except ValueError as e:  # for linux
                raise Exception("Split column timestamp precision is ms or μs, Please specify a new "
                                "split_column_precision(3 or 6, default 0)!")
            self.start_time = datetime.strptime(self.start_time, "%Y-%m-%d %H:%M:%S").timestamp() * (
                        10 ** self.split_column_precision) if self.start_time else self.table.split_column_min
            self.end_time = datetime.strptime(self.end_time, "%Y-%m-%d %H:%M:%S").timestamp() * (
                        10 ** self.split_column_precision) if self.end_time else self.table.split_column_max
        elif self.table.split_column_datatype in ("date", "datetime", "timestamp"):
            self.start_time = datetime.strptime(self.start_time, "%Y-%m-%d %H:%M:%S") if self.start_time else \
                self.table.split_column_min
            self.end_time = datetime.strptime(self.end_time, "%Y-%m-%d %H:%M:%S") if self.end_time else \
                self.table.split_column_max
            self.split_interval = timedelta(seconds=self.split_interval)
        else:
            raise Exception("Unsupported split column Data type: {self.table.split_column_datatype}!")
        # Done
        log.info("Validating SQL Done...")

    def run(self):
        log.info(f"Time Range [{self.start_time},{self.end_time}]")
        task_start_time = self.start_time
        if self.execute:
            with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
                while task_start_time < self.end_time:
                    task_end_time = task_start_time + self.split_interval * (10 ** self.split_column_precision)
                    if task_end_time >= self.end_time:
                        pool.submit(self.__run_task, task_start_time, self.end_time)
                        break
                    else:
                        pool.submit(self.__run_task, task_start_time, task_end_time)
                        task_start_time = task_end_time
        else:
            with ThreadPoolExecutor(max_workers=1) as pool:
                pool.submit(self.__run_task, task_start_time, task_start_time + self.split_interval)

    def __run_task(self, start, stop):
        try:
            parsed_sql = sqlparse.parse(self.sql)[0]
            sql_tokens = list(filter(lambda token: token.ttype not in (T.Whitespace, T.Newline), parsed_sql.tokens))
            if self.table.split_column_datatype in ('int', 'bigint'):
                for i in range(len(sql_tokens)):
                    if isinstance(sql_tokens[i], sqlparse.sql.Where):
                        sql_tokens[i].value = sql_tokens[i].value.replace(
                            "WHERE",
                            f"WHERE {self.concat_table_name}.{self.table.split_column} >= {start} AND {self.concat_table_name}.{self.table.split_column} < {stop} AND ("
                        )
                        break
            else:
                for i in range(len(sql_tokens)):
                    if isinstance(sql_tokens[i], sqlparse.sql.Where):
                        sql_tokens[i].value = sql_tokens[i].value.replace(
                            "WHERE",
                            f"WHERE {self.concat_table_name}.{self.table.split_column} >= '{start}' AND {self.concat_table_name}.{self.table.split_column} < '{stop}' AND ("
                        )
                        break
            sql_token_values = list(map(lambda token: token.value, sql_tokens))
            task_sql = ' '.join(sql_token_values) + ")"
            batch_sql = task_sql + f" limit {self.batch_size};"
        except Exception as e:
            log.error(f"Task SQL Generate Failed On [{start},{stop}) :{e}, Exception:\n{format_exc()}")
            raise e
        if self.execute:
            retry = 0
            while retry < 3:
                try:
                    conn = self.connction_pool.get()
                    affected_rows = 1
                    task_start = datetime.now()
                    while affected_rows > 0:
                        batch_start_time = datetime.now()
                        with conn.cursor() as c:
                            affected_rows = c.execute(batch_sql)
                        conn.commit()
                        batch_end_time = datetime.now()
                        log.info(f"Task Batch On [{start},{stop}) OK, {affected_rows} Rows Affected"
                                 f"({batch_end_time - batch_start_time}).\nSQL: {batch_sql}")
                    if affected_rows == 0:
                        task_end = datetime.now()
                        log.info(f"Task On [{start},{stop}) Finished,({task_end - task_start}).\nSQL: {task_sql}")
                    self.connction_pool.put(conn)
                    break
                except Exception as e:
                    log.error(f"SQL Retry {retry} Failed: {batch_sql}")
                    log.error(f"Task Execute Failed On [{start},{stop}): {e}, Exception:\n{format_exc()}")
                    self.connction_pool.put(conn)
            if retry == 3:
                log.error(f"SQL Retry {retry} Times Failed, Exit Now: {batch_sql}")
                os.kill(os.getpid(), signal.SIGINT)
        else:
            log.info(f"Task On [{start},{stop}) Dry Run:\nSQL: {batch_sql}")


if __name__ == '__main__':
    args = argParse()
    config_file, log_file = args.config, args.log
    conf = Config(config_file=config_file, log_file=log_file)
    conf.parse()
    log = FileLogger(filename=conf.log_file)
    print(f"See logs in {conf.log_file} ...")

    # create connection pool
    pool = MySQLConnectionPool(host=conf.host, port=int(conf.port), user=conf.user, password=conf.password,
                               db=conf.db, pool_size=conf.max_workers * 2)
    pool.init()
    pool.start_monitor()

    # load table info
    conn = pool.get()
    table = Table(table_name=conf.table, db=conf.db, split_column=conf.split_column, conn=conn)
    table.load()
    pool.put(conn)

    # start a sql operator
    operator = SQLOperator(pool=pool, sql=conf.sql, table=table, split_interval=conf.split_interval,
                           split_column_precision=conf.split_column_precision,
                           start_time=conf.start_time, end_time=conf.end_time, batch_size=conf.batch_size,
                           max_workers=conf.max_workers, execute=conf.execute)
    operator.validate()
    operator.run()

    # close connection pool
    pool.close()
