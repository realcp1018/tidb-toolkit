# coding=utf-8
# @Time: 2021/8/20 14:38
# @Author: forevermessi@foxmail.com
"""
Usage:
    Following sql types supported：
        1.delete from <table> where <...>
        2.update <table> set <...> where <...>
        3.insert into <table_target> select <...> from <table> where <...>
    _tidb_rowid will be used as the default split column.
    SQL will be splited into multiple chunks by _tidb_rowid&chunk_size
"""
import os
import argparse
import signal
from concurrent.futures import ThreadPoolExecutor, Future, as_completed
from queue import Queue, Empty
from threading import Thread
from time import sleep
from datetime import datetime, timedelta
# from pprint import pprint
from typing import Set
import pymysql
import sqlparse

from utils.logger import FileLogger
from conf.config import Config

# Global Constants
SUPPORTED_SQL_TYPES = ["DELETE", "UPDATE", "INSERT"]


def argParse():
    parser = argparse.ArgumentParser(description="TiDB Chunk Update Script.")
    parser.add_argument("-f", dest="config", type=str, required=True, help="config file")
    parser.add_argument("-l", dest="log", type=str, help="log file name, default <host>.log.<now>")
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
        log.info(f"Initializing MySQL Connection Pool(size={self.pool_size})...")
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

    def get(self) -> pymysql.Connection:
        conn: pymysql.Connection = self.pool.get()
        try:
            conn.ping(reconnect=True)
        except Exception as e:
            # reconnect=True so here only print the exception
            log.error(e)
        return conn

    def put(self, conn: pymysql.Connection):
        self.pool.put(conn)

    def start_monitor(self):
        # start a monitor to report the pool size
        def report_size():
            while True:
                log.info(f"ConnectionPool Monitor: Size {self.pool.qsize()}")
                sleep(10)

        thd = Thread(target=report_size, daemon=True)
        thd.start()


# collect table details from database
class Table(object):
    def __init__(self, name: str, db: str, conn: pymysql.Connection):
        self.name = name
        self.db = db
        self.conn = conn
        self.rowid = None
        self.rowid_min = None
        self.rowid_max = None

    def __str__(self):
        return f"table: `{self.db}`.`{self.name}`, rowid: `{self.rowid}`({self.rowid_min}, {self.rowid_max})"

    def load(self) -> None:
        log.info(f"Loading Table Info of {self.db}.{self.name} ...")
        with self.conn.cursor() as c:
            query = f"select column_name,data_type from information_schema.columns where table_schema='{self.db}' " \
                    f"and table_name='{self.name}' and column_key='PRI';"
            c.execute(query)
            pk_info = c.fetchall()
            # set self.rowid
            if len(pk_info) == 1 and pk_info[0][1] in ('int', 'bigint'):
                self.rowid = pk_info[0][0]
            else:
                self.rowid = "_tidb_rowid"
            query = f"select min({self.rowid}),max({self.rowid}) from {self.db}.{self.name};"
            c.execute(query)
            # set self.rowid extremum
            self.rowid_min, self.rowid_max = c.fetchone()
        log.info(f"Load Table Info of {self.db}.{self.name} Done.")


# read & write savepoint
class SavePoint(object):
    def __init__(self, file_name=None):
        self.file_name = file_name

    def get(self) -> int:
        try:
            with open(self.file_name) as f:
                v = f.read()
                return int(v) if v else 0
        except FileNotFoundError:
            return 0

    def set(self, savepoint: int):
        with open(self.file_name, "w") as f:
            f.write(str(savepoint))


# format and validate the input sql
class Sql(object):
    def __init__(self, text: str, table: Table):
        self.text: str = text
        self.table: Table = table
        self.table_alias: str = None

    def validate(self):
        log.info("Validating SQL Start...")
        """
        1.only SUPPORTED_SQL_TYPES are supported
        2.set table alias(to table.name if no alias) && add tableRangeScan hint /*+ use_index(table_name) */
        3.exit when no where condition
        """
        self.text = sqlparse.format(self.text, use_space_around_operators=True, keyword_case="upper")
        # 1
        parsed_sql = sqlparse.parse(self.text)[0]
        sql_type = parsed_sql.get_type()
        if sql_type not in SUPPORTED_SQL_TYPES:
            raise Exception(f"Unsupported SQL type: {sql_type}!")
        # 2
        sql_tokens: sqlparse.sql.TokenList = parsed_sql.tokens
        for token in sql_tokens:
            if isinstance(token, sqlparse.sql.Identifier) and token.get_real_name() == self.table.name.lower():
                self.table_alias = token.get_alias() if token.get_alias() else self.table.name
                break
        for token in sql_tokens:
            if token.ttype == sqlparse.tokens.Keyword and token.value == "FROM":
                self.text = self.text.replace("FROM", f"/*+ USE_INDEX({self.table_alias}) */ FROM")
                break
        # 3
        where_token = list(filter(lambda token: isinstance(token, sqlparse.sql.Where), sql_tokens))
        if len(where_token) == 0:
            raise Exception("No where condition in SQL(try with `WHERE 1=1`)")


# chunks that can be executed
class Chunk(object):
    def __init__(self, seq: int, split_time: timedelta, start: int, stop: int, sql_text: str):
        self.seq = seq
        self.split_time = split_time
        self.start = start
        self.end = stop
        self.sql_text = sql_text

    def __str__(self):
        return f"[{self.seq}]: {self.sql_text}"

    def execute(self, pool: MySQLConnectionPool, retry_limit: int = 3):
        retry_time = 0
        while retry_time < retry_limit:
            try:
                conn = pool.get()
                start_time = datetime.now()
                with conn.cursor() as c:
                    rows = c.execute(self.sql_text)
                conn.commit()
                end_time = datetime.now()
                log.info(f"chunk {self.seq} Done [split_time={self.split_time}] [duration={end_time-start_time}] "
                         f"[rows={rows}] [sql={self.sql_text}]")
                break
            except Exception as e:
                log.error(f"chunk {self.seq} Retry Time {retry_time} Failed [error={e}]")
                retry_time += 1
                continue
            finally:
                if 'conn' in locals():
                    pool.put(conn)
        if retry_time == retry_limit:
            log.error(f"chunk {self.seq} Retry All {retry_limit} Times Failed, Exit Now [sql={self.sql_text}]")
            os._exit(1)  # exit main process


# split Sql into multiple Chunks
class ChunkSpliter(object):
    def __init__(self, sql: Sql, table: Table, pool: MySQLConnectionPool, chunk_size: int = 1000):
        self.sql = sql
        self.table = table
        self.pool = pool
        self.chunk_size = chunk_size

    def split(self):
        current_seq = 1
        current_rowid = self.table.rowid_min
        conn = self.pool.get()
        while current_rowid < self.table.rowid_max:
            start_time = datetime.now()
            query = f"select max({self.table.rowid}) from (select {self.table.rowid} from {self.table.name}  where " \
                    f"{self.table.rowid} > {current_rowid} order by {self.table.rowid} limit 0,{self.chunk_size}) t"
            with conn.cursor() as c:
                c.execute(query)
                result = c.fetchone()
                if len(result) == 0:
                    return
            end_time = datetime.now()
            chunk_left = current_rowid
            chunk_right = result[0]

            if chunk_right < self.table.rowid_max:
                chunk_sql = f"{self.sql.text} and (`{self.sql.table_alias}`.`{self.table.rowid}` >= {chunk_left} " \
                            f"and `{self.sql.table_alias}`.`{self.table.rowid}` < {chunk_right})"
                yield Chunk(seq=current_seq, split_time=end_time-start_time, start=chunk_left, stop=chunk_right, sql_text=chunk_sql)
            else:
                chunk_sql = f"{self.sql.text} and (`{self.sql.table_alias}`.`{self.table.rowid}` >= {chunk_left} " \
                            f"and `{self.sql.table_alias}`.`{self.table.rowid}` <= {self.table.rowid_max})"
                self.pool.put(conn)
                yield Chunk(seq=current_seq, split_time=end_time-start_time, start=chunk_left, stop=self.table.rowid_max, sql_text=chunk_sql)

            current_seq += 1
            current_rowid = chunk_right


class Executor(object):
    def __init__(self, pool: MySQLConnectionPool = None, table: Table = None, sql_text: str = None,
                 chunk_size: int = 5000, max_workers: int = 50, savepoint_file: str = None, execute: bool = False):
        self.table: Table = table
        self.sql_text = sql_text
        self.chunk_size = chunk_size
        self.max_workers = max_workers
        self.savepoint = SavePoint(file_name=savepoint_file) if savepoint_file else None
        self.execute = execute
        self.pool: MySQLConnectionPool = pool

    def run(self):
        sql = Sql(text=self.sql_text, table=self.table)
        sql.validate()
        chunk_spliter = ChunkSpliter(sql=sql, table=self.table, pool=self.pool, chunk_size=self.chunk_size)
        if not self.execute:
            for chunk in chunk_spliter.split():
                log.info("will exit on the first chunk [execute=false]")
                log.info(chunk)
                break
        else:
            futures: Set[Future] = set()
            """
            1. when len(futures) >= max_workers * 10, wait all futures in set completed and remove them from set
            2. then you can write savepoint and prevent memory exhausted when millions of futures needed
            3. another way is: remove one completed future and create a new one when len(futures) >= max_workers * 10,
            this way will be more efficient than the above, but it'll be difficult to write a savepoint
            """
            with ThreadPoolExecutor(max_workers=self.max_workers) as thread_pool:
                for chunk in chunk_spliter.split():
                    if len(futures) >= self.max_workers * 10:
                        for f in as_completed(futures):
                            futures.remove(f)
                        if self.savepoint:
                            log.info(f"write savepoint {chunk.start}, complete percent: {round(chunk.start*100/self.table.rowid_max, 2)}%")
                            self.savepoint.set(chunk.start)
                    future = thread_pool.submit(chunk.execute, self.pool)
                    futures.add(future)
            # the `with` statement will wait for all futures done executing then shutdown
            if self.savepoint:
                log.info(f"write savepoint {self.table.rowid_max}, complete percent: 100%")
                self.savepoint.set(self.table.rowid_max)


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
    table = Table(name=conf.table, db=conf.db, conn=conn)
    table.load()
    current_savepoint = SavePoint(file_name=conf.savepoint).get()
    if current_savepoint > table.rowid_min:
        log.info(f"savepoint {current_savepoint} larger than min(rowid) {table.rowid_min}, use savepoint instead.")
        table.rowid_min = current_savepoint
    log.info(table)
    pool.put(conn)

    # run
    executor = Executor(pool=pool, table=table, sql_text=conf.sql.strip().strip(";"), savepoint_file=conf.savepoint, execute=conf.execute)
    start_time = datetime.now()
    executor.run()
    end_time = datetime.now()
    log.info(f"total elapsed time: {end_time - start_time}")

    # close connection pool
    pool.close()