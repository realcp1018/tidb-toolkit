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
    If table was sharded(SHARD_ROW_ID_BITS or auto_random used), use tk_dml_bytime instead.
    SQL will be splited into multiple batches by _tidb_rowid&batch_size(new sqls with between statement on _tidb_rowid), there will be <max_workers> batches run simultaneously
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from queue import Queue, Empty
from threading import Thread
from time import sleep
from traceback import format_exc

import pymysql
import sqlparse
import sqlparse.tokens as T

from utils.logger import FileLogger
from utils.config import Config

# Global Constants
SUPPORTED_SQL_TYPES = ["DELETE", "UPDATE", "INSERT"]


def argParse():
    parser = argparse.ArgumentParser(description="TiDB Massive DML Tool(by id).")
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
        logger.info("Initializing MySQL Connection Pool...")
        for i in range(self.pool_size):
            try:
                conn = pymysql.connect(host=self.host, port=self.port, user=self.user, password=self.password,
                                       database=self.db, charset="utf8mb4")
                conn.autocommit(True)
                self.pool.put(conn)
            except Exception as e:
                logger.fatal("Create mysql connections failed, please check database connectivity! Exit!\n%s", e)
                raise e
        logger.info("Initializing MySQL Connection Pool Finished...")

    def close(self):
        logger.info("Closing MySQL Connection Pool...")
        for i in range(self.pool_size):
            try:
                conn: pymysql.Connection = self.pool.get(timeout=5)
                conn.close()
            except Empty:
                logger.info("Connection Pool is empty, exit...")
                return
            except Exception as e:
                logger.info(f"Connection Pool close with Exception: {e}")
        logger.info("Closing MySQL Connection Pool Finished...")

    def get(self):
        return self.pool.get()

    def put(self, conn: pymysql.Connection):
        self.pool.put(conn)

    def start_monitor(self):
        # start a monitor to report the pool size
        def report_size():
            while True:
                logger.info(f"ConnectionPool Monitor: Size {self.pool.qsize()}")
                sleep(10)

        thd = Thread(target=report_size, daemon=True)
        thd.start()


# Table on Which Batches Division Based
class Table(object):
    def __init__(self, table_name=None, db=None, conn=None):
        self.name = table_name
        self.db = db
        self.conn: pymysql.Connection = conn
        self.rowid = None
        self.rowid_min = None
        self.rowid_max = None
        self.is_rowid_sharded = False

    def load(self):
        logger.info(f"Loading Table Info of {self.db}.{self.name} ...")
        with self.conn.cursor() as c:
            sql = f"select COLUMN_NAME,DATA_TYPE from information_schema.COLUMNS where table_schema='{self.db}' " \
                  f"and table_name='{self.name}' and COLUMN_KEY='PRI';"
            c.execute(sql)
            pri_info = c.fetchall()
            if len(pri_info) == 1 and pri_info[0][1] in ('int', 'bigint'):
                self.rowid = pri_info[0][0]
            else:
                self.rowid = "_tidb_rowid"  # use _tidb_rowid when no primary or primary is not int

            sql = f"select TIDB_ROW_ID_SHARDING_INFO from information_schema.TABLES where TABLE_SCHEMA='{self.db}' " \
                  f"and TABLE_NAME='{self.name}'"
            try:
                c.execute(sql)
                rowid_shard_info = c.fetchone()[0]
                if not rowid_shard_info.startswith("NOT_SHARDED"):
                    self.is_rowid_sharded = True
            except Exception as e:
                if e.args[0] == 1054:
                    print("Warning: TiDB version <=4.0.0, Please check if Rowid was sharded before execution!")
                    pass
                    # TIDB_ROW_ID_SHARDING_INFO not supported if tidb version <= 4.0，so print a warning here.
                    # This warning told you to check table sharded info manually.
                else:
                    raise e
            sql = f"select min({self.rowid}),max({self.rowid}) from {self.db}.{self.name};"
            c.execute(sql)
            self.rowid_min, self.rowid_max = c.fetchone()
        logger.info(f"Load Table Info of {self.db}.{self.name} Done.")


class SQLOperator(object):
    def __init__(self, pool: MySQLConnectionPool = None, table: Table = None, sql=None, batch_size=None,
                 max_workers=None, start_rowid=None, end_rowid=None, execute=None):
        self.table: Table = table
        self.sql = sql.strip(";")
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.start_rowid = int(start_rowid) if start_rowid else self.table.rowid_min
        self.end_rowid = int(end_rowid) if end_rowid else self.table.rowid_max
        self.execute = execute
        self.connction_pool: MySQLConnectionPool = pool

    def validate(self):
        logger.info("Validating SQL Start...")
        """
        1.use sqlparse.format to format sql
        2.only SUPPORTED_SQL_TYPES are supported
        3.exit when no where condition
        4.exit if table was sharded(SHARD_ROW_ID_BITS or auto_random)
        """
        # 1
        self.sql = sqlparse.format(self.sql, reindent_aligned=True, use_space_around_operators=True,
                                   keyword_case="upper")
        logger.info(f"SQL will be batched: \n{self.sql}")
        # 2
        parsed_sql = sqlparse.parse(self.sql)[0]
        sql_type = parsed_sql.get_type()
        if sql_type not in SUPPORTED_SQL_TYPES:
            raise Exception(f"Unsupported SQL type: {sql_type}!")
        # 3
        sql_tokens = parsed_sql.tokens
        where_token = list(filter(lambda token: isinstance(token, sqlparse.sql.Where), sql_tokens))
        if len(where_token) == 0:
            raise Exception("No where condition in SQL(try where 1=1), exit...")
        # 4
        if self.table.is_rowid_sharded:
            raise Exception(f"Table {self.table.name} was set SHARD_ROW_ID_BITS or AUTO_RANDOM! exit...")

        logger.info(f"Rowid [{self.table.rowid}] will be used for batching.")
        logger.info("Validating SQL Done...")

    def run(self):
        thread_count = (self.end_rowid - self.start_rowid) // self.batch_size + 1
        logger.info(f"Max Thread Count: {thread_count}, Rowid Range [{self.start_rowid},{self.end_rowid}]")
        if not self.execute:
            with ThreadPoolExecutor(max_workers=1) as pool:
                pool.submit(self.__run_batch,
                            self.start_rowid,
                            self.start_rowid + self.batch_size,
                            1,
                            thread_count)
        else:
            i = 0  # release concurrent.futures every 1000 threads
            while i < thread_count:
                with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
                    for j in range(i, i + 1000):
                        pool.submit(self.__run_batch,
                                    self.start_rowid + (j * self.batch_size),
                                    self.start_rowid + ((j + 1) * self.batch_size),
                                    j + 1,
                                    thread_count)
                i += 1000

    def __run_batch(self, start: int, stop: int, batch_id, max_batch_id):
        try:
            sql_tokens = sqlparse.parse(self.sql)[0].tokens
            sql_tokens = list(filter(lambda token: token.ttype not in (T.Whitespace, T.Newline), sql_tokens))
            rowid_condition = "WHERE {0}.{1} >= {2} AND {0}.{1} < {3} AND".format(self.table.name, self.table.rowid,
                                                                                  start, stop)
            for i in range(len(sql_tokens)):
                if isinstance(sql_tokens[i], sqlparse.sql.Where):
                    sql_tokens[i].value = sql_tokens[i].value.replace("WHERE", rowid_condition)
                    break
            sql_token_values = list(map(lambda token: token.value, sql_tokens))
            batch_sql = ' '.join(sql_token_values)
        except Exception as e:
            logger.error(f"Batch {batch_id} failed with exeception {e}, exit... Exception:\n {format_exc()}")
            raise
        if self.execute:
            conn = self.connction_pool.get()
            try:
                start_time = datetime.now()
                with conn.cursor() as c:
                    affected_rows = c.execute(batch_sql)
                conn.commit()
                end_time = datetime.now()
                logger.info(f"Batch {batch_id} of {max_batch_id} OK, {affected_rows} Rows Affected ("
                            f"{end_time - start_time}).\nSQL: {batch_sql}")
            except Exception as e:
                logger.error(f"Batch {batch_id} of {max_batch_id} Failed: {e}, Exception:\n {format_exc()}")
                raise
            finally:
                if conn:
                    self.connction_pool.put(conn)
        else:
            logger.info(f"Batch {batch_id} of {max_batch_id} Dry Run:\nSQL: {batch_sql}")


if __name__ == '__main__':
    args = argParse()
    config_file, log_file = args.config, args.log
    conf = Config(config_file=config_file, log_file=log_file)
    conf.parse()
    logger = FileLogger(filename=conf.log_file)
    print(f"See logs in {conf.log_file} ...")

    # create connection pool
    pool = MySQLConnectionPool(host=conf.host, port=int(conf.port), user=conf.user, password=conf.password,
                               db=conf.db, pool_size=conf.max_workers * 2)
    pool.init()
    pool.start_monitor()

    # load table info
    conn = pool.get()
    table = Table(table_name=conf.table, db=conf.db, conn=conn)
    table.load()
    pool.put(conn)

    # start sql operator
    operator = SQLOperator(pool=pool, table=table, sql=conf.sql, batch_size=conf.batch_size, execute=conf.execute,
                           max_workers=conf.max_workers, start_rowid=conf.start_rowid, end_rowid=conf.end_rowid)
    operator.validate()
    operator.run()

    # close connection pool
    pool.close()
