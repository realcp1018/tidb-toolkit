# coding=utf-8
# @Time: 2021/8/20 14:38
# @Author: forevermessi@foxmail.com
"""
Usage:
    Following sql types supported：
        1.delete from <table> where <...>
        2.update <table> set <...> where <...>
        3.insert into <table_target> select <...> from <table> where <...>
    numeric pk or _tidb_rowid（both of them is called rowid here） will be used as the default split column.
    If table was sharded(SHARD_ROW_ID_BITS or auto_random used), use tk_dml_bytime instead.
    SQL will be split into multiple batches by rowid&batch_size(new sqls with between statement on rowid), there will be <max_workers> batches run simultaneously
"""
import os
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
from conf.config import Config

# Global Constants
SUPPORTED_SQL_TYPES = ["DELETE", "UPDATE", "INSERT"]
UNSUPPORTED_KEYWORDS = ["LIMIT"]


def argParse():
    parser = argparse.ArgumentParser(description="TiDB Massive DML Tool(by id).")
    parser.add_argument("-f", dest="config", type=str, required=True, help="config file")
    parser.add_argument("-l", "--log", dest="log", type=str, required=True, help="log file name")
    parser.add_argument("-e", "--execute", dest="execute", action="store_true",
                        help="execute or just print the first batch by default")
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
            # Validate the connection without triggering an automatic reconnect
            conn.ping(reconnect=False)
        except Exception as e:
            log.error(e)
            try:
                conn.close()
            except Exception as close_err:
                log.error(f"Error closing dead MySQL connection: {close_err}")
            # Create a new connection to replace the dead one
            conn = pymysql.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.db,
                charset="utf8mb4"
            )
            conn.autocommit(True)
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


# Table on Which Batches Division Based
class Table(object):
    def __init__(self, table_name=None, db=None, conn=None):
        self.name = table_name
        self.db = db
        self.conn: pymysql.Connection = conn
        self.rowid = None
        self.rowid_min = None
        self.rowid_max = None

    def is_from_tidb(self) -> bool:
        """
        check if table is from tidb
        """
        with self.conn.cursor() as cursor:
            cursor.execute("select version();")
            version = cursor.fetchone()[0]
            if "TiDB" in version:
                return True
        return False

    def load(self):
        log.info(f"Loading Table Info of {self.db}.{self.name} ...")
        is_tidb_table = self.is_from_tidb()
        with self.conn.cursor() as c:
            sql = f"select COLUMN_NAME,DATA_TYPE from information_schema.COLUMNS where table_schema='{self.db}' " \
                  f"and table_name='{self.name}' and COLUMN_KEY='PRI';"
            c.execute(sql)
            pri_info = c.fetchall()
            if len(pri_info) == 1 and pri_info[0][1] in ('int', 'bigint'):
                self.rowid = pri_info[0][0]
            else:
                if not is_tidb_table:
                    raise Exception("No numeric primary key in this non-TiDB table!")
                self.rowid = "_tidb_rowid"  # use _tidb_rowid when no primary or primary is not int
            # if tidb table is sharded, exit directly
            if is_tidb_table:
                sql = f"select TIDB_ROW_ID_SHARDING_INFO from information_schema.TABLES where TABLE_SCHEMA='{self.db}' " \
                      f"and TABLE_NAME='{self.name}'"
                try:
                    c.execute(sql)
                    rowid_shard_info = c.fetchone()[0]
                    if not rowid_shard_info.startswith("NOT_SHARDED"):
                        raise Exception(f"Table {self.name} was set SHARD_ROW_ID_BITS or AUTO_RANDOM! exit...")
                except Exception as e:
                    if e.args[0] == 1054:
                        print(
                            "Warning: TiDB version <=4.0.0, Please check if Rowid was sharded manually before execution!")
                        pass
                        # TIDB_ROW_ID_SHARDING_INFO not supported if tidb version <= 4.0，so print a warning here.
                        # This warning told you to check table sharded info manually.
                    else:
                        raise e
            sql = f"select min({self.rowid}),max({self.rowid}) from {self.db}.{self.name};"
            c.execute(sql)
            self.rowid_min, self.rowid_max = c.fetchone()
        log.info(f"Load Table Info of {self.db}.{self.name} Done.")


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

    def set(self, savepoint):
        with open(self.file_name, "w") as f:
            f.write(str(savepoint))

    def delete(self):
        if os.path.exists(self.file_name):
            os.remove(self.file_name)


class SQLOperator(object):
    def __init__(self, pool: MySQLConnectionPool = None, table: Table = None, sql=None, batch_size=None,
                 max_workers=None, start_rowid=None, end_rowid=None, savepoint_file: str = None, execute=None):
        self.pool: MySQLConnectionPool = pool
        self.table: Table = table
        self.sql: str = sql
        self.table_alias = None
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.start_rowid = int(start_rowid) if start_rowid else self.table.rowid_min
        self.end_rowid = int(end_rowid) if end_rowid else self.table.rowid_max
        self.savepoint = SavePoint(file_name=savepoint_file)
        self.execute = execute

    def validate(self):
        """
        1. only SUPPORTED_SQL_TYPES are supported
        2. exit when no where condition
        3. exit when have unsupported keywords
        4. set table alias to table.name if no alias && add tableRangeScan hint /*+ use_index(table_alias) */ for tidb
        5. wrap where condition with parentheses
        6. set start_rowid to max of [start_rowid, savepoint]
        """
        log.info("Checking SQL ...")
        self.sql = sqlparse.format(self.sql, use_space_around_operators=True, keyword_case="upper")
        log.info(f"SQL will be batched: \n{self.sql}")
        parsed_sql = sqlparse.parse(self.sql)[0]
        # 1
        sql_type = parsed_sql.get_type()
        if sql_type not in SUPPORTED_SQL_TYPES:
            raise Exception(f"Unsupported SQL type: {sql_type}!")

        sql_tokens: sqlparse.sql.TokenList = parsed_sql.tokens
        # 2
        where_tokens = list(filter(lambda token: isinstance(token, sqlparse.sql.Where), sql_tokens))
        if len(where_tokens) == 0:
            raise Exception("No where condition in SQL(try where 1=1), exit...")
        # 3
        for token in sql_tokens:
            if token.ttype == sqlparse.tokens.Keyword and token.value in UNSUPPORTED_KEYWORDS:
                raise Exception(f"Unsupported keyword `{token.value}` in SQL!")
        # 4
        for token in sql_tokens:
            if isinstance(token, sqlparse.sql.Identifier) and token.get_real_name() == self.table.name:
                self.table_alias = token.get_alias() if token.get_alias() else self.table.name
                break
        # if no table_alias found, it means table specified in config file might not exist in sql
        if not self.table_alias:
            raise Exception(f"Table `{self.table.name}` specified in config file not found in sql, please check!")
        for token in sql_tokens:
            if token.ttype == sqlparse.tokens.Keyword and token.value == "FROM":
                self.sql = self.sql.replace("FROM", f"/*+ USE_INDEX({self.table_alias}) */ FROM")
                break
        # 5
        where_token = where_tokens[0]
        where_str = str(where_token)
        new_where = f"WHERE ({where_str[5:]} ) "
        self.sql = self.sql.replace(where_str, new_where, 1)
        # 6
        print("Using savepoint file:", self.savepoint.file_name)
        self.start_rowid = max(self.savepoint.get() + 1, self.start_rowid)
        if self.start_rowid > self.end_rowid:
            raise Exception("start_rowid larger than end_rowid, Nothing to Do, Exit...")
        log.info("SQL Checked.")

    def run(self):
        thread_count = (self.end_rowid - self.start_rowid) // self.batch_size + 1
        log.info(f"Max Thread Count: {thread_count}, Rowid Range [{self.start_rowid},{self.end_rowid}]")
        if not self.execute:
            with ThreadPoolExecutor(max_workers=1) as pool:
                pool.submit(self.__run_batch,
                            self.start_rowid,
                            self.start_rowid + self.batch_size,
                            1,
                            thread_count)
        else:
            print(f"write initial savepoint {self.start_rowid - 1}")
            self.savepoint.set(self.start_rowid - 1)
            i = 0  # release concurrent.futures every 100 threads
            while i < thread_count:
                with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
                    for j in range(i, i + 100):
                        pool.submit(self.__run_batch,
                                    self.start_rowid + (j * self.batch_size),
                                    self.start_rowid + ((j + 1) * self.batch_size),
                                    j + 1,
                                    thread_count)
                i += 100
                sp = self.start_rowid + (i * self.batch_size)
                print(f"write savepoint {sp}, complete percent: {round(sp * 100 / self.table.rowid_max, 2)}%")
                self.savepoint.set(sp)
            print("All Batches Done, Remove Savepoint File.")
            self.savepoint.delete()

    def __run_batch(self, start: int, stop: int, batch_id, max_batch_id):
        try:
            sql_tokens = sqlparse.parse(self.sql)[0].tokens
            sql_tokens = list(filter(lambda token: token.ttype not in (T.Whitespace, T.Newline), sql_tokens))
            rowid_condition = "WHERE {0}.{1} >= {2} AND {0}.{1} < {3} AND ".format(self.table_alias,
                                                                                   self.table.rowid,
                                                                                   start, stop)
            for i in range(len(sql_tokens)):
                if isinstance(sql_tokens[i], sqlparse.sql.Where):
                    sql_tokens[i].value = sql_tokens[i].value.replace("WHERE", rowid_condition)
                    break
            sql_token_values = list(map(lambda token: token.value, sql_tokens))
            batch_sql = ' '.join(sql_token_values)
        except Exception as e:
            log.error(f"Batch {batch_id} failed with exception {e}, exit... Exception:\n {format_exc()}")
            raise
        if self.execute:
            retry = 0
            while retry < 3:
                conn = self.pool.get()
                try:
                    start_time = datetime.now()
                    with conn.cursor() as c:
                        affected_rows = c.execute(batch_sql)
                    conn.commit()
                    end_time = datetime.now()
                    log.info(f"Batch {batch_id} of {max_batch_id} OK, {affected_rows} Rows Affected ("
                             f"{end_time - start_time}).\nSQL: {batch_sql}")
                    break
                except Exception as e:
                    retry += 1
                    log.error(f"SQL Retry {retry} Failed: {batch_sql}")
                    log.error(f"Batch {batch_id} of {max_batch_id} Failed: {e}, Exception:\n {format_exc()}")
                finally:
                    self.pool.put(conn)
            if retry == 3:
                log.error(f"SQL Retry {retry} Times Failed, Exit Now: {batch_sql}")
                os._exit(1)
        else:
            log.info(f"Batch {batch_id} of {max_batch_id} Dry Run:\nSQL: {batch_sql}")


if __name__ == '__main__':
    args = argParse()
    config_file, log_file, execute_flag = args.config, args.log, args.execute
    conf = Config(config_file=config_file, log_file=log_file)
    conf.parse()
    log = FileLogger(filename=log_file)
    print(f"See logs in {log_file} ...")
    log.info(">>>>>>> DML Tool(by id) Start...")

    # create connection pool
    pool = MySQLConnectionPool(host=conf.host, port=int(conf.port), user=conf.user, password=conf.password,
                               db=conf.db, pool_size=conf.max_workers * 2)
    pool.init()
    pool.start_monitor()
    try:
        # load table info
        conn = pool.get()
        table = Table(table_name=conf.table, db=conf.db, conn=conn)
        table.load()
        pool.put(conn)
        # start sql operator
        operator = SQLOperator(pool=pool, table=table, sql=conf.sql.strip().strip(";"), batch_size=conf.batch_size,
                               max_workers=conf.max_workers, start_rowid=conf.start_rowid, end_rowid=conf.end_rowid,
                               savepoint_file=conf.savepoint, execute=execute_flag)
        operator.validate()
        operator.run()
    except Exception as e:
        log.critical(f"<<<<<<< DML Tool(by id) Failed! Exception: {e}")
        raise e
    finally:
        # close connection pool
        pool.close()
    log.info("<<<<<<< DML Tool(by id) Finished.")
