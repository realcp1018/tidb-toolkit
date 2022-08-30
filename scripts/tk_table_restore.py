# coding=utf-8
# @Time: 2021/1/06 14:38
# @Author: forevermessi@foxmail.com
"""
Usage:
    table will be restored to `_<table_name>_YYYYMMDDHH24MISS`
    if override is true，the original table will be overrided by the new table.
"""
import sys
import pymysql
import threading
import argparse
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from utils.color import Color
from utils.config import Config
from utils.logger import FileLogger


def argParse():
    parser = argparse.ArgumentParser(description="TiDB Flashback Table Tool.")
    parser.add_argument("-f", dest="config", type=str, required=True, help="config file")
    parser.add_argument("-l", dest="log", type=str, help="Log File Name, Default <host>.log.<now>")
    args = parser.parse_args()
    return args


class FlashBackOperator(object):
    def __init__(self, config: Config = None):
        self.host = config.host
        self.port = config.port
        self.user = config.user
        self.password = config.password
        self.db = config.db
        self.table = config.table
        self.new_table = "_%s_%s" % (self.table, datetime.now().strftime('%Y%m%d%H%M%S'))
        self.until_time = datetime.strptime(config.until_time, '%Y-%m-%d %H:%M:%S')
        self.batch_size = config.batch_size
        self.max_workers = config.max_workers
        self.sem = threading.Semaphore(self.max_workers)
        self.override = config.override
        self.start_time = datetime.now()
        self.elapsed_time = None
        self.connect_info = {'host': self.host, 'port': int(self.port), 'user': self.user, 'password': self.password,
                             'db': self.db, 'connect_timeout': 5}

    def change_tidb_gc_value(self, gc_value):
        mysql_conn = pymysql.connect(**self.connect_info)
        sql = "update mysql.tidb set VARIABLE_VALUE='%s' where VARIABLE_NAME in ('tikv_gc_life_time'," \
              "'tikv_gc_run_interval');" % gc_value
        with mysql_conn.cursor() as c:
            c.execute(sql)
            mysql_conn.commit()
        mysql_conn.close()
        log.info("tikv_gc_run_interval/tikv_gc_life_time was set to %s ..." % gc_value)

    def check_gc_safe_point(self):
        # check tikv_gc_safe_point
        mysql_conn = pymysql.connect(**self.connect_info)
        with mysql_conn.cursor() as c:
            query = "select VARIABLE_VALUE from mysql.tidb where VARIABLE_NAME='tikv_gc_safe_point'"
            c.execute(query)
            tikv_gc_safe_point = c.fetchone()[0][:-6]  # cut off time-zone part
            mysql_conn.commit()
        mysql_conn.close()
        tikv_gc_safe_point = datetime.strptime(tikv_gc_safe_point, '%Y%m%d-%H:%M:%S')
        if self.until_time <= tikv_gc_safe_point:
            log.info("Specifed until_time(%s) is before tikv_gc_safe_point(%s), exit..." % (self.until_time,
                                                                                            tikv_gc_safe_point))
            sys.exit(1)
        else:
            # set tikv_gc_life_time to 720h
            self.change_tidb_gc_value('720h')

    def create_new_table(self):
        mysql_conn = pymysql.connect(**self.connect_info)
        with mysql_conn.cursor() as c:
            c.execute("set @@tidb_snapshot=\'%s\'" % self.until_time)
            c.execute("show create table %s" % self.table)
            create_sql = c.fetchone()[1]
            c.execute("set @@tidb_snapshot=''")
            create_sql = create_sql.replace("CREATE TABLE `%s`" % self.table, "CREATE TABLE `%s`" % self.new_table)
            log.info(create_sql)
            c.execute(create_sql)
            mysql_conn.commit()
        mysql_conn.close()

    def get_rowid_info(self):
        mysql_conn = pymysql.connect(**self.connect_info)
        with mysql_conn.cursor() as c:
            c.execute("set @@tidb_snapshot=\'%s\'" % self.until_time)
            rowid_check_sql = "select COLUMN_NAME,DATA_TYPE from information_schema.COLUMNS where table_schema='%s' " \
                              "and table_name='%s' and COLUMN_KEY='PRI';" % (self.db, self.table)
            c.execute(rowid_check_sql)
            rowid_info = c.fetchall()
            if len(rowid_info) == 1 and rowid_info[0][1] in ('int', 'bigint'):
                rowid_column = rowid_info[0][0]
            else:
                rowid_column = '_tidb_rowid'
            sql = "select TIDB_ROW_ID_SHARDING_INFO from information_schema.TABLES where TABLE_SCHEMA='{0}' " \
                  "and TABLE_NAME='{1}'".format(self.db, self.table)
            try:
                c.execute(sql)
                rowid_shard_info = c.fetchone()[0]
                if not rowid_shard_info.startswith("NOT_SHARDED"):
                    log.error("Rowid is sharded! Either AUTO_RANDOM or SHARD_ROW_ID_BITS was set, not supported!")
                    sys.exit(1)
            except Exception as e:
                if e.args[0] == 1054:
                    log.warning("Warning: TiDB version <=4.0.0, Please check if Rowid was sharded before execution!")
                    pass
                else:
                    raise e
            c.execute("select max(%s) from %s" % (rowid_column, self.table))
            max_rowid = c.fetchone()[0]
        mysql_conn.close()
        return rowid_column, max_rowid

    def flashback_table_batch(self, rowid_column, batch_seq):
        with self.sem:
            mysql_conn = pymysql.connect(**self.connect_info)
            with mysql_conn.cursor() as c:
                c.execute("set @@tidb_snapshot=\'%s\'" % self.until_time)
                query = "SELECT * FROM %s WHERE %s BETWEEN %d AND %d" % (
                    self.table, rowid_column, batch_seq * self.batch_size,
                    batch_seq * self.batch_size + self.batch_size - 1)
                c.execute(query)
                result = c.fetchall()
                if result:
                    column_count = len(result[0])
                    insert_sql = "insert into %s values(" % self.new_table
                    for i in range(column_count):
                        insert_sql += "%s,"
                    insert_sql = insert_sql.strip(',') + ')'
                    # write operation not allowed when you set a @@tidb_snapshot，so restore is to empty
                    c.execute("set @@tidb_snapshot=''")
                    # c.execute("set @@allow_auto_random_explicit_insert = true")
                    c.executemany(insert_sql, result)
                    mysql_conn.commit()
                    log.info("\t%s\t(%d rows processed)..." % (query, len(result)))
                else:
                    # log.info("\t%s\t(0 rows processed)..." % query)
                    pass
            mysql_conn.close()

    def flashback_table(self):
        self.create_new_table()
        rowid_column, max_rowid = self.get_rowid_info()
        log.info("Flashbacking table `%s`(%d rows estimated]) to `%s`..." % (self.table,
                                                                             max_rowid if max_rowid else 0,
                                                                             self.new_table))
        try:
            max_thread_count = max_rowid // self.batch_size + 1 if max_rowid else 0
            log.info("[MAX_THREAD_COUNT: %d]" % max_thread_count)
            i = 0
            while i < max_thread_count:
                with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
                    # release futures every 200 threads
                    for j in range(i, i + 200):
                        pool.submit(self.flashback_table_batch,
                                    rowid_column,
                                    j)
                i += 200
        except Exception as e:
            log.error(e)
            log.info("\tFlashbacking table `%s` failed, exit..." % self.table)
            sys.exit(1)

    def check_override(self):
        if self.override:
            log.info("\tWill override table `%s`..." % self.table)
            mysql_conn = pymysql.connect(**self.connect_info)
            with mysql_conn.cursor() as c:
                log.info("\tDrop table `%s` if exists..." % self.table)
                c.execute("drop table if exists %s" % self.table)
                log.info("\tRename table `%s` to `%s`..." % (self.new_table, self.table))
                c.execute("alter table %s rename to %s" % (self.new_table, self.table))
                mysql_conn.commit()
            mysql_conn.close()
            end_time = datetime.now()
            self.elapsed_time = end_time - self.start_time
            log.info("\tFlashback table `%s` Done.%s(Elapsed Time: %s)%s" % (self.table, Color.font.yellow,
                                                                             self.elapsed_time, Color.display.default))
        else:
            end_time = datetime.now()
            self.elapsed_time = end_time - self.start_time
            log.info("\tFlashback table `%s` Done, find data in table `%s`.(Elapsed Time: %s)" \
                     % (self.table, self.new_table, self.elapsed_time))


if __name__ == '__main__':
    args = argParse()
    config_file, log_file = args.config, args.log
    conf = Config(config_file=config_file, log_file=log_file)
    conf.parse()
    log = FileLogger(filename=conf.log_file)
    print(f"See logs in {conf.log_file} ...")
    try:
        log.info("----------------------------------------------------------------------------------------------")
        log.info("[Table `%s` Flashback]:" % conf.table)
        operator = FlashBackOperator(config=conf)
        log.info("Connect Info: %s@%s:%d ..." % (operator.user, operator.host, operator.port))
        operator.check_gc_safe_point()
        operator.flashback_table()
        operator.check_override()
    except Exception as e:
        log.error("Table flashback failed, please check!!!")
        log.warning("tikv_gc_life_time/tikv_gc_run_interval was set to 720h, Please recover it manually!")
        raise
    else:
        log.info("FlashBack Done!")
        log.warning("tikv_gc_life_time/tikv_gc_run_interval was set to 720h, Please recover it manually!")
