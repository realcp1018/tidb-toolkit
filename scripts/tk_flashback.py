# coding=utf-8
# @Time: 2021/1/06 14:38
# @Author: forevermessi@foxmail.com
"""
Usage:
    将指定指定表全量恢复到指定的时间点,默认恢复到一个快照表中不会覆盖原表
    当集群版本>=4.0时可以使用官方的FLASHBACK TABLE table_name [TO other_table_name]语法来恢复drop和truncate删除的表
    但是flashback语法有一些限制：
        1.只能恢复DDL语句
        2.只能恢复到的最近的一个DDL之前，无法指定时间点恢复
    本工具默认将表恢复至`_<table_name>_YYYYMMDDHH24MISS`表内，如果指定了override项，那么会直接使用恢复表覆盖原表(即不会保留原表，释放磁盘空间)
"""
import sys
import toml
import pymysql
import threading
import argparse
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from utils.color import Color

color = Color()


def argParse():
    parser = argparse.ArgumentParser(description="TiDB Flashback Table Tool.")
    parser.add_argument("-f", dest="config", type=str, required=True, help="config file")
    args = parser.parse_args()
    return args


class Config(object):
    def __init__(self, config_file):
        # toml config file
        self.config_file = config_file
        self.host, self.port, self.user, self.password = None, None, None, None
        self.db = None
        self.table = None
        self.until_time = None
        self.batch_size = None
        self.max_workers = None
        self.override = None

    def parse(self):
        with open(self.config_file, encoding='utf8') as f:
            config = toml.load(f)
        self.host = config["basic"]["host"]
        self.port = config["basic"]["port"]
        self.user = config["basic"]["user"]
        self.password = config["basic"]["password"]
        self.db = config["flashback"]["db"]
        self.table = config["flashback"]["table"]
        self.until_time = config["flashback"]["until_time"]
        self.batch_size = config["flashback"].get("batch_size", 1000)
        self.max_workers = config["flashback"].get("max_workers", 20)
        self.override = config["flashback"].get("override", False)


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
        sql = "update mysql.tidb set VARIABLE_VALUE='%s' where VARIABLE_NAME in ('tikv_gc_life_time','tikv_gc_run_interval');" % gc_value
        with mysql_conn.cursor() as c:
            c.execute(sql)
            mysql_conn.commit()
        mysql_conn.close()
        print("tikv_gc_run_interval/tikv_gc_life_time was set to %s ..." % gc_value)

    def check_gc_safe_point(self):
        # 检查集群tikv_gc_safe_point，当until_time<=tikv_gc_safe_point时无法进行flashback
        mysql_conn = pymysql.connect(**self.connect_info)
        with mysql_conn.cursor() as c:
            query = "select VARIABLE_VALUE from mysql.tidb where VARIABLE_NAME='tikv_gc_safe_point'"
            c.execute(query)
            tikv_gc_safe_point = c.fetchone()[0][:-6]  # 去除时区部分
            mysql_conn.commit()
        mysql_conn.close()
        tikv_gc_safe_point = datetime.strptime(tikv_gc_safe_point, '%Y%m%d-%H:%M:%S')
        if self.until_time <= tikv_gc_safe_point:
            print("Specifed until_time(%s) is before tikv_gc_safe_point(%s), exit..." % (self.until_time,
                                                                                         tikv_gc_safe_point))
            sys.exit(1)
        else:
            # 当符合flashback条件时将集群tikv_gc_life_time设置为720h,为兼顾多次恢复的场景，此值修改后不会自动恢复
            self.change_tidb_gc_value('720h')

    def create_new_table(self):
        mysql_conn = pymysql.connect(**self.connect_info)
        with mysql_conn.cursor() as c:
            # 存储快照中的表结构，然后新建临时表(设置快照时不能有写操作所以需要多次切换快照参数)
            c.execute("set @@tidb_snapshot=\'%s\'" % self.until_time)
            c.execute("show create table %s" % self.table)
            create_sql = c.fetchone()[1]
            c.execute("set @@tidb_snapshot=''")
            create_sql = create_sql.replace("CREATE TABLE `%s`" % self.table, "CREATE TABLE `%s`" % self.new_table)
            color.print_yellow(create_sql)
            c.execute(create_sql)
            mysql_conn.commit()
        mysql_conn.close()

    def get_rowid_info(self):
        mysql_conn = pymysql.connect(**self.connect_info)
        with mysql_conn.cursor() as c:
            # 读取快照获取总行数
            c.execute("set @@tidb_snapshot=\'%s\'" % self.until_time)
            # 检查表主键，单数字型主键使用主键排序，否则使用_tidb_rowid
            rowid_check_sql = "select COLUMN_NAME,DATA_TYPE from information_schema.COLUMNS where table_schema='%s' " \
                              "and table_name='%s' and COLUMN_KEY='PRI';" % (self.db, self.table)
            c.execute(rowid_check_sql)
            rowid_info = c.fetchall()
            if len(rowid_info) == 1 and rowid_info[0][1] in ('int', 'bigint'):
                rowid_column = rowid_info[0][0]
            else:
                rowid_column = '_tidb_rowid'
            # 检查shard_rowid_bits情况，当主键为auto_random或者_tidb_rowid设置了hard_rowid_bits时，报错退出：
            sql = "select TIDB_ROW_ID_SHARDING_INFO from information_schema.TABLES where TABLE_SCHEMA='{0}' " \
                  "and TABLE_NAME='{1}'".format(self.db, self.table)
            try:
                c.execute(sql)
                rowid_shard_info = c.fetchone()[0]
                if not rowid_shard_info.startswith("NOT_SHARDED"):
                    print("Rowid is sharded! Either AUTO_RANDOM or SHARD_ROW_ID_BITS was set, not supported!")
                    sys.exit(1)
            except Exception as e:
                if e.args[0] == 1054:
                    print("Warning: TiDB version <=4.0.0, Please check if Rowid was sharded before execution!")
                    pass
                    # 小于4.0的版本没有TIDB_ROW_ID_SHARDING_INFO字段，因此打印一句警告，提示需要注意rowid是否分片
                else:
                    raise e
            # 获取表的最大rowid
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
                    # 当设置显式的@@tidb_snapshot时，会话无法执行写操作，因此需要还原为空值
                    c.execute("set @@tidb_snapshot=''")
                    # c.execute("set @@allow_auto_random_explicit_insert = true")
                    c.executemany(insert_sql, result)
                    mysql_conn.commit()
                    print("\t%s\t(%d rows processed)..." % (query, len(result)))
                else:
                    # print("\t%s\t(0 rows processed)..." % query)
                    pass
            mysql_conn.close()

    def flashback_table(self):
        self.create_new_table()
        # 回滚表到指定时间点
        rowid_column, max_rowid = self.get_rowid_info()
        print("Flashbacking table `%s`(%d rows estimated]) to `%s`..." % (self.table,
                                                                          max_rowid if max_rowid else 0,
                                                                          self.new_table))
        try:
            max_thread_count = max_rowid // self.batch_size + 1 if max_rowid else 0
            print("[MAX_THREAD_COUNT: %d]" % max_thread_count)
            i = 0
            while i < max_thread_count:
                with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
                    # 每完成200个线程释放一次futures
                    for j in range(i, i + 200):
                        pool.submit(self.flashback_table_batch,
                                    rowid_column,
                                    i + j)
                i += 200
        except Exception as e:
            print(e)
            print("\tFlashbacking table `%s` failed, exit..." % self.table)
            sys.exit(1)

    def check_override(self):
        # 至此数据恢复完毕，选择是否覆盖原表
        if self.override:
            print("\tWill override table `%s`..." % self.table)
            mysql_conn = pymysql.connect(**self.connect_info)
            with mysql_conn.cursor() as c:
                print("\tDrop table `%s` if exists..." % self.table)
                c.execute("drop table if exists %s" % self.table)
                print("\tRename table `%s` to `%s`..." % (self.new_table, self.table))
                c.execute("alter table %s rename to %s" % (self.new_table, self.table))
                mysql_conn.commit()
            mysql_conn.close()
            end_time = datetime.now()
            self.elapsed_time = end_time - self.start_time
            print("\tFlashback table `%s` Done.%s(Elapsed Time: %s)%s" % (self.table, Color.font.yellow,
                                                                          self.elapsed_time, Color.display.default))
        else:
            end_time = datetime.now()
            self.elapsed_time = end_time - self.start_time
            print("\tFlashback table `%s` Done, find data in table `%s`.%s(Elapsed Time: %s)%s" \
                  % (self.table, self.new_table, Color.font.yellow, self.elapsed_time, Color.display.default))


if __name__ == '__main__':
    args = argParse()
    config_file = args.config
    conf = Config(config_file=config_file)
    conf.parse()
    try:
        print("----------------------------------------------------------------------------------------------")
        color.print_highlight("[Table `%s` Flashback]:" % conf.table)
        operator = FlashBackOperator(config=conf)
        print("Connect Info: %s@%s:%d ..." % (operator.user, operator.host, operator.port))
        operator.check_gc_safe_point()
        operator.flashback_table()
        operator.check_override()
    except Exception as e:
        color.print_red("Table flashback failed, please check!!!")
        color.print_red("tikv_gc_life_time/tikv_gc_run_interval was set to 720h, Please recover it manually!")
        raise
    else:
        color.print_blue("FlashBack Done!")
        recover_gc = input("Recover tikv_gc_life_time/tikv_gc_run_interval or not:[y/n]")
        if recover_gc.lower() in ["y", "yes"]:
            operator.change_tidb_gc_value("30m")
        else:
            pass
