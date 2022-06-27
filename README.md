# Introduction
Toolkits is for TiDB. 
1. Flashback the whole table to a time point which is before gc_safe_point.
2. Run sql on a table which has billions of records, sql will be split into tasks/batches automatically.
    * by id (use primary-key id or internal _tidb_rowid)
    * by time (use a date/datetime/time-related numerical column)
3. Pretty print the stores/regions/labels of a tidb cluster, which gives you a clearer understood about the data distribution of your cluster.

# Python
![](https://img.shields.io/static/v1?label=Python&message=3.6&color=green&?style=for-the-badge)

#### Requirements 
Run "python3 -m pip install -r requirements.txt" for dependencies.

# Examples
##### 1. Flashback table tb1kb_1
```
# edit tidb.toml basic and flashback part
...
db = "test"
table ="tb1kb_1"
until_time = "2021-12-17 17:29:45"
override = false
...
# Run:
python3 scripts/tk_flashback.py -f conf/tidb.toml -l tb1kb_1.log
```
##### 2. Execute "delete from where ..." on big table tb1kb_1(table not sharded)
```
# update tidb.toml's basic, dml and dml.by_id part
db = "test"
table = "tb1kb_1"
sql = "delete from tb1kb_1 where is_active=0;"
execute = false
# Run:
python3 scripts/tk_dml_byid.py -f conf/tidb.toml -l tb1kb_1.log
# execute = false: Do nothing except print the first batch's SQL 
# make sure the result sql is correct, then set execute to true and rerun
```
##### 3. Execute "delete from where ..." on big table tb1kb_1(table sharded)
```
# update tidb.toml's basic, dml and dml.by_time part
db = "test"
table = "tb1kb_1"
sql = "delete from tb1kb_1 where is_active=0;"
# assume create_time type is int(timstamp in miliseconds)
split_column = "create_time"
split_column_precision = 3
split_interval = 3600
start_time = "2021-01-01 00:00:00"
end_time = "2021-12-31 00:00:00"
execute = false
# Run:
python3 scripts/tk_dml_byid.py -f conf/tidb.toml -l tb1kb_1.log
```
##### 4. Show Store/Reions of a cluster
```
# Examples:
python tk_pdctl.py -u <pd ip:port> -o showStores
# Location-Label Rules: host (force: false)
StoreAddr                StoreID        State          LCt/RCt        LWt/RWt ...
---------                -------        -----          -------        ------- ...
1.1.1.1:20171            6              Up             3370/10910     1/1     ... 
```
# Notes
#### About tk_dml_byid.py and tk_dml_bytime.py:
Following sql types supported：
```
1.delete from <table> where <...>
2.update <table> set <...> where <...>
3.insert into <target_table> select <...> from <source_table> where <...>
```
By id:
>* _tidb_rowid will be used as the default split column.
>* If table was sharded(SHARD_ROW_ID_BITS or auto_random used), use tk_dml_bytime instead.
>* SQL will be splited into multiple batches by _tidb_rowid&batch_size(new sqls with between statement on _tidb_rowid), there will be <max_workers> batches run simultaneously.

By time:
>* SQL will be splitted into multiple tasks by split_column & split_interval(new sqls with between statement on split column).
>* Every task will run batches serially, default batch size is 1000(which means task sql will be suffixed by `limit 1000` and run multiple times until affected rows=0).
>* There will be <max_workers> tasks run simultaneously.
>* Run `grep Finished <log-name> | tail` to find out how many tasks finished.