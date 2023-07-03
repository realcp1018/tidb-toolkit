# Introduction
**Toolkits is for TiDB.**
1. *Run sql on a table which has billions of records, sql will be split into tasks/batches automatically.*
    * *by id (use primary-key id or internal _tidb_rowid)*
    * *by time (use a date/datetime/time-related numerical column)* 
    * **[NEW]** *chunk update(split sql into chunks use rowid, regardless of auto_random or shard_rowid_bits)* 
2. *Pretty print the stores/regions/labels of a tidb cluster, which gives you a clearer understood about the data distribution of your cluster.*
3. *Flashback the whole table to a time point which is before gc_safe_point.* **[Deprecated]**  *use [Binlog reparo tool](https://github.com/realcp1018/tidb-binlog) instead.*

# Python
![py1](images/1.svg)

#### Requirements 
*Run "python3 -m pip install -r requirements.txt" for dependencies.*

Add project path to PYTHONPATH before run:
```
# let's say the repo is in /data
export PYTHONPATH=$PYTHONPATH:/data/tidb-toolkit
```

# Examples
**1. Flashback table tb1kb_1** *[Deprecated]*
```
# edit tidb.toml [basic] and [flashback] part
...
db = "test"
table ="tb1kb_1"
until_time = "2021-12-17 17:29:45"
override = false
...
# Run:
python3 scripts/tk_flashback.py -f conf/tidb.toml -l tb1kb_1.log
```
**2. Execute "delete from where ..." on big table tb1kb_1(table not sharded)**
```
# update tidb.toml's [basic], [dml] and [dml.by_id] part
db = "test"
table = "tb1kb_1"
sql = "delete from tb1kb_1 where is_active=0;"
execute = false
# Run:
python3 scripts/tk_dml_byid.py -f conf/tidb.toml -l tb1kb_1.log
# execute = false: Do nothing except print the first batch's SQL 
# make sure the result sql is correct, then set execute to true and rerun
```
**3. Execute "delete from where ..." on big table tb1kb_1(table sharded)**
```
# update tidb.toml's [basic], [dml] and [dml.by_time] part
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
**4. Show Store/Reions of a cluster**
```
# Examples:
python3 tk_pdctl.py -u <pd ip:port> -o showStores
# Location-Label Rules: [host] (force match: false)
# Space-Ratio Settings: [0.8, 0.85]
StoreAddr                StoreID        State          LCt/RCt        LWt/RWt   SpaceUsed      ......
---------                -------        -----          -------        -------   ---------      ......
192.168.1.1:20160        13982296       Up             126917/249153  1/1       46.90%         ......
192.168.1.1:20161        13982297       Up             6176/118871    1/1       51.73%         ......
192.168.1.1:20162        13982298       Up             5963/17031     1/1       52.38%         ......
192.168.1.2:20160        16094694       Up             56045/212419   1/1       49.38%         ......
192.168.1.2:20161        16094693       Up             6119/60132     1/1       51.47%         ......
192.168.1.2:20162        16094695       Up             6102/72299     1/1       54.38%         ......
192.168.1.3:20160        2512345        Up             20752/71596    1/1       54.97%         ......
192.168.1.3:20161        2512344        Up             5996/17158     1/1       54.38%         ......
192.168.1.3:20162        2512343        Up             11660/54383    1/1       51.23%         ......
192.168.1.4:20160        2512341        Up             15247/98909    1/1       54.00%         ......
192.168.1.4:20161        2512342        Up             10392/53068    1/1       48.90%         ......
192.168.1.4:20162        2512340        Up             13414/71809    1/1       53.68%         ......
192.168.1.5:20160        2512337        Up             78254/90260    1/1       53.13%         ......
192.168.1.5:20161        2512338        Up             50367/71361    1/1       49.88%         ......
192.168.1.5:20162        2512339        Up             24831/56256    1/1       56.07%         ......
```

# Notes
**1. About tk_dml_byid.py and tk_dml_bytime.py:**

Following sql types supported：
```
1.delete from <table> where <...>
2.update <table> set <...> where <...>
3.insert into <target_table> select <...> from <source_table> where <...>
```
By id:
>* Built-in _tidb_rowid will be used as the default split column.
>* If table was sharded(`SHARD_ROW_ID_BITS or auto_random used`), use tk_dml_bytime instead.
>* SQL will be splited into multiple batches by _tidb_rowid&batch_size(`new sqls with between statement on _tidb_rowid`), there will be <max_workers> batches run simultaneously.

By time:
>* SQL will be splitted into multiple tasks by split_column & split_interval(`new sqls with between statement on split column`).
>* Every task run batches serially, default batch size = 1000(`task sql will be suffixed by [limit 1000] and run multiple times until affected_rows=0`).
>* There will be <max_workers> tasks run simultaneously.
>* Run `grep Finished <log-name> | tail` to get how many tasks finished.

**2. About tk_chunk_update.py:**

A promoted way compared with `tk_dml_byid.py` and `tk_dml_bytime.py` when you want to do dml on a large table.
```
# update tidb.toml's [basic] [dml] and [dml.chunk_update] part
# Run:
python3 scripts/tk_chunk_update.py -f conf/tidb.toml -l <log-path>.log
```
Chunk update:
>* SQL will be splitted into multiple chunks by rowid, just like by_id
>* by_id will process a lot of unused rowids because these rowid may not exits, while chunk_update use `order by rowid limit <chunk_size>` to produce chunks    
>* by_id can not process tables which shard_rowid_bits/auto_random was set, while chunk_update can be used with all tidb tables
>* by_time still can be used when you just want to process the target time range, by_id and chunk_update will scan all the rowids

See complete percentage with:

`tailf <log-file>|grep "write savepoint"`

**3. About savepoint**

tk_dml_by_id.py and tk_chunk_update.py will write savepoint on running.
If process failed or stopped, just rerun it and the savepoint will be used as start rowid

tk_dml_by_time.py will **not** write savepoint on running. If process failed or stopped you can set a new 
start_time in tidb.toml