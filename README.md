# Introduction
Toolkits is for TiDB. 

# Python Env
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
StoreAddr                StoreID        State          LCt/RCt        LWt/RWt 
---------                -------        -----          -------        -------
1.1.1.1:20171            6              Up             3370/10910     1/1    
```
