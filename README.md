# Introduction
Toolkits is for TiDB. 

Like splitting large scale DML SQL into small batches, flashback full table based on tidb GC.  

# Python Env
![](https://img.shields.io/static/v1?label=Python&message=3.6&color=green&?style=for-the-badge)

## Prepare 
Run "python3 -m pip install -r requirements.txt" for dependencies.

## Examples
    
Use independent tidb.toml for every single task:

###### 1. Flashback table tb1kb_1
```
# tidb.toml's [basic] part is for all scripts.
# update [flashback] part for flashback task：
db = "test"
table ="tb1kb_1"
until_time = "2021-12-17 17:29:45"
override = false

# Run 
python3 scripts/tk_flashback.py -f conf/tidb.toml > tb1kb_1.log
```
###### 2. Execute "delete from where ..." on table tb1kb_1(which has billions of records)
```
# update tidb.toml's [dml] part
db = "test"
table = "tb1kb_1"
sql = "delete from tb1kb_1 where is_active=0;"
execute = false

# execute = false means: just print the first batch's SQL, no running
python3 scripts/tk_dml_byid.py -f conf/tidb.toml -l 111.log
# make sure the result sql is correct, then set execute to true and rerun
```
###### 3. Show Store/Reions info of a cluster(no need for tidb.toml)  
```
# Examples:
python tk_pdctl.py -u <pd ip:port> -o showStores
# Location-Label Rules: host (force: false)
StoreAddr                StoreID        State          LCt/RCt        LWt/RWt   StartTime                     
---------                -------        -----          -------        -------   ---------                    
1.1.1.1:20171            6              Up             3370/10910     1/1       2020-11-19T09:18:06+08:00
```
