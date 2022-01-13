# Introduction
Toolkits is for TiDB. 

Like splitting large scale DML SQL into small batches, flashback full table based on tidb GC.  

# Python Env
![](https://img.shields.io/static/v1?label=Python&message=3.5&color=green&?style=for-the-badge)

## Prepare 
Run "python3 -m pip install -r requirements.txt" for dependencies.

## Examples
    
Use independent tidb.toml for every single task:

###### 1. Flashback table tb1kb_1
    # tidb.toml's [basic] part is for all scripts.
    # update [flashback] part for flashback task：
    db = "test"
    table ="tb1kb_1"
    until_time = "2021-12-17 17:29:45"
    override = false
    
    # Run 
    python3 scripts/tidb_flashback.py -f conf/tidb.toml
    
###### 2. Execute "delete from where ..." on table tb1kb_1(which has billions of records)
    # update tidb.toml's [massive_dml] part
    db = "test"
    table = "tb1kb_1"
    sql = "delete from tb1kb_1 where is_active=0;"
    execute = false
    
    # execute = false means: just print the first batch's SQL, no running
    python3 scripts/tidb_massive_dml.py -f conf/tidb.toml
    # make sure the result sql is correct, then set execute to true and rerun
    
###### 3. Show Store/Reions info of a cluster(no need for tidb.toml)  
    # Help:
    python tidb_store_region.py -h
    usage: tidb_store_region.py [-h] -u URL [-s STOREID] [-r REGIONID] -o
                            {showStore,showStores,showRegion,showRegions,showStoreRegions,removeRegionPeer,removeStorePeers}
                            [-l LIMIT]

    PD HTTP API store/region info formatter.
    
    optional arguments:
      -h, --help            show this help message and exit
      -u URL                PD Addr(ip:port)
      -s STOREID, 
      --store_id STOREID
                            Store ID
      -r REGIONID, 
      --region_id REGIONID
                            Region ID
      -o {showStore,showStores,showRegion,showRegions,showStoreRegions,removeRegionPeer,removeStorePeers}, 
      --option {showStore,showStores,showRegion,showRegions,showStoreRegions,removeRegionPeer,removeStorePeers}
                            Store/Region Actions
      -l LIMIT, 
      --limit LIMIT
                            Region show limit(default 5)
    
    # Examples:
    python tidb_store_region.py -u <your pd ip:port> -o showStores
    StoreAddr         StoreID        State          LCt/RCt        LWt/RWt        StartTime                     
    ---------         -------        -----          -------        -------        ---------                     
    <store_addr>      1015857        Down           0/34145        0/0            2021-12-12T22:00:45+08:00 
    ......
    
    python tidb_store_region.py -u <your pd ip:port> -o showRegions
    # top 5 Regions(limit 5):
    RegionID       StoreList               Leader     LeaderAddr        DownPeersStoreID    PendingPeersStoreID      Size      Keys      
    --------       ---------               ------     ----------        ----------------    -------------------      ----      ----      
    3356430        [4, 1015859, 5]         4          <region_leader>   [1015859]           [1015859]                95        1070886   
    2233918        [4, 1015859, 2740576]   4          <region_leader>   [1015859]           [1015859]                95        1070886   
    1739070        [4, 1015859, 2740574]   4          <region_leader>   [1015859]           [1015859]                44        491521    
    3024458        [1015859, 2740576, 4]   4          <region_leader>   [1015859]           [1015859]                95        1070886   
    1667897        [4, 1015859, 2740576]   4          <region_leader>   [1015859]           [1015859]                118       1326804 
