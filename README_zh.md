# 简介
**TiDB辅助工具集，用途包含：**
1. *在数亿、数十亿的表上高效的删除大量数据而避免触发tidb事务大小限制*
    * *by id (通过rowid`(主键数字id或内置的_tidb_rowid)`进行拆分,不支持auto_random或设置了shard_rowid_bits的表)*
    * *by time (通过 date/datetime/time-related numerical 等时间列/数字时间列进行拆分)*
    * **[新增]** *chunk update(通过rowid分进行动态定界和拆分，兼容auto_random或设置了shard_rowid_bits的表)* 
2. *格式化打印tidb集群的 stores/regions/location-labels 信息, 以及一些核心的pd配置信息和警告，可以迅速厘清集群的region分布情况*
3. *闪回整表至gc_safe_point之后的任意时间点* **[废弃]**

# Python环境
![py1](images/1.svg)

#### 环境要求 
*运行 "python3 -m pip install -r requirements.txt" 安装python3库依赖.*

将项目目录添加至$PYTHONPATH：
```
# 假设项目被clone至/data目录下：
export PYTHONPATH=$PYTHONPATH:/data/tidb-toolkit
```

# 全部示例：
**1. 闪回表 tb1kb_1** *[废弃]*
```
# 编辑 tidb.toml 的[basic] 和 [flashback] 部分，其他部分的设置不影响本次运行
...
db = "test"
table ="tb1kb_1"
until_time = "2021-12-17 17:29:45"
override = false
...
# 运行:
python3 scripts/tk_flashback.py -f conf/tidb.toml -l tb1kb_1.log
```
**2. 对大表执行 "delete from where ..." (表必须未设置auto_random或shard_rowid_bits，如果误在此类表上运行也没事，只是效率极底)**
```
# 编辑 tidb.toml 的 [basic], [dml] 和 [dml.by_id] 部分，其他部分的设置不影响本次运行
db = "test"
table = "tb1kb_1"
sql = "delete from tb1kb_1 where is_active=0;"
execute = false
# 运行:
python3 scripts/tk_dml_byid.py -f conf/tidb.toml -l tb1kb_1.log
# execute = false: 设置此项表示不实际进行数据删除，仅打印一个拆分后的示例SQL，适用于比较谨慎的场景
# 确保输出的拆分SQL符合预期，然后可以修改为true实际运行
```
**3. 对大表执行 "delete from where ..." (表已设置auto_random或shard_rowid_bits，或者仅仅想根据时间列删除极少部分数据)**
```
# 编辑 tidb.toml 的 [basic], [dml] and [dml.by_time] 部分
db = "test"
table = "tb1kb_1"
sql = "delete from tb1kb_1 where is_active=0;"
# 假设 create_time 类型为 int(时间精度为ms)
split_column = "create_time"
split_column_precision = 3
split_interval = 3600
start_time = "2021-01-01 00:00:00"
end_time = "2021-12-31 00:00:00"
execute = false
# 运行:
python3 scripts/tk_dml_byid.py -f conf/tidb.toml -l tb1kb_1.log
```
**4. 对大表执行 "delete from where ..." (通用脚本，无需考虑表是否设置auto_random或shard_rowid_bits)**
```
# 编辑 tidb.toml 的 [basic], [dml] 和 [dml.chunk_update] 部分
db = "test"
table = "tb1kb_1"
sql = "delete from tb1kb_1 where is_active=0;"
execute = false
# 运行:
python3 scripts/tk_chunk_update.py -f conf/tidb.toml -l tb1kb_1.log
# execute = false
# 确保输出的拆分SQL符合预期，然后可以修改为true实际运行
```
**5. 展示集群 Store/Reions 信息**
```
# 示例:
python3 tk_pdctl.py -u <pd ip:port> -o showStores
PD Addr: ...
PD Core Configs:
>> Location-Label Rules: [zone,host] (force match: false)
>> Space-Ratio Settings: [0.6, 0.8]
StoreAddr                StoreID        State          LCt/RCt        LWt/RWt   SpaceUsed      ......
---------                -------        -----          -------        -------   ---------      ......
192.168.1.1:20160        1              Up             12691/24915    1/1       46.90%         ......
192.168.1.1:20161        2              Up             6176/118871    1/1       51.73%         ......
192.168.1.1:20162        3              Up             5963/17031     1/1       52.38%         ......
192.168.1.2:20160        4              Up             56045/212419   1/1       49.38%         ......
192.168.1.2:20161        5              Up             6119/60132     1/1       51.47%         ......
192.168.1.2:20162        6              Up             6102/72299     1/1       54.38%         ......
192.168.1.3:20160        7              Up             20752/71596    1/1       54.97%         ......
192.168.1.3:20161        8              Up             5996/17158     1/1       54.38%         ......
192.168.1.3:20162        9              Up             11660/54383    1/1       51.23%         ......
192.168.1.4:20160        10             Up             15247/98909    1/1       54.00%         ......
192.168.1.4:20161        11             Up             10392/53068    1/1       48.90%         ......
192.168.1.4:20162        12             Up             13414/71809    1/1       53.68%         ......
192.168.1.5:20160        13             Up             78254/90260    1/1       53.13%         ......
192.168.1.5:20161        14             Up             50367/71361    1/1       49.88%         ......
192.168.1.5:20162        15             Up             24831/56256    1/1       56.07%         ......
```
格式化的打印出集群所有tikv实例的地址、ID、状态、LeaderCount/RegionPeerCount、LeaderWeight/RegionWeight、capacity使用率、 label信息等。

# 常见问题
**1. 如何在tk_chunk_update 和 tk_dml_by_id, tk_dml_by_time之间做出选择？**

tk_chunk_update是最通用的，相比tk_dml_by_id可以避免大量无效rowid扫描，相比tk_dml_by_time则单条SQL执行更快。
而且tk_chunk_update无需人为进行表的类型判断，适用性高。

但是如果表包含大量空region，那么使用tk_chunk_update可能遭遇性能衰退的窘境，此时在执行日志中你会看到如下情况：

`ConnectionPool Monitor: Size 99`

以及:

`chunk xxx Done [split_time=0:00:00.523525] [duration=0:00:00.229860] [rows=1000] [sql=...]`

可以看到 split_time 大于 duration,这意味着chunk的生产速度慢于消费速度, 而tk_chunk_update的核心之一是就是需要保证chunk的生产速度远大于消费速度。

**其原理如下**, 首先ChunkSpliter.split()通过如下查询条件获取chunk的右边界：
```sql
select max(rowid) from 
   (select rowid from table_name where rowid > current_rowid order by rowid limit 0,chunk_size) t`
```
并以current_rowid作为chunk左边界，然后生成拆分后的SQL，输出一个Chunk同时将current_rowid推进至chunk右边界以便下一个chunk拆分使用，而split()是一个python生成器。

然后Executor遍历split()生成的chunk，调用其execute方法并将其作为一个future放入ThreadPoolExecutor中(执行并发度为max_workers)。

这个机制要求chunk的生成速度大于消费速度，否则会衰退为单线程执行，但是为了规避by_id的缺点又必须采用这种动态生成的方式，
因此当你遇到此类性能衰退时, 请使用 tk_dml_by_id/tk_dml_by_time.

**2. 关于 tk_dml_byid.py 和 tk_dml_bytime.py:**

此类工具支持以下几种SQL类型：
```
1.delete from <table> where <...>
2.update <table> set <...> where <...>
3.insert into <target_table> select <...> from <source_table> where <...>
```
By id:
>* 默认使用rowid作为拆分列(如官网所示，数字类型主键就是rowid，其他情况有一个内置的_tidb_rowid作为rowid)
>* 如果表设置了(`SHARD_ROW_ID_BITS 或 auto_random used`), 那么建议使用 tk_dml_bytime 或 tk_chunk_update.
>* SQL的拆分方式很简单，直接按rowid累加batch_size拆分为无数个batch(`rowid >= 1 and rowid < 1000`), 并发执行度为 <max_workers>.

By time:
>* 与by id的拆分方式相似，但是是通过时间列拆分为无数个task，拆分单位为配置文件中的 split_interval
>* 执行方式与by id略有不同，因为按时间列拆分后的task内部可能包含的记录数扔超出事务限制，因此实际上在task内部会以batch_size为单位顺序执行同一条分页SQL直到影响行数为0
>* 通过 `grep Finished <log-name> | tail` 可以看到有多少task已完成

**3. 关于 tk_chunk_update.py:**

通过如下命令可以查看当前任务的执行进度：

`tailf <log-file>|grep "write savepoint"`

**4. 关于 savepoint**

tk_dml_by_id.py 和 tk_chunk_update.py 在执行过程中会生产检查点，检查点表示在这之前已经处理完毕的rowid，
无论是异常退出还是主动终止，再次运行时如果检查点文件存在则会跳过已处理的rowid.
tk_dml_by_time.py 则 **不会** 产生检查点，如果任务失败建议查看执行日志手动设置一个start_time。