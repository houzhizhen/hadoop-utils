# hadoop-utils
Various hadoop utils

## 1. MkDirs
```bash
hadoop jar ./fs-test-1.8.10.jar com.baidu.fs.raw.RawFs mkdirs /user/hive
```

## 2. PutAndListAndInterrupt

```bash
hadoop jar ./fs-test-1.8.10.jar basePath threadNum subdirNum fileNum iterationTimes
```
示例
```bash
hadoop jar ./fs-test-1.8.10.jar com.baidu.fs.parallel.PutAndListAndInterrupt hdfs://master-4b115ab:8020/base 100 1 100 100
```

参数说明
```bash
basePath：路径根地址
threadNum：线程数量，每个线程会创建一个 ${basePath}/thread_${threadId} 目录作为子线程的根目录。
subdirNum：每个线程子目录的数量
fileNum：每个子目录下文件的数量。
iterationTimes：读取的迭代次数
```

## 3. PutAndList
多线程 put 和 list
```bash
hadoop jar ./fs-test-1.8.10.jar com.baidu.fs.parallel.PutAndList basePath threadNum subdirNum fileNum iterationTimes
```
示例
```bash
hadoop jar ./fs-test-1.8.10.jar com.baidu.fs.parallel.PutAndList bos://bmr-rd-wh/houzhizhen/test 1 2000 1 1
```
参数说明
```bash
basePath：路径根地址
threadNum：线程数量，每个线程会创建一个 ${basePath}/thread_${threadId} 目录作为子线程的根目录。
subdirNum：每个线程子目录的数量
fileNum：每个子目录下文件的数量。
iterationTimes：读取的迭代次数
```
## 3. LongTimeDirTest
长时间 Dir 测试，每轮创建100个目录，然后删除这100个目录。
每个目录操作 sleep 1 分钟，那么创建 100 个目录大约 sleep 100 分钟。
```bash
hadoop jar ./fs-test-1.8.10.jar com.baidu.fs.test.LongTimeDirTest basePath subdirNum
```
示例
```bash
hadoop jar ./fs-test-1.8.10.jar com.baidu.fs.test.LongTimeDirTest bos://bmr-rd-wh/houzhizhen/test  2000 
```
参数说明
```bash
basePath：路径根地址
subdirNum：根地址下子目录的数量
```

## 4. TestGetApplicationReport
可以有两个参数，第一个参数是 clusterTimestamp, 类型为 long。 第2个参数是 applicationId, 类型为 int。默认 clusterTimestamp=1, applicationId=2.
```bash
hadoop jar yarn-1.8.10.jar com.baidu.resourcemanager.TestGetApplicationReport
```

## 4. Bos Conflict Test
使用 Bos filesystem 作为 resourcemanager 的 store 的时候，抛出异常。
path remainCount loopCount.


```bash
hadoop jar ./fs-test-1.8.10.jar com.baidu.fs.test.BosConflictTest path remainCount loopCount
```

## MakeDirAndFile
多线程执行创建目录和文件


以下命令 以 /tmp/test 作为基准目录，迭代2次，每次的迭代目录为 /tmp/test/d[0-1]。每次迭代使用 3 个线程，每个线程以 /tmp/test/d[0-1]/d[0-2] 作为基准目录。4 每个目录文件的个数, 5是文件的长度;6，7 是每个线程创建的目录.
总共创建 2 * 3 * 4 * 6 * 7  = 1008 个文件。2 * 3 * 6 * 7  = 1008 个文件。
```bash
hadoop jar ./fs-test-1.8.10.jar com.baidu.fs.parallel.MakeDirAndFile --base-path /tmp/test --iterator-time 2 --iterator-start-index 0 --thread-num 10 --file-per-dir 100 --file-length  0 --dirs-per-level 100,100

```
多线程写文件测试。线程数：128，每个线程写10G文件。
```bash
hadoop jar ./fs-test-1.8.10.jar com.baidu.fs.parallel.MakeDirAndFile  --base-path /tmp/test --iterator-time 1 --iterator-start-index 0 --thread-num 128 --file-per-dir 2 --file-length  10737418240 --dirs-per-level 1
```
## FilePerformanceTest


```bash
export HADOOP_CLIENT_OPTS="-Xmx5g -Xms5g -Xmn1g"
nohup hadoop jar ./fs-test-1.8.10.jar com.baidu.fs.test.FilePerformanceTest \
  --base-dr /home/disk2/hdfs/data/FilePerformanceTest \
  --dir-num-per-level 256,256 \
  --iterator-time 75 > FilePerformanceTest.log 2>&1 &



```
## TestLightWeightResizableGSet
```bash
export HADOOP_CLIENT_OPTS="-Xmx500g -Xms500g -Xmn5g"
hadoop jar fs-test-1.8.10.jar com.baidu.fs.test.TestLightWeightResizableGSet 100000000

```

## FindTopDirectories
找到一个目录下及其所有子目录中，文件对象最多的目录。

```bash
hadoop jar ./fs-test-1.8.10.jar com.baidu.fs.parallel.FindTopDirectories path topN
```
如
```bash
hadoop jar ./fs-test-1.8.10.jar com.baidu.fs.parallel.FindTopDirectories --root-dir file:///Users/houzhizhen/git/baidu/bce-bmr/hadoop --top-num 10
```

## TestManyDirsInDirectory
创建在指定目录下创建 指定个子目录

```bash
hadoop jar ./fs-test-1.8.10.jar com.baidu.fs.test.CreateSubDirsInDirectory path number-of-subdirectories
```
hadoop jar ./fs-test-1.8.10.jar com.baidu.fs.test.CreateSubDirsInDirectory file:///Users/houzhizhen/git/hadoop-utils/fs-test/target 10 
如创建 1000000 个子目录， 每创建 10000 个打印下时间。
```bash
hadoop jar ./fs-test-1.8.10.jar com.baidu.fs.test.CreateSubDirsInDirectory hdfs://bmr-cluster/user/hive/warehouse/test/ 1000000 10000
```

## DistributedReadTest
```bash
hadoop jar ./fs-test-1.8.10.jar com.baidu.fs.distributed.DistributedReadTest -maps 1 -baseDir /tmp/ -parameters '/data/fs-test,1,100,100'
```

## DeleteDirAndFileBaseOnModifyTime

```bash
hadoop jar ./fs-test-1.8.10.jar com.baidu.fs.util.DeleteDirAndFileBaseOnModifyTime --dir-to-delete 'file:///Users/houzhizhen/git/hadoop-utils/fs-test/target' --hours 1
```

## EcReadWriteTest

```bash
hadoop jar ./fs-test-1.8.10.jar com.baidu.fs.test.ec.EcReadWriteTest --paths "/Users/houzhizhen/ec/a/a,/Users/houzhizhen/ec/b/b" --file-length 1073741824
```


## ParallelSlowReader

```bash
hadoop jar ./fs-test-1.8.10.jar com.baidu.fs.parallel.ParallelSlowReader --parallel 15000 --path /benchmarks/TestDFSIO/io_data --bytesPerSecond 4096 --createThreadPerSecond 256
```
## ParallelTailTest
```bash
exort HADOOP_HEAPSIZE=20g
hadoop jar fs-test-1.8.10.jar com.baidu.fs.parallel.ParallelTailTest --path bos://bmr-rd-wh/test/test-tail --parallel 100 --printInterval 1000 --seekNum 100
```
## ParallelWriteTest
```bash
exort HADOOP_HEAPSIZE=2g
hadoop jar fs-test-1.8.10.jar com.baidu.fs.parallel.ParallelWriteTest --basePath hdfs://localhost:8020/test/parallel-write --parallel 10 --filesize 10G --fileNumPerThread 10
```

参数说明：
- `basePath`：写入根路径，每个线程会创建 `basePath/sub<threadNum>/file-<i>`
- `parallel`：并发线程数
- `filesize`：单个文件大小，支持带二进制前缀的写法（大小写不敏感，1024 进制），例如 `10G`、`512M`、`4K`、`10240`（纯数字视为字节数）。默认 `4096`
- `bufferSize`：单次 `write` 的缓冲区大小，同样支持 `10G` / `512M` 之类的写法。默认 `1M`。小于 `filesize` 时按 buffer 循环写；大于 `filesize` 时自动截断到 `filesize`，避免浪费内存
- `fileNumPerThread`：每个线程写入的文件数，默认 100
- `deleteAfterWrite`：写完是否删除文件，默认 false

## ParallelReadTest
```bash
exort HADOOP_HEAPSIZE=2g
hadoop jar fs-test-1.8.10.jar com.baidu.fs.parallel.ParallelReadTest --basePath hdfs://localhost:8020/test/parallel-read --parallel 10 --filesize 10G --fileNumPerThread 10
```

参数说明：
- `basePath`：数据根路径，每个线程读取 `basePath/file-<threadNum>`（若不存在会先写出，长度为 `filesize`）
- `parallel`：并发线程数
- `filesize`：单个文件大小，写法同 `ParallelWriteTest`，默认 `4096`
- `bufferSize`：单次 `read` / `write` 的缓冲区大小，默认 `1M`
- `fileNumPerThread`：每个线程读取的迭代次数，默认 100

## ParallelReadWriteByPercent
```bash
exort HADOOP_HEAPSIZE=2g
hadoop jar fs-test-1.8.10.jar com.baidu.fs.parallel.ParallelReadWriteByPercent --basePath hdfs://localhost:8020/test/parallel --parallel 10 --readPercent 0 --filesize 10G --fileNumPerThread 10
```

```bash
exort HADOOP_HEAPSIZE=2g
hadoop jar fs-test-1.8.10.jar com.baidu.fs.parallel.ParallelReadWriteByPercent --basePath file:///ssd1/hdfs/data/temp --parallel 10 --readPercent 0 --filesize 10G --fileNumPerThread 10
```
除 `readPercent`（读线程占比，`[0,1]` 之间）外，`filesize`、`bufferSize`、`fileNumPerThread` 语义同 `ParallelWriteTest` / `ParallelReadTest`。

## DistributedReadWriteByPercent

```java
 hadoop jar fs-test-1.8.10.jar com.baidu.fs.distributed.DistributedReadWriteByPercent \
 --maps 10 \
 --sleepTime 1000 \
 --baseDir hdfs://localhost:8020/tmp/distributed_test \
 --parameters "--parallel 2 --readPercent 0 --filesize 10G --fileNumPerThread 10"
```
`--parameters` 里的参数会透传给每个 mapper 内的 `ParallelReadWriteByPercent`，`filesize` / `bufferSize` 支持 `10G` / `512M` 这类写法。

## RepeatCreateSameFile

重复覆盖写入同一个文件的测试工具

```bash
hadoop jar fs-test-1.8.10.jar com.baidu.fs.raw.RepeatCreateSameFile \
  --hdfs-path hdfs://localhost:8020/tmp/test.txt \
  --size 10KB \
  --time 20m 
```

参数说明：
- `hdfs-path`: HDFS文件路径
- `size`: 文件大小（支持B, KB, MB, GB）
- `time`: 运行时间（支持ms, s, m, h）
- `buffer-size`: 缓冲区大小（字节），可选，默认4096

测试完成后会输出统计信息，包括：
- 总运行时间和写入次数
- 最长单次写入时间及其发生时间
- 平均吞吐量和每秒写入次数

## RepeatMkRmSameDir

重复对同一个目录执行 mkdir 和 delete 的测试工具

```bash
hadoop jar fs-test-1.8.10.jar com.baidu.fs.raw.RepeatMkRmSameDir \
  --hdfs-path hdfs://xafj-sys-rpm58y98bhi.xafj.baidu.com:8020/tmp/test-dir \
  --time 20m
```

参数说明：
- `hdfs-path`: HDFS目录路径
- `time`: 运行时间（支持ms, s, m, h）

测试完成后会输出统计信息，包括：
- mkdir+delete 总次数
- 最长单次迭代时间

## RepeatGetBlockInfo

用于测试 NameNode 清除 dead datanode 时停顿的时间间隔。

```bash
hadoop jar fs-test-1.8.10.jar com.baidu.fs.raw.RepeatGetBlockInfo \
  --hdfs-path hdfs://localhost:8020/tmp/test.txt \
  --size 10KB \
  --time 20m 
```

参数说明：
- `hdfs-path`: HDFS文件路径
- `size`: 文件大小（支持B, KB, MB, GB）
- `time`: 运行时间（支持ms, s, m, h）
- `buffer-size`: 缓冲区大小（字节），可选，默认4096

测试完成后会输出统计信息，包括：
- 总运行时间和写入次数
- 最长单次写入时间及其发生时间
- 平均吞吐量和每秒写入次数

## ParallelTestDirsExist
单机多线程按序执行 `fs.exists()` 的测试工具。启动 `parallel` 个线程，每个线程内**按顺序**探测 `fileNumPerThread` 个路径是否存在。

```bash
export HADOOP_HEAPSIZE=2g
hadoop jar fs-test-1.8.10.jar com.baidu.fs.parallel.ParallelTestDirsExist \
  --basePath hdfs://localhost:8020/test/parallel-write \
  --parallel 10 \
  --fileNumPerThread 10 \
  --useSubDir true
```

参数说明：
- `basePath`：路径根地址
- `parallel`：并发线程数
- `fileNumPerThread`：每个线程按序探测的文件数，默认 100
- `useSubDir`：是否每线程一个子目录，默认 true
    - `true`：探测 `basePath/sub<threadNum>/file-<i>`，与 `ParallelWriteTest` 的写入目录结构对齐
    - `false`：全部平铺，探测 `basePath/file-<threadNum>-<i>`
- `subDirPrefix`：子目录前缀，默认 `sub`
- `filePrefix`：文件名前缀，默认 `file-`

执行完成后打印 `existCount` 和 `notExistCount`。`exists()` 返回 false 不视为错误，只累计到 `notExistCount`；只有抛 `IOException` 才让所有线程尽快退出。

## DistributedTestDirsExist
基于 MapReduce 的分布式版本，起 `maps` 个 mapper-only 任务，每个 mapper 内部调用 `ParallelTestDirsExist`。所有 mapper 通过 `sleepTime` 定义的 barrier 尽量同时起跑。

先用 `ParallelWriteTest` 或 `DistributedReadWriteByPercent` 之类工具预置数据，再跑 exists 探测（存在率场景）：
```bash
# Step 1: 预置数据（每 mapper 独立目录 map-<taskID>/sub<i>/file-<j>）
hadoop jar fs-test-1.8.10.jar com.baidu.fs.distributed.DistributedReadWriteByPercent \
  --maps 10 \
  --sleepTime 60000 \
  --baseDir bos://bmr-bj-namespace/dirs_exist_test \
  --parameters "--parallel 100 --readPercent 0 --filesize 1024 --fileNumPerThread 100"

# Step 2: 对同样的路径按序 exists
hadoop jar fs-test-1.8.10.jar com.baidu.fs.distributed.DistributedTestDirsExist \
  --maps 10 \
  --sleepTime 60000 \
  --baseDir bos://bmr-bj-namespace/dirs_exist_test \
  --parameters "--parallel 100 --fileNumPerThread 100 --useSubDir true"
```
> 注意：`DistributedReadWriteByPercent` 的写数据落在 `baseDir/map-<taskID>/write/sub<i>/file-<j>`，而本工具默认落在 `baseDir/map-<taskID>/sub<i>/file-<j>`，路径不完全一致；如需精确对齐，请把两个工具的写路径统一，或用下一个"不存在率场景"的方式。

对空目录跑 exists（不存在率场景）：
```bash
hadoop jar fs-test-1.8.10.jar com.baidu.fs.distributed.DistributedTestDirsExist \
  --maps 10 \
  --sleepTime 60000 \
  --baseDir bos://bmr-bj-namespace/dirs_exist_test \
  --parameters "--parallel 100 --fileNumPerThread 100"
# NotExistCount ≈ maps * parallel * fileNumPerThread
```

顶层参数（与 `DistributedReadWriteByPercent` 一致）：
- `maps`：mapper 个数，约等于参与压测的节点数
- `sleepTime`：barrier 目标启动时刻相对提交时刻的毫秒偏移，生产建议 30000–60000
- `baseDir`：MR 控制目录 + 每 mapper 默认 basePath 的父目录
- `parameters`：透传给 mapper 内 `ParallelTestDirsExist` 的参数字符串

`parameters` 内可用参数除 `ParallelTestDirsExist` 的全部参数外，还支持：
- `overrideBasePath`：mapper 是否把 `basePath` 覆写为 `baseDir/map-<taskID>`，默认 `true`。设为 `false` 时使用 `parameters` 里显式传入的 `basePath`，用于对接已有目录

执行完成后聚合以下 counter 并计算吞吐：
- `TimeUSED`：所有 mapper 耗时之和（毫秒）
- `ExistCount` / `NotExistCount`：全局存在/不存在的探测数

## ParallelMkDirs
单机多线程按序执行 `fs.mkdirs()` 的压测工具。启动 `parallel` 个线程，每个线程内**按顺序**创建 `fileNumPerThread` 个目录。

```bash
export HADOOP_HEAPSIZE=2g
hadoop jar fs-test-1.8.10.jar com.baidu.fs.parallel.ParallelMkDirs \
  --basePath hdfs://localhost:8020/test/parallel-mkdirs \
  --parallel 10 \
  --fileNumPerThread 100 \
  --useSubDir true
```

参数说明：
- `basePath`：路径根地址，会尝试 `mkdirs(basePath)` 兜底；**不会**主动清理已有子树，避免误删
- `parallel`：并发线程数
- `fileNumPerThread`：每个线程按序创建的目录数，默认 100
- `useSubDir`：是否每线程一个子目录，默认 true
    - `true`：创建 `basePath/sub<threadNum>/dir-<i>`，与 `ParallelWriteTest` / `ParallelTestDirsExist` 目录约定一致
    - `false`：全部平铺，创建 `basePath/dir-<threadNum>-<i>`，用来测单目录锁竞争
- `subDirPrefix`：子目录前缀，默认 `sub`
- `dirPrefix`：目录名前缀，默认 `dir-`

执行完成后打印 `mkDirsCount`。任一线程抛 `IOException` 会让所有线程尽快退出，并作为最终失败上抛（区别于 `ParallelTestDirsExist`，`mkdirs` 场景没有"合法的 false"结果）。

## DistributedMkDirs
基于 MapReduce 的分布式版本，起 `maps` 个 mapper-only 任务，每个 mapper 内部调用 `ParallelMkDirs`。所有 mapper 通过 `sleepTime` 定义的 barrier 尽量同时起跑。

```bash
hadoop jar fs-test-1.8.10.jar com.baidu.fs.distributed.DistributedMkDirs \
  --maps 10 \
  --sleepTime 60000 \
  --baseDir bos://bmr-bj-namespace/distributed_mkdirs \
  --parameters "--parallel 100 --fileNumPerThread 100 --useSubDir true"
# MkDirsCount ≈ maps * parallel * fileNumPerThread
```

顶层参数（与 `DistributedReadWriteByPercent` / `DistributedTestDirsExist` 一致）：
- `maps`：mapper 个数，约等于参与压测的节点数
- `sleepTime`：barrier 目标启动时刻相对提交时刻的毫秒偏移，生产建议 30000–60000
- `baseDir`：MR 控制目录 + 每 mapper 默认 basePath 的父目录
- `parameters`：透传给 mapper 内 `ParallelMkDirs` 的参数字符串

`parameters` 内可用参数除 `ParallelMkDirs` 的全部参数外，还支持：
- `overrideBasePath`：mapper 是否把 `basePath` 覆写为 `baseDir/map-<taskID>`，默认 `true`。设为 `false` 时使用 `parameters` 里显式传入的 `basePath`，用于对接已有目录

执行完成后聚合以下 counter 并计算吞吐：
- `TimeUSED`：所有 mapper 耗时之和（毫秒）
- `MkDirsCount`：全局成功 mkdir 的总次数

## LogBlocksPlugin
配置 namenode 的 plugin，用于测试清除 dead 的 datanode 后，数据块的 triples 不等于 3的数量，和 triples 数组的元素为空的数量。每 10 秒钟打印一次。
hdfs-site.xml
```xml
 <property>
      <name>dfs.namenode.plugins</name>
      <value>org.apache.hadoop.hdfs.server.blockmanagement.LogBlocksPlugin</value>
  </property>
```