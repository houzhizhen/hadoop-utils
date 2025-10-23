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
## 3. MakeDirs

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

```bash
hadoop jar ./fs-test-1.8.10.jar com.baidu.fs.parallel.MakeDirAndFile baseDir iteratorTime threadNum fileNumPerDir fileLength level0DirNum, ... , levelNFileNum
## eg, 以下命令 以 /tmp/test 作为基准目录，迭代2次，每次的迭代目录为 /tmp/test/d[0-1]。每次迭代使用 3 个线程，每个线程以 /tmp/test/d[0-1]/d[0-2] 作为基准目录。4 每个目录文件的个数, 5是文件的长度;6，7 是每个线程创建的目录.
总共创建 2 * 3 * 4 * 6 * 7  = 1008 个文件。2 * 3 * 6 * 7  = 1008 个文件。

hadoop jar ./fs-test-1.8.10.jar com.baidu.fs.parallel.MakeDirAndFile /tmp/test 2 3 4 5 6 7
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