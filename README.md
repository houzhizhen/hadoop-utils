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

