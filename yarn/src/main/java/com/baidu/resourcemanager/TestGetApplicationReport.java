package com.baidu.resourcemanager;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class TestGetApplicationReport {
    public static void main(String[] args) {
        System.out.println("TestGetApplicationReport");
        long clusterTimestamp = 1L;
        int id = 2;
        if (args.length == 2) {
            clusterTimestamp = Long.parseLong(args[0]);
            id = Integer.parseInt(args[1]);
        }
        System.out.println("clusterTimestamp: " + clusterTimestamp);
        System.out.println("id: " + id);
        final ApplicationId applicationId = ApplicationId.newInstance(clusterTimestamp, id);
        ExecutorService es = Executors.newFixedThreadPool(10);
        es.submit(
                () -> {
                    YarnConfiguration conf = new YarnConfiguration();
                    YarnClient client = YarnClient.createYarnClient();
                    client.init(conf);
                    client.start();
                    List<QueueInfo> queueInfoList = null;
                    try {
                        queueInfoList = client.getAllQueues();
                        System.out.println("get Queues size " + queueInfoList.size());
                        for (QueueInfo queueInfo : queueInfoList) {
                            System.out.println(queueInfo.getQueueName());
                        }
                        client.getApplicationReport(applicationId);
                        System.out.println("client stopped");
                    } catch (YarnException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    } finally {
                        client.stop();
                    }
                }
        );

        try {
            TimeUnit.SECONDS.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        es.shutdownNow();
    }
}
