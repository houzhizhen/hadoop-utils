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

    private static final String THREAD_NUM = "--thread-num";
    private static final String WAIT_TIME = "--wait-time";
    private static final String APP_ID = "--app-id";
    private static final String CLUSTER_TIMESTAMP = "--cluster-timestamp";

    public static void main(String[] args) throws IOException, YarnException {
        System.out.println("TestGetApplicationReport");
        int threadNum = 10;
        int waitTime = 10;
        int id = 1;
        long clusterTimestamp = 1L;
        for(int i = 0; i < args.length; ++i) {
            if(args[i].equals(THREAD_NUM)) {

                threadNum = Integer.parseInt(args[++i]);
            }
            if(args[i].equals(WAIT_TIME)) {
                waitTime = Integer.parseInt(args[++i]);
            }
            if(args[i].equals(APP_ID)) {
                id = Integer.parseInt(args[++i]);
            }
            if(args[i].equals(CLUSTER_TIMESTAMP)) {
                clusterTimestamp = Long.parseLong(args[++i]);
            }
        }
        System.out.println("clusterTimestamp: " + clusterTimestamp);
        System.out.println("id: " + id);
        System.out.println("threadNum: " + threadNum);
        System.out.println("waitTime: " + waitTime);
        System.out.println("appId: " + ApplicationId.newInstance(clusterTimestamp, id));

        final ApplicationId applicationId = ApplicationId.newInstance(clusterTimestamp, id);
        ExecutorService es = Executors.newFixedThreadPool(threadNum);
        YarnConfiguration conf = new YarnConfiguration();
        YarnClient client = YarnClient.createYarnClient();
        client.init(conf);
        client.start();
        List<QueueInfo> queueInfoList = client.getAllQueues();
        System.out.println("get Queues size " + queueInfoList.size());
        for (QueueInfo queueInfo : queueInfoList) {
            System.out.println(queueInfo.getQueueName());
        }
        for (int i = 0; i < threadNum; ++i) {
            es.submit(
                    () -> {
                        try {
                            client.getApplicationReport(applicationId);
                            System.out.println("get report");
                        } catch (YarnException e) {
                            e.printStackTrace();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
            );
        }

        es.shutdownNow();
        try {
            TimeUnit.SECONDS.sleep(waitTime);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        client.stop();
    }
}
