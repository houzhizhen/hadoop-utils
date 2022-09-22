package org.apache.hadoop.yarn.server.timeline;

import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.util.List;

public class LeveldbTimeLineTest {

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            printUsage();
            System.exit(1);
        }
        String dbPath = args[0];
        LeveldbTimelineStore leveldbTimelineStore = new LeveldbTimelineStore();
        YarnConfiguration conf = new YarnConfiguration();
        conf.set(YarnConfiguration.TIMELINE_SERVICE_LEVELDB_PATH, dbPath);
        // Never clean up
        conf.set(YarnConfiguration.TIMELINE_SERVICE_TTL_MS, Long.MAX_VALUE + "");
        leveldbTimelineStore.init(conf);
        List<String> list = leveldbTimelineStore.getEntityTypes();
        leveldbTimelineStore.discardOldEntities(System.currentTimeMillis());
        leveldbTimelineStore.serviceStop();
        System.out.println("Begin print EntityTypes:");
        for (String type : list) {
            System.out.println(type);
        }
        System.out.println("End print EntityTypes:");

//        byte[] entityEntryPrefix = "e".getBytes(Charset.forName("UTF-8"));
//        Set<String> set = new HashSet<>();
//        LevelDbUtil.filter(dbPath, entityEntryPrefix, (Map.Entry<byte[], byte[]> entry) -> {
//
//        });
    }

    private static void printUsage() {
        System.out.println("Usage:");
        System.out.println("hadoop jar yarn-1.8.10.jar org.apache.hadoop.yarn.server.timeline.LeveldbTimeLineTest dbPath");
    }
}
