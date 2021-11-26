import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;

public class ConnectWithKerberosTest {
    public static final Log LOG = LogFactory.getLog(ConnectWithKerberosTest.class);

    public ConnectWithKerberosTest() {

    }

    public static void connect(Configuration conf, String principal, String keytabLocation) throws IOException {
        FileSystem fs = null;
        try {
            UserGroupInformation.loginUserFromKeytab(principal, keytabLocation);
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (fs != null) {
                fs.close();
            }
        }
    }


    public static void main(String[] args) throws IOException {
        if(args.length != 3) {
            printUsage();
            System.exit(1);
        }

        Configuration conf = new HdfsConfiguration();
        conf.set("fs.hdfs.impl.disable.cache", "true");
        String principal = System.getProperty("kerberosPrincipal", args[0]);
        String keytabLocation = System.getProperty("kerberosKeytab",args[1]);
        long connectTimes = Integer.parseInt(args[2]);
        LOG.info(String.format("principal = %s, keytabLocation = %s, connectTimes=%s", principal, keytabLocation, connectTimes));
        // kinit with principal and keytab
        UserGroupInformation.setConfiguration(conf);

        long minTime = Long.MAX_VALUE;
        long maxTime = Long.MIN_VALUE;
        long beginTime = System.currentTimeMillis();
        for (int i = 0; i < connectTimes; i++) {
            long startTime = System.currentTimeMillis();
            connect(conf, principal, keytabLocation);
            long duration = System.currentTimeMillis() - startTime;
            if (duration < minTime) {
                minTime = duration;
            }
            if (duration > maxTime) {
                maxTime = duration;
            }
        }

        long duration = System.currentTimeMillis() - beginTime;
        LOG.info(String.format("minTime = %s, maxTime = %s, totalTime = %s", minTime, maxTime, duration));
    }

    private static void printUsage() {
        LOG.info("Usage: ConnectWithKerberosTest principal keytab-file connectTimes");
    }
}
