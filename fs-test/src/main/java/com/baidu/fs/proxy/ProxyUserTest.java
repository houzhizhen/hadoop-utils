package com.baidu.fs.proxy;

import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

public class ProxyUserTest {

    public static void main(String[] args) throws IOException, InterruptedException {
        UserGroupInformation superUser = UserGroupInformation.getCurrentUser();
        //创建proxyUser用户
        UserGroupInformation proxyUgi = UserGroupInformation.createProxyUser("proxyUser", superUser);
        // 使用proxyUser用户访问集群
        proxyUgi.doAs(new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
                // 使用proxy用户访问hdfs
                UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
                System.out.println(ugi.getShortUserName());

                return null;
            }
        });
    }
}
