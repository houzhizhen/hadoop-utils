package com.baidu.credentials;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

public class CredentialsTest {

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        conf.set("hadoop.security.credential.provider.path", "jceks://file/etc/hive/conf/hive.jceks");
        conf.reloadConfiguration();
        char[] pass = conf.getPassword("javax.jdo.option.ConnectionPassword");
        if (pass != null) {
            System.out.println(new String(pass));
        } else {
            System.out.println("pass == null");
        }
    }
}
