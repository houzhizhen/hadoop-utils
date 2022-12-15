package com.baidu.fs.compare;

import org.junit.Test;

import java.io.IOException;

public class TestDeleteFilesNotExistInFirstDir {

    @Test
    public void test() throws IOException {
        DeleteFilesNotExistInFirstDir.main(
                new String[]{"file:///Users/houzhizhen/git/baidu/bce-bmr/hive-1.2/hive-1.2",
                             "file:///Users/houzhizhen/git/baidu/bce-bmr/hive"});
    }
}
