package com.baidu.fs.raw;

import java.io.IOException;

public interface Command {

    void exec(String[] args) throws IOException;
}
