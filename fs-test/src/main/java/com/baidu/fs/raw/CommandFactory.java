package com.baidu.fs.raw;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

import java.util.HashMap;
import java.util.Map;

public class CommandFactory {

    private static Map<String, Class> commandMap = new HashMap<>();

    static {
        commandMap.put(MV.NAME, MV.class);
        commandMap.put(MakeDirs.NAME, MakeDirs.class);
    }

    public static Command getCommand(Configuration conf, String name) {
        Class<Command> commandClass = commandMap.get(name);
        if (commandClass == null) {
            throw new RuntimeException("Can't find command " + name);
        }
        return ReflectionUtils.newInstance(commandClass, conf);
    }
}
