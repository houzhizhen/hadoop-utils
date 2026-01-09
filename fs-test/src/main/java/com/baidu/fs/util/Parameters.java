package com.baidu.fs.util;

import java.util.HashMap;
import java.util.Map;

public class Parameters {
private Map<String, String> paraMap = new HashMap<>();

private Parameters(Map<String, String> paraMap) {
    this.paraMap = paraMap;
}

public static Parameters get(String[] args) {
    Map<String, String> paraMap = new HashMap<>();
    for (int i = 0; i < args.length; i++) {
        if (args[i].startsWith("--")) {
            String parameterName = args[i].substring(2);
            String parameterValue = "";
            // next is not parameter name
            if (i < args.length - 1 && !args[i+1].startsWith("--")) {
                parameterValue = args[i+1];
                i++;
            }
            paraMap.put(parameterName, parameterValue);
        }
    }
    return new Parameters(paraMap);
}
public Parameters(String[] args) {
    for (int i = 0; i < args.length; i++) {
        if (args[i].startsWith("--")) {
            String parameterName = args[i].substring(2);
            String parameterValue = "";
            // next is not parameter name
            if (i < args.length - 1 && !args[i+1].startsWith("--")) {
                parameterValue = args[i+1];
                i++;
            }
            paraMap.put(parameterName, parameterValue);
        }
    }
}

public String get(String key) {
    return paraMap.get(key);
}

public String[] getArray(String key) {
    String value = paraMap.get(key);
    if (value == null) {
        throw new RuntimeException("Not has parameter " + key);
    }
    return value.split(",");
}

public String[] getArray(String key, String defValue) {
    String value = paraMap.get(key);
    if (value == null) {
        return defValue.split(",");
    }
    return value.split(",");
}

public int[] getIntArray(String key) {
    String[] strings = getArray(key);
    int[] array = new int[strings.length];
    for (int i=0; i < strings.length; i++) {
        array[i] = Integer.parseInt(strings[i]);
    }
    return array;
}
public int[] getIntArray(String key, String defaultValue) {
    String[] strings = getArray(key, defaultValue);
    int[] array = new int[strings.length];
    for (int i=0; i < strings.length; i++) {
        array[i] = Integer.parseInt(strings[i]);
    }
    return array;
}

public String get(String key, String defaultValue) {
    String value = paraMap.get(key);
    return value == null ?  defaultValue : value;
}

public int getInt(String key, int defaultValue) {
    String value = paraMap.get(key);
    return value == null ? defaultValue : Integer.parseInt(value);
}

public int getInt(String key) {
    String value = paraMap.get(key);
    if (value == null) {
        throw new RuntimeException("Not has parameter " + key);
    }
    return Integer.parseInt(value);
}

public boolean has(String key) {
    return paraMap.keySet().contains(key);
}

public static void main(String[] args) {
    Parameters parameters = new Parameters(
        new String[]{"--key1",  "value1",
            "--key2", "value2",
            "--key3",
            "--key4", "value4"});
    System.out.println(parameters.paraMap);

    parameters = new Parameters(
        new String[]{"--key1",  "value1",
            "--key2", "value2"});
    System.out.println(parameters.paraMap);
    parameters = new Parameters(
        new String[]{"--key1",  "value1",
            "--key2"});
    System.out.println(parameters.paraMap);
}


public long getLong(String key) {
    String value = paraMap.get(key);
    if (value == null) {
        throw new RuntimeException("Not has parameter " + key);
    }
    return Long.parseLong(value);
}
}
