package com.baidu.fs.util;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Deduplicate {
    private final Path rootDir;
    Map<String, String> fileMap = new HashMap<>();
    Map<String, Long> sizeMap = new HashMap<>();
    public Deduplicate(String rootDir) {
        this.rootDir = new Path(rootDir);
    }

    private static Options createOptions() {
        Options result = new Options();

        result.addOption(OptionBuilder
                .withLongOpt("directory")
                .withDescription("Specify a directory to deduplicate")
                .hasArg()
                .create('d'));

        return result;
    }

    public static void main(String[] args) throws Exception {
        Options opts = createOptions();
        CommandLine cli = new GnuParser().parse(opts, args);

        String rootDir;
        if (cli.hasOption("d")) {
            rootDir = cli.getOptionValue("d");
        } else {
            throw new RuntimeException("You must specify directory to deduplicate");
        }

        Deduplicate task = new Deduplicate(rootDir);
        task.execute();
    }

    private void execute() throws IOException {
        FileSystem fs = rootDir.getFileSystem(new Configuration());
        if (! fs.exists(rootDir)) {
            throw new IOException("Dir " + rootDir.toString() + " does not exist");
        }
        if (fs.isDirectory(rootDir)) {
            recursive(fs, rootDir, 0);
        } else {
            throw new IOException("Dir " + rootDir.toString() + " is a file");
        }
    }

    private void recursive(FileSystem fs, Path dir, int level) throws IOException {
        FileStatus[] statuses = fs.listStatus(dir);
        for (FileStatus status : statuses) {
            if (status.isFile()) {
                String existPath = fileMap.get(status.getPath().getName());
                if (existPath != null && sizeMap.get(status.getPath().getName()) == status.getLen()) {
                    String relativePath = status.getPath().toString().substring(rootDir.toString().length());
                    System.out.println("rm -rf " + relativePath);
                    System.out.println("ln -s " + existPath + " " + relativePath);
                    // fs.delete(status.getPath());
                    // fs.createSymlink(new Path(existPath), status.getPath(), false);

                } else {
                    String relativePath = status.getPath().toString().substring(rootDir.toString().length());
                    fileMap.put(status.getPath().getName(), relativePath);
                    sizeMap.put(status.getPath().getName(), status.getLen());
                }
            } else {
                recursive(fs, status.getPath(), level + 1);
            }
        }
    }
}
