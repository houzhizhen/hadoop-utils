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

public class DeleteDirAndFileBaseOnModifyTime {

    private final boolean dryRun;
    private final Path rootDir;
    private final long timeLimitInMillions;

    public DeleteDirAndFileBaseOnModifyTime(boolean dryRun, String rootDir, int hours) {
        this.dryRun = dryRun;
        this.rootDir = new Path(rootDir);
        this.timeLimitInMillions = System.currentTimeMillis() - hours * 60 * 60 * 1000L;
    }

    public void execute() throws IOException {
        try (FileSystem fs = FileSystem.get(rootDir.toUri(), new Configuration())) {
            if (! fs.exists(rootDir)) {
                throw new RuntimeException(rootDir + " does not exist");
            }
            FileStatus status = fs.getFileStatus(rootDir);
            if (status.isFile()) {
                throw new RuntimeException(rootDir + " is file");
            }
            recursiveDelete(fs, fs.getFileStatus(rootDir));
        }
    }

    private boolean recursiveDelete(FileSystem fs, FileStatus dir) {
        try {
            boolean allSubDirsAndFilesReachTime = true;
            FileStatus[] files = fs.listStatus(dir.getPath());

            for (FileStatus file : files) {
                if (file.isFile()) {
                    if (!reachTime(file)) {
                        System.out.println("File " + file.getPath() +" does not reach time limit");
                        allSubDirsAndFilesReachTime = false;
                        continue;
                    }
                    System.out.println("Deleting file " + file.getPath());
                    if (!dryRun) {
                        fs.delete(file.getPath(), true);
                    }
                } else { // is directory
                    allSubDirsAndFilesReachTime = recursiveDelete(fs, file) && allSubDirsAndFilesReachTime;
                }
            }
            allSubDirsAndFilesReachTime = allSubDirsAndFilesReachTime && reachTime(dir);
            if (allSubDirsAndFilesReachTime) {
                System.out.println("Deleting dir " + dir.getPath());
                if (!dryRun) {
                    fs.delete(dir.getPath(), true);
                }
            } else {
                System.out.println("Directory " + dir.getPath() +" does not reach time limit or has children");
            }
            return allSubDirsAndFilesReachTime;
        } catch (Exception e) {
            return false;
        }
    }

    private boolean reachTime(FileStatus  status) {
        return status.getModificationTime() < timeLimitInMillions;
    }

    private static Options createOptions() {
        Options result = new Options();

        // add -r and --dry-run to generate list only
        result.addOption(OptionBuilder
                                 .withLongOpt("dry-run")
                                 .withDescription("Print the expired directories or files on console")
                                 .create('r'));

        result.addOption(OptionBuilder
                                 .withLongOpt("dir-to-delete")
                                 .withDescription("Specify a non-default location of the scratch dir")
                                 .hasArg()
                                 .create('s'));

        result.addOption(OptionBuilder
                                 .withLongOpt("days")
                                 .withDescription("only delete dirs created days before")
                                 .hasArg()
                                 .create('d'));

        result.addOption(OptionBuilder
            .withLongOpt("hours")
            .withDescription("only delete dirs created hours before")
            .hasArg()
            .create('h'));

        return result;
    }

    public static void main(String[] args) throws Exception {
        Options opts = createOptions();
        CommandLine cli = new GnuParser().parse(opts, args);

        boolean dryRun = cli.hasOption("r");

        String rootDir;
        if (cli.hasOption("s")) {
            rootDir = cli.getOptionValue("s");
        } else {
            throw new RuntimeException("You must specify dir-to-delete");
        }

        int hours;
        if (cli.hasOption("h")) {
            hours = Integer.parseInt(cli.getOptionValue("h"));
        } else {
            if (cli.hasOption("d")) {
                hours = Integer.parseInt(cli.getOptionValue("d")) * 24;
            } else {
                throw new RuntimeException("You must specify days the file created before today");
            }
        }
        System.out.println("hours=" + hours);
        DeleteDirAndFileBaseOnModifyTime task = new DeleteDirAndFileBaseOnModifyTime(dryRun, rootDir, hours);
        task.execute();
    }
}
