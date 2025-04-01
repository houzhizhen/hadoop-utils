package com.baidu.fs.parallel;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;

/**
 * Find top directories with max sub-files or sub-directories.
 */
public class FindTopDirectories {
    public static final Log LOG = LogFactory.getLog(FindTopDirectories.class);

    private final FileSystem fs;

    private Path path;
    private int topNum;
    PriorityQueue<Pair<Path, Integer>> queue;

    public FindTopDirectories(FileSystem fs, Path path, int topNum) {
        this.fs = fs;
        this.path = path;
        this.topNum = topNum;
        this.queue = new PriorityQueue(topNum, new Comparator<Pair<Path, Integer>>() {
            @Override
            public int compare(Pair<Path, Integer> o1, Pair<Path, Integer> o2) {
                return o1.getValue() - o2.getValue();
            }
        });
    }

    public void run() throws IOException {
        try {
            dfs(this.path);

            while (! queue.isEmpty()) {
                Pair<Path, Integer> pair = queue.remove();
                System.out.println("Path: " + pair.getKey() + " has sub files or directories:" + pair.getValue());
            }
        } finally {
            fs.close();
        }
    }

    private void dfs(Path path) throws IOException {
        try {
            FileStatus[] statuses = fs.listStatus(path);
            int size = statuses.length;
            addIfNeeded(path, size);
            for (FileStatus status : statuses) {
                if (status.isDirectory()) {
                    dfs(status.getPath());
                }
            }
        } catch (FileNotFoundException e) {
            // ignore not found path
        }
    }

    private synchronized void addIfNeeded(Path path, int count) {
        if (queue.size() < this.topNum) {
            queue.add(new Pair<>(path, count));
        } else {
            Pair<Path, Integer> last = queue.peek();
            if (last.getValue() > count) {
                // do nothing
            } else {
                queue.poll();
                queue.add(new Pair<>(path, count));
            }
        }
    }

    private static Options createOptions() {
        Options result = new Options();

        result.addOption(OptionBuilder
                .withLongOpt("root-dir")
                .withDescription("Specify a root directory to find")
                .hasArg()
                .create('d'));

        result.addOption(OptionBuilder
                .withLongOpt("top-num")
                .withDescription("Specify number of top directories with max sub-directories or sub-files")
                .hasArg()
                .create('n'));
        return result;
    }

    public static void main(String[] args) throws IOException, ParseException {
        Options opts = createOptions();
        CommandLine cli = new GnuParser().parse(opts, args);
        String rootDir = null;
        if (cli.hasOption('d')) {
            rootDir = cli.getOptionValue('d');
        } else {
            System.err.println("must has parameter --root-dir or -d to specify root dir");
        }
        int topNum = 10;
        if (cli.hasOption('n')) {
            topNum = Integer.parseInt(cli.getOptionValue('n'));
        }

        LOG.info(String.format("basePath='%s', topNum= %s", rootDir, topNum));

        Path basePath = new Path(rootDir);

        Configuration conf = new HdfsConfiguration();
        FileSystem fs = FileSystem.get(URI.create(rootDir), conf);
        if (!fs.exists(basePath)) {
            LOG.info("Path " + basePath + "  does not exist");
            fs.close();
            return;
        }

        FindTopDirectories findTopDirectories = new FindTopDirectories(fs, basePath, topNum);
        findTopDirectories.run();
    }
}
