package com.baidu.fs.compare;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

public class DeleteFilesNotExistInFirstDir {
    public static final Log LOG = LogFactory.getLog(DeleteFilesNotExistInFirstDir.class);

    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            printUsage();
            System.exit(1);
        }
        LOG.info("basePath: " + args[0]);
        LOG.info("targetPath: " + args[1]);
        Configuration conf = new HdfsConfiguration();
        URI baseUri = URI.create(args[0]);
        FileSystem baseFs = FileSystem.get(baseUri, conf);
        Path basePath = new Path(baseUri.getPath());
        URI targetUri = URI.create(args[1]);
        FileSystem targetFs = FileSystem.get(targetUri, conf);
        Path targetPath = new Path(targetUri.getPath());
        if (!baseFs.exists(basePath)) {
            LOG.info("basePath: " + basePath + " does not exist");
        }

        if (! baseFs.isDirectory(basePath)) {
            LOG.info("basePath: " + basePath + " is not directory");
        }

        if (!targetFs.exists(targetPath)) {
            LOG.info("targetPath: " + targetPath + " does not exist");
        }

        if (! targetFs.isDirectory(targetPath)) {
            LOG.info("targetPath: " + targetPath + " is not directory");
        }
        System.out.println("baseFs.getScheme():" + baseFs.getScheme());
        System.out.println("targetFs.getScheme():" + targetFs.getScheme());
        deleteRecursively(baseFs, basePath, targetFs, targetPath);
    }

    private static void deleteRecursively(FileSystem baseFs, Path basePath, FileSystem targetFs, Path targetPath) throws IOException {
        Set<String> baseFilesSet = new HashSet<>() ;

        FileStatus[] baseFiles = baseFs.listStatus(basePath);
        for (FileStatus baseFile : baseFiles) {
            baseFilesSet.add(baseFile.getPath().getName());
        }
        FileStatus[] targetFiles = targetFs.listStatus(targetPath);
        for (FileStatus targetFile : targetFiles) {

            String name = targetFile.getPath().getName();

            if (! baseFilesSet.contains(name)) {
                if (name.startsWith(".")) {
                    continue;
                }
                System.out.println("rm -rf " + targetFile.getPath().toString().substring(targetFs.getScheme().length()+1));
            } else if (baseFs.isDirectory(new Path(basePath, name)) &&
                    targetFs.isDirectory(new Path(targetPath, name))) {
                    deleteRecursively(baseFs, new Path(basePath, name),
                            targetFs, new Path(targetPath, name));
           } else if (baseFs.isDirectory(new Path(basePath, name)) ||
                    targetFs.isDirectory(new Path(targetPath, name))){
                    System.out.println("base path: " + new Path(basePath, name) + " is directory " +
                            baseFs.isDirectory(new Path(basePath, name)));
                    System.out.println("target path: " + new Path(targetPath, name) + " is directory " +
                            targetFs.isDirectory(new Path(targetPath, name)));
          }
        }
    }

    private static void printUsage() {
        LOG.info("Usage: DeleteFilesNotExistInFirstDir baseDir targetDir.");
    }
}
