package com.baidu.hadoop.hdfs.aliasMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockIdManager;
import org.apache.hadoop.hdfs.server.common.BlockAlias;
import org.apache.hadoop.hdfs.server.common.blockaliasmap.BlockAliasMap;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INodeFile;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class AliasMapImportTool {
private final FSNamesystem namesystem;
private final Configuration conf;
private final BlockAliasMap aliasMap;

public AliasMapImportTool(FSNamesystem namesystem, Configuration conf) {
    this.namesystem = namesystem;
    this.conf = conf;
    this.aliasMap = namesystem.getBlocka

        .getBlockAliasMap();
}
}
