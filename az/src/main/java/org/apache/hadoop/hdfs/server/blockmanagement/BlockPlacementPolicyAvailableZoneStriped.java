package org.apache.hadoop.hdfs.server.blockmanagement;

import org.apache.hadoop.hdfs.protocol.BlockType;

public class BlockPlacementPolicyAvailableZoneStriped extends BlockPlacementPolicyAvailableZone {

  @Override
  protected BlockType getBlockType() {
    return BlockType.STRIPED;
  }
}

