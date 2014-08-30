package com.datatorrent.template;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventId
{

  public long batchId;
  public long tupleId;
  public long windowId;
  public long partitionId;
  public long operatorId;
  public long recieveWindowId;

  @Override
  public String toString()
  {
    return batchId + ", tupleId=" + tupleId
      + ", windowId=" + Long.toHexString(windowId) + ", partitionId=" + partitionId + ", operatorId=" + operatorId + ", recieveWindowId=" + Long.toHexString(recieveWindowId) + '}';
  }

  private static final Logger logger = LoggerFactory.getLogger(EventId.class);
}
