package com.datatorrent.template;

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
    StringBuilder sb = new StringBuilder();
    sb.append("batchId: ");
    sb.append(batchId);
    sb.append(" tupleId: ");
    sb.append(tupleId);
    sb.append(" windowId: ");
    sb.append(windowId);
    sb.append(" partitionId: ");
    sb.append(partitionId);
    sb.append(" operatorId: ");
    sb.append(operatorId);
    sb.append(" recieveWindowId: ");
    sb.append(recieveWindowId);
    sb.append("\n");
    
    return sb.toString();
  }
}
