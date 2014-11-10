package com.datatorrent.template;

import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.OperatorContext;

import com.datatorrent.lib.io.fs.AbstractFSWriter;

public class EventWriter extends AbstractFSWriter<EventId, EventId>
{
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(EventWriter.class);
  private transient int operatorId;

  @Override
  protected byte[] getBytesForTuple(EventId t)
  {
    return t.toString().getBytes();
  }

  private transient String windowIdString;

  public EventWriter()
  {
    append = false;
    maxOpenFiles = 1;
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    windowIdString = Long.toString(windowId);
  }

  @Override
  public void endWindow()
  {
    endOffsets.clear();
  }

  @Override
  protected String getFileName(EventId tuple)
  {
    return operatorId + "/" + windowIdString;
  }

  @Override
  public void setup(OperatorContext context)
  {
    operatorId = context.getId();
    super.setup(context);
  }

}
