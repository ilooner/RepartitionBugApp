package com.datatorrent.template;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;

/**
 * Created by gaurav on 11/11/14.
 */
public class EventEmitterUnifier implements Operator.Unifier<EventId>
{
  private transient long windowId;
  private long tupleCounter = 0;
  private static final Logger LOG = LoggerFactory.getLogger(EventEmitterUnifier.class);
  public final transient DefaultOutputPort<EventId> output = new DefaultOutputPort<EventId>();
  private transient boolean firstTuple = true;

  @Override
  public void process(EventId tuple)
  {
    tupleCounter++;
    if(firstTuple){
      firstTuple = false;
      LOG.info("window id {} of the tuple and window id {} of the operator",tuple.windowId, windowId);
    }
    output.emit(tuple);
  }

  @Override
  public void beginWindow(long windowId)
  {
    this.windowId = windowId;
    firstTuple = true;
  }

  @Override
  public void endWindow()
  {

    LOG.info("total tuples {} in window id {}", tupleCounter, windowId);
  }

  @Override
  public void setup(Context.OperatorContext context)
  {

  }

  @Override
  public void teardown()
  {

  }
}
