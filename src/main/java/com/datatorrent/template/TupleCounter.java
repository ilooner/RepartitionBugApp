/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.datatorrent.template;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TupleCounter extends BaseOperator
{
  private static final Logger LOG = LoggerFactory.getLogger(TupleCounter.class);
  public final transient DefaultInputPort<EventId> input = new DefaultInputPort<EventId>() {

    @Override
    public void process(EventId tuple)
    {
      if(tuple.recieveWindowId != activationWindowId)
      {
        throw new RuntimeException("Activation Windows don't equal " + tuple.recieveWindowId + " " + activationWindowId);
      }
      
      assert(tuple.recieveWindowId == activationWindowId);
      tupleCounter++;
      tuple.recieveWindowId = windowId;
      eventOutput.emit(tuple);
    }
  };

  private transient long windowId;
  public final transient DefaultOutputPort<String> counterOutput = new DefaultOutputPort<String>();
  public final transient DefaultOutputPort<EventId> eventOutput = new DefaultOutputPort<EventId>();
  private long activationWindowId;
  protected long tupleCounter = 0;

  @Override
  public void setup(Context.OperatorContext context)
  {
    activationWindowId = context.getValue(OperatorContext.ACTIVATION_WINDOW_ID);
  }

  @Override
  public void beginWindow(long windowId)
  {
    this.windowId = windowId;
  }

  @Override
  public void endWindow()
  {
    counterOutput.emit("TupleCounter " + windowId + " : " + tupleCounter);
  }
}
