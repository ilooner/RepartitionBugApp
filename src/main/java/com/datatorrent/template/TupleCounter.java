/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.datatorrent.template;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;

public class TupleCounter extends BaseOperator
{
  private transient long windowId;
  private long activationWindowId;
  protected long tupleCounter = 0;
  private static final Logger LOG = LoggerFactory.getLogger(TupleCounter.class);

  public final transient DefaultOutputPort<String> counterOutput = new DefaultOutputPort<String>();
  public final transient DefaultOutputPort<EventId> eventOutput = new DefaultOutputPort<EventId>();
  public final transient DefaultInputPort<EventId> input = new DefaultInputPort<EventId>()
  {
    @Override
    public void process(EventId tuple)
    {
      // upstream operator can be activated at earlier windowId
      if (tuple.recieveWindowId > activationWindowId) {
        throw new RuntimeException("Invalid activation windows " + tuple.recieveWindowId + " " + activationWindowId);
      }

      //assert(tuple.recieveWindowId == activationWindowId);
      tupleCounter++;
      tuple.recieveWindowId = windowId;
      eventOutput.emit(tuple);
    }
  };

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
