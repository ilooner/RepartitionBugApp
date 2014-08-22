/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.datatorrent.template;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TupleCounter<T> extends BaseOperator
{
  private static final Logger LOG = LoggerFactory.getLogger(TupleCounter.class);
  public final transient DefaultInputPort<T> input = new DefaultInputPort<T>() {

    @Override
    public void process(T tuple)
    {
      tupleCounter++;
    }
  };
  
  private transient long windowId;
  private transient long time = System.currentTimeMillis();
  public final transient DefaultOutputPort<String> output = new DefaultOutputPort<String>();
  
  protected long tupleCounter = 0;
  
  @Override
  public void setup(Context.OperatorContext context)
  {
    LOG.info("Tuple Counter Setup");
  }

  @Override
  public void beginWindow(long windowId)
  {
    this.windowId = windowId;
  }

  @Override
  public void endWindow()
  {
    output.emit("TupleCounter " + windowId + " : " + tupleCounter);
  }
}
