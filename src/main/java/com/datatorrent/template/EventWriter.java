package com.datatorrent.template;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.io.fs.AbstractHdfsFileOutputOperator;
import org.apache.hadoop.fs.Path;

public class EventWriter extends AbstractHdfsFileOutputOperator<EventId>
{
  public static final String FILE_NAME = "hdfs://node2.morado.com/user/tim/repartitionTest/reptest.txt";
  
  @Override
  protected void processTuple(EventId t)
  {
    try
    {
      if(bufferedOutput == null)
      {
        openFile(new Path(FILE_NAME));
      }
    
      bufferedOutput.write(getBytesForTuple(t));
    }
    catch(Exception e)
    {
    }
  }

  @Override
  protected byte[] getBytesForTuple(EventId t)
  {
    return t.toString().getBytes();
  }
  
  @Override
  public void endWindow()
  {
    try
    {
      closeFile();
    }
    catch(Exception e)
    {
      
    }
  }
  
  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    
    try
    {
      fs.delete(new Path(FILE_NAME), true);
    }
    catch(Exception e)
    {
    }
  }
}
